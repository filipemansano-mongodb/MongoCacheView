const INTERNAL_DBS = ["local", "config", "admin"];
const ROWS_TO_SHOW = 30;
const REPORT_TIME = 20;
const MB = 1e6;

const getDatabaseNames = () => {
    const dbInfos = db.getSiblingDB("admin")
        .adminCommand({ listDatabases: 1, nameOnly: true });

    return dbInfos.databases
        .filter(database => !INTERNAL_DBS.includes(database.name) && !database.name.startsWith("__realm"))
        .map(database => database.name);
};

const fetchCollectionStats = async (dbName, collName) => {
    return await db.getSiblingDB(dbName)
        .getCollection(collName)
        .stats({ scale: MB, indexDetails: true });
};

const fetchAllStats = async (collectionInfos) => {
    const promises = [];
    for (const { db, coll } of collectionInfos) {
        promises.push(fetchCollectionStats(db, coll));
    }
    return await Promise.all(promises);
};

const getCacheStats = (stats, info) => {

    const inCache = Math.floor(stats["cache"]["bytes currently in the cache"]/MB)
    const cacheRead = Math.floor(stats["cache"]["bytes read into cache"]/MB)
    const cacheWrite = Math.floor(stats["cache"]["bytes written from cache"]/MB)
    const pagesUsed = Math.floor(stats["cache"]["pages requested from the cache"])
    
    const sizeDiff = Math.floor((inCache - info.inCache)/REPORT_TIME)
    const readDiff = Math.floor((cacheRead - info.cacheRead)/REPORT_TIME)
    const writeDiff = Math.floor((cacheWrite - info.cacheWrite)/REPORT_TIME)
    const pageUseDiff = Math.floor((pagesUsed - info.pagesUsed)/REPORT_TIME)

    return {
        inCache,
        cacheRead,
        cacheWrite,
        pagesUsed,
        sizeDiff,
        readDiff,
        writeDiff,
        pageUseDiff,
    }
}

const displayTable = (rows) => {

    rows.sort((a, b) => b.cached - a.cached);
    if (rows.length > ROWS_TO_SHOW) {
        rows = rows.splice(0, ROWS_TO_SHOW);
    }

    console.clear();
    console.table(rows);
};

(async () => {
    const dbNames = getDatabaseNames();

    const collectionInfos = dbNames.flatMap(dbName => {
        const currentDb = db.getSiblingDB(dbName);
        return currentDb.getCollectionInfos()
            .filter(coll => coll.type === "collection" && !coll.name.startsWith("system."))
            .map(coll => {
                const indexesInfo = currentDb.getCollection(coll.name)
                    .getIndexes()
                    .map(index => ({
                        name: index.name,
                        inCache: 0,
                        cacheRead: 0,
                        cacheWrite: 0,
                        pagesUsed: 0
                    }));
                return {
                    db: dbName,
                    coll: coll.name,
                    inCache: 0,
                    cacheRead: 0,
                    cacheWrite: 0,
                    pagesUsed: 0,
                    indexesInfo: indexesInfo
                };
            });
    });

    while (true) {
        const rows = [];
        const stats = await fetchAllStats(collectionInfos);

        stats.forEach((collStats, index) => {
            const collInfo = collectionInfos[index];

            const {
                inCache,
                cacheRead,
                cacheWrite,
                pagesUsed,
                sizeDiff,
                readDiff,
                writeDiff,
                pageUseDiff,
            } = getCacheStats(collStats["wiredTiger"], collInfo);

            const collSize = collStats["size"] + collStats['totalIndexSize'];
            const namespace = `${collInfo.db}.${collInfo.coll}`;

            if (collSize > 0) {
                rows.push({
                    name: namespace,
                    size: collSize,
                    cached: inCache,
                    percentage: Math.floor((inCache / collSize) * 100),
                    delta: sizeDiff,
                    read: readDiff,
                    written: writeDiff,
                    used: pageUseDiff
                });
            }

            collectionInfos[index].inCache = inCache;
            collectionInfos[index].cacheRead = cacheRead;
            collectionInfos[index].cacheWrite = cacheWrite;
            collectionInfos[index].pagesUsed = pagesUsed;

            collInfo.indexesInfo.forEach((indexInfo, idx) => {
                const indexStats = collStats.indexDetails[indexInfo.name];

                const {
                    inCache,
                    cacheRead,
                    cacheWrite,
                    pagesUsed,
                    sizeDiff,
                    readDiff,
                    writeDiff,
                    pageUseDiff,
                } = getCacheStats(indexStats, indexInfo);

                const indexSize = collStats.indexSizes[indexInfo.name];

                if (indexSize > 0) {
                    rows.push({
                        name: `${namespace} - IX: ${indexInfo.name}`,
                        size: indexSize,
                        cached: inCache,
                        percentage: Math.floor((inCache / indexSize) * 100),
                        delta: sizeDiff,
                        read: readDiff,
                        written: writeDiff,
                        used: pageUseDiff
                    });
                }

                collectionInfos[index].indexesInfo[idx].inCache = inCache;
                collectionInfos[index].indexesInfo[idx].cacheRead = cacheRead;
                collectionInfos[index].indexesInfo[idx].cacheWrite = cacheWrite;
                collectionInfos[index].indexesInfo[idx].pagesUsed = pagesUsed;
            });
        });

        displayTable(rows);

        console.log(`Last updated at: ${new Date().toLocaleTimeString()}`);
        console.log(`Next update at: ${new Date(Date.now() + REPORT_TIME * 1000).toLocaleTimeString()}`);
        await sleep(REPORT_TIME * 1000);
    }
})();
