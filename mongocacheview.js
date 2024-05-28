//Main Loop
const INTERNAL_DBS = ["local", "config", "admin"]

db = db.getSiblingDB("admin")
const dbInfos = db.adminCommand({listDatabases:1, nameOnly: true})
const dbNames = dbInfos.databases.filter(
    database => !INTERNAL_DBS.includes(database.name) && !database.name.startsWith("__realm")
).map(database => database.name);

const collectionInfos = []
dbNames.forEach(dbName => {
    const currentDb = db.getSiblingDB(dbName);

    const dbCollections = currentDb.getCollectionInfos()
        .filter(coll => coll.type === "collection" && !coll.name.startsWith("system."))
        .map(coll => {

            const indexesInfo = currentDb.getCollection(coll.name)
                .getIndexes()
                .map(index => {
                    return {
                        name: index.name,
                        inCache: 0,
                        cacheRead: 0,
                        cacheWrite: 0,
                        pagesUsed: 0
                    }
                });
            
            return {
                db: dbName,
                coll: coll.name,
                inCache:0,
                cacheRead:0,
                cacheWrite:0,
                pagesUsed:0,
                indexesInfo: indexesInfo
            }
        })
    
    collectionInfos.push(...dbCollections)
})

const REPORT_TIME = 60
const MB = 1e6
const ROWS_TO_SHOW = 30


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

while(true){
    const rows = [];
    for (let collIndex = 0; collIndex < collectionInfos.length; collIndex++) {
        const collInfo = collectionInfos[collIndex];

        const collStats = db.getSiblingDB(collInfo.db)
            .getCollection(collInfo.coll)
            .stats({scale: MB, indexDetails: true})

        const {
            inCache,
            cacheRead,
            cacheWrite,
            pagesUsed,
            sizeDiff,
            readDiff,
            writeDiff,
            pageUseDiff,
        } = getCacheStats(collStats["wiredTiger"], collInfo)

        const collSize = collStats["size"] + collStats['totalIndexSize']

        const namespace = `${collInfo.db}.${collInfo.coll}`;

        if(collSize > 0) {
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

        collectionInfos[collIndex].inCache = inCache;
        collectionInfos[collIndex].cacheRead = cacheRead;
        collectionInfos[collIndex].cacheWrite = cacheWrite;
        collectionInfos[collIndex].pagesUsed = pagesUsed;

        // print index stats
        for (let indexIndex = 0; indexIndex < collInfo.indexesInfo.length; indexIndex++) {
            const indexInfo = collInfo.indexesInfo[indexIndex];
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
            } = getCacheStats(indexStats, indexInfo)
    
            const indexSize = collStats.indexSizes[indexInfo.name];

            if(indexSize > 0) {
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

            collectionInfos[collIndex].indexesInfo[indexIndex].inCache = inCache;
            collectionInfos[collIndex].indexesInfo[indexIndex].cacheRead = cacheRead;
            collectionInfos[collIndex].indexesInfo[indexIndex].cacheWrite = cacheWrite;
            collectionInfos[collIndex].indexesInfo[indexIndex].pagesUsed = pagesUsed;
        }

        if(rows.length > ROWS_TO_SHOW) {
            const topRows = rows.sort((a, b) => b.cached - a.cached).slice(0, ROWS_TO_SHOW);
            console.clear();
            console.table(topRows);
        }else{
            console.clear();
            console.table(rows);
        }

        console.log(`Last updated at: ${new Date().toLocaleTimeString()}`);
    }
    console.log(`Next updated at ${new Date(Date.now() + REPORT_TIME * 1000).toLocaleTimeString()}`);
    sleep(REPORT_TIME * 1000)
}
