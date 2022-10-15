class ActivityMonitor:
    def __init__(self, checkTime):
        self.checkTime = checkTime
    
    def getQuery(self, db):
        messagesFailed = db.getMessagesFailed()
        totalMessages = db.getMessagesSent()
        avgTime = db.getAvgTime()
        print("Messages Failed : {0}\n Messages Delivered: {1}\n Average Message Delivery Time: {2}\n".format(messagesFailed, totalMessages, avgTime))
        return messagesFailed, totalMessages, avgTime

    async def run(self):
        logger = logging.getLogger(__name__)
        db = trackerDB.TrackerDB(logger, None)
        if not db.dynamodb:
                db.dynamodb = db.connect()
        if not db.exists("tracker"):
                print("Does not exist")
                db.createTable("tracker")
        while True:
            self.getQuery(db)
            await asyncio.sleep(self.checkTime)

if __name__ == '__main__':
    
    import argparse
    import asyncio
    import producerUtil, senderUtil
    import logging
    import trackerDB


    parser = argparse.ArgumentParser()
    parser.add_argument('-t', dest='activityTime', type=int, help='Set number of seconds to casually look over the stats')
    args = parser.parse_args()
    monitor = ActivityMonitor(args.activityTime)
    asyncio.run(monitor.run())