import asyncio
import src.producerUtil as producerUtil, src.senderUtil as senderUtil
import logging


class ActivityMonitor:
    def __init__(self, checkTime):
        self.checkTime = checkTime
    
    def getQuery(self):
        logger = logging.getLogger(__name__)
        db = producerUtil.MessageDB(logger, None)
        # messagesFailed = db.getStatus(0)
        # totalMessages = db.getStatus(1)+messagesFailed
        # avgTime = db.getAvgTime()
        return messagesFailed, totalMessages, avgTime

    async def run(self):
        while True:
            messagesFailed, totalMessages, avgTime = self.getQuery()
            print(messagesFailed, totalMessages, avgTime)
            await asyncio.sleep(self.checkTime)

    # if __name__ == '__main__':
    #     asyncio.run(ActivityMonitor(30).run())