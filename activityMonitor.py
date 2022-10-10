import asyncio


class ActivityMonitor:
    def __init__(self, checkTime):
        self.checkTime = checkTime
    
    def getQuery(self):
        pass
    
    async def run(self):
        while True:
            print("Hello")
            await asyncio.sleep(self.checkTime)

    # if __name__ == '__main__':
    #     asyncio.run(ActivityMonitor(30).run())