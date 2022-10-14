from time import sleep
import boto3
from botocore.exceptions import ClientError
import logging
import uuid
import senderUtil, producerUtil
from datetime import datetime



class Sender:
    def __init__(self, logger, waitTime, failureRate):
        self.logger = logger
        self.senderID = uuid.uuid4()
        self.waitTime = waitTime
        self.failureRate = failureRate
    
    def configureWaitTime():
        #query to get avgWaitTime
        pass
    
    def process_message(self,dynamodb, sqs, queueUrl):
        currFailRate = 0
        n_failed = 0
        total = 0
        for i in range(10):
            messageID = sqs.getMessgaeFromQueue(dynamodb, queueUrl)
            print("Message ID: {}", messageID)
            if messageID !=None:
                total+=1
                currFailRate = n_failed/total
                print(currFailRate, self.failureRate, currFailRate>self.failureRate )
                #elapsedTime = dynamodb.getElapsedTime(messageID)
                if currFailRate<self.failureRate:
                    #fail message
                    dynamodb.updateStatus(messageID, 0)
                    n_failed +=1
                    currFailRate = n_failed/total
                # elif elapsedTime > self.waitTime:
                #     #fail message
                #     dynamodb.updateStatus(messageID, 0)
                #     n_failed +=1
                #     currFailRate = n_failed/total
                else:
                    dynamodb.updateStatus(messageID, 1)
                if currFailRate >= self.failureRate:
                    break
        sleep(self.waitTime)
    
    def run(self):
        logger = logging.getLogger(__name__)
        db = producerUtil.MessageDB(self.logger, None)
        queue = senderUtil.MessageQueue(self.logger, None)
        if not db.dynamodb:
            db.dynamodb = db.connect()
        if not db.exists("messages"):
            print("Does not exist")
            db.createTable("messages")
        queue.sqs = queue.getSQSInstance()
        queueUrl = queue.getQueueUrl('message-queue.fifo')
        self.process_message(db, queue, queueUrl)

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    sender = Sender(logger, 5, 0.70)
    sender.run()

