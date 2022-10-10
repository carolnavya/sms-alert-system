from time import sleep
import boto3
from botocore.exceptions import ClientError
import logging
import uuid
import senderUtil, producerUtil

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
        response = sqs.receive_message(QueueUrl=queueUrl, MaxNumberOfMessages=1, WaitTimeSeconds=5)
        self.logger.info(response)
        print(response)
        for message in response.get("Messages", []):
            self.deleteMessage(message['ReceiptHandle'])
            messageID = message["Body"]
            receiptHandle = message["ReceiptHandle"]
            #dynamodb.getStatus(messageID)
            #query to update Received Time
            if currFailRate<self.failureRate:
                #fail message
                dynamodb.updateStatus(0)
            # elif <waittime> > self.waitTime:
            #     #fail message
            #     dynamodb.updateStatus(0)
            else:
                dynamodb.updateStatus(1)

            print(f"Message body: {messageID}")
            print(f"Receipt Handle: {receiptHandle}")
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

