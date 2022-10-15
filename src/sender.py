from time import sleep
import boto3
from botocore.exceptions import ClientError
import logging
import uuid
import src.senderUtil as senderUtil, src.producerUtil as producerUtil
from datetime import datetime

class Sender:
    """
    Creates an object of the Sender class
    :param logger: Track the logging info
    :param waitTime: Time the sender waits before sending tne next message
    :param failureRate: Failure rate of the sender
    """

    def __init__(self, logger, waitTime, failureRate):
        self.logger = logger
        self.senderID = uuid.uuid4()
        self.waitTime = waitTime
        self.failureRate = failureRate
    
    def process_message(self,dynamodb, sqs, queueUrl):
        """
        Pick message from the Producer and simulate sending SMS

        :param dynamodb: dynamo DB instance
        :param sqs:  Amazon SQS instance
        :param queueURL: SQS Queue name
        """
        currFailRate = 0
        n_failed = 0
        total = 0
        flag = True
        while(flag):
            messageID = sqs.getMessgaeFromQueue(dynamodb, queueUrl)
            print("Message ID: {}", messageID)
            if messageID !=None:
                total+=1
                currFailRate = round(n_failed/total,1)
                print(currFailRate, self.failureRate, currFailRate>self.failureRate )
                #elapsedTime = dynamodb.getElapsedTime(messageID)
                if currFailRate<self.failureRate:
                    #fail message
                    dynamodb.updateStatus(messageID, 0)
                    n_failed +=1
                    currFailRate =round(n_failed/total,1)
                else:
                    dynamodb.updateStatus(messageID, 1)
            if currFailRate <= self.failureRate or messageID == None: 
                flag = False
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
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-w', dest='waitTime', type=int, help='Add time that each sender has to wait (in seconds) before sending the next message')
    parser.add_argument('-f', dest='failureRate', type=float, help='Add Failure Rate for the sender (percentage %)')
    args = parser.parse_args()
    logger = logging.getLogger(__name__)
    sender = Sender(logger, args.waitTime, args.failureRate/100)
    sender.run()

