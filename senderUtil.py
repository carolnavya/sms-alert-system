
from email.message import Message
import logging
from urllib import response
import boto3
from botocore.exceptions import ClientError
import json

class MessageQueue:

    def __init__(self, logger,sqs):
        self.logger = logger
        self.sqs = sqs
        self.msgQueue = None
        self.deadLetterQueue = None
    
    def getSQSInstance(self):
        try:
            sqs = boto3.client("sqs")
            return sqs
        except ClientError as err:
            self.logger.error("Couldn't connect to AWS SQS. Here's why: %s: %s", err.response['Error']['Code'], err.response['Error']['Message'])
            raise


    def createQueue(self, queueName):
        try:
            response = self.sqs.create_queue(QueueName = queueName,
                                                  Attributes = {
                                                    'DelaySeconds':'5',
                                                    'FifoQueue': 'true',
                                                    'MessageRetentionPeriod':'86400',
                                                    'ContentBasedDeduplication':'true'
                                                    
                                                  })
            self.msgQueue = response['QueueUrl']
            self.logger.info(response)
            return self.msgQueue

        except ClientError as err:
            self.logger.error("Couldn't create SQS queue. Here's why: %s: %s", err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    
    def getQueueUrl(self, queueName):
        try:
            queueUrl = self.sqs.get_queue_url(QueueName=queueName)['QueueUrl']
            self.msgQueue = queueUrl
            return queueUrl
        except ClientError as err:
            self.msgQueue = self.createQueue(queueName)
            return self.msgQueue
    
    def addToQueue(self, message):
        try:
            response = self.sqs.send_message(QueueUrl=self.msgQueue, MessageBody=json.dumps(message), MessageGroupId='sender-messages')
            self.logger.info("Successfully Added message %s to queue:  %s ", message, str(response))
        except ClientError as err:
            self.logger.error("Couldn't send the message %s to the SQS queue. Here's why: %s: %s", str(message), err.response['Error']['Code'], err.response['Error']['Message'])
        return
    
    def getMessgaeFromQueue(self):
        response = self.sqs.receive_message(QueueUrl=self.msgQueue)
        self.logger.info(response)
        print(response)
        for message in response.get("Messages", []):
            message_body = message["Body"]
            self.deleteMessage(message['ReceiptHandle'])
            print(f"Message body: {json.loads(message_body)}")
            print(f"Receipt Handle: {message['ReceiptHandle']}")
    
    def deleteMessage(self,receiptHandle):
        response = self.sqs.delete_message(
            QueueUrl=self.msgQueue,
            ReceiptHandle=receiptHandle,
        )
        self.logger.info("Successfully Deleted Message: " + str(response))
        print(response)
        


if __name__ =='__main__':
    logger = logging.getLogger(__name__)
    sqsVar = MessageQueue(logger, None)
    sqsVar.sqs = sqsVar.getSQSInstance()
    queueUrl = sqsVar.getQueueUrl('message-queue.fifo')
    sqsVar.getMessgaeFromQueue()