
import logging
from urllib import response
import boto3
from botocore.exceptions import ClientError
import producerUtil

class MessageQueue:

    def __init__(self, logger,sqs):
        self.logger = logger
        self.sqs = sqs
    
    def getSQSInstance(self):
        try:
            sqs = boto3.client("sqs", region_name="us-west-2")
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
                                                    'ContentBasedDeduplication':'true' })
            self.logger.info(response)
            return response['QueueUrl']

        except ClientError as err:
            self.logger.error("Couldn't create SQS queue. Here's why: %s: %s", err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    
    def getQueueUrl(self, queueName):
        try:
            queueUrl = self.sqs.get_queue_url(QueueName=queueName)['QueueUrl']
            return queueUrl
        except ClientError as err:
            queueUrl = self.createQueue(queueName)
            return queueUrl
    
    def addToQueue(self, queueUrl, message):
        try:
            response = self.sqs.send_message(QueueUrl=queueUrl, MessageBody=message, MessageGroupId='sender-messages')
            self.logger.info("Successfully Added message %s to queue:  %s ", message, str(response))
        except ClientError as err:
            self.logger.error("Couldn't send the message with message ID %s to the SQS queue. Here's why: %s: %s", message, err.response['Error']['Code'], err.response['Error']['Message'])
        return
    
    def getMessgaeFromQueue(self, dynamodb, queueUrl):
        response = self.sqs.receive_message(QueueUrl=queueUrl, AttributeNames=['sentTimestamp'], MaxNumberOfMessages=1, WaitTimeSeconds=1, MessageAttributeNames=['All'])
        self.logger.info(response)
        if 'Messages' in response.keys():
            metadata = response['ResponseMetadata']
            message = response['Messages'][0]
            self.deleteMessage(message['ReceiptHandle'], queueUrl)
            print(f"Message body: {message['Body']}\n")
            print(f"Received Time: {metadata['HTTPHeaders']['date']}\n")
            dynamodb.updateTimer(message['Body'], metadata['HTTPHeaders']['date'])
            return message['Body']
        return None

    def deleteMessage(self,receiptHandle, queueUrl):
        response = self.sqs.delete_message(
            QueueUrl=queueUrl,
            ReceiptHandle=receiptHandle,
        )
        self.logger.info("Successfully Deleted Message: " + str(response))
        print("deleted")
        return

# if __name__ =='__main__':
#     
#     logger = logging.getLogger(__name__)
#     sqsVar = MessageQueue(logger, None)
#     sqsVar.sqs = sqsVar.getSQSInstance()
#     queueUrl = sqsVar.getQueueUrl('message-queue.fifo')
#     sqsVar.addToQueue(queueUrl)
#     sqsVar.getMessgaeFromQueue(queueUrl)
