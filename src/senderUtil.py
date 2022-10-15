
import logging
from urllib import response
import boto3
from botocore.exceptions import ClientError
import src.producerUtil as producerUtil

class MessageQueue:

    """
    Creates an object SQS Message queue class
    :param logger: Track the log info
    :param sqs: Instance of AWS SQS
    """  

    def __init__(self, logger,sqs):
        self.logger = logger
        self.sqs = sqs
    
    def getSQSInstance(self):
        """
        Connect to AWS SQS
        """ 
        try:
            sqs = boto3.client("sqs", region_name="us-west-2")
            return sqs
        except ClientError as err:
            self.logger.error("Couldn't connect to AWS SQS. Here's why: %s: %s", err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def createQueue(self, queueName):
        """
        Creates a SQS queue

        :param queueName: SQS queue name to be assigned
        :returns: URL of the SQS queue created 
        """  

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
        """
        Retrieves the URL of the Amazon SQS Queue of an existing queue 

        :param queueName: Existing SQS queue name
        :returns: URL of the message queue
        """  
        try:
            queueUrl = self.sqs.get_queue_url(QueueName=queueName)['QueueUrl']
            return queueUrl
        except ClientError as err:
            queueUrl = self.createQueue(queueName)
            return queueUrl
    
    def addToQueue(self, queueUrl, message):
        """
        Insert a message into the SQS queue

        :param queuerl: SQS Queue instance
        :param message: message to be inserted
        """    
        try:
            response = self.sqs.send_message(QueueUrl=queueUrl, MessageBody=message, MessageGroupId='sender-messages')
            self.logger.info("Successfully Added message %s to queue:  %s ", message, str(response))
        except ClientError as err:
            self.logger.error("Couldn't send the message with message ID %s to the SQS queue. Here's why: %s: %s", message, err.response['Error']['Code'], err.response['Error']['Message'])
        return
    
    def getMessgaeFromQueue(self, dynamodb, queueUrl):
        """
        Retrieve a message from the SQS queue

        :param dynamodb: dynamo DB instance
        :param queuerl: SQS Queue instance
        :returns: message collected
        """
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
        """
        Deletes a message from the SQS queue

        :param receiptHandle: identifier of the message to be deleted
        :param queuerl: SQS Queue instance
        :returns: status of the query
        """  

        response = self.sqs.delete_message(
            QueueUrl=queueUrl,
            ReceiptHandle=receiptHandle,
        )
        self.logger.info("Successfully Deleted Message: " + str(response))
        print("deleted")
        return
