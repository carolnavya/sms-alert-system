
from datetime import datetime
import logging
import boto3
from botocore.exceptions import ClientError
import pytz
from boto3.dynamodb.conditions import Key, Attr

class MessageDB:
    """
    Creates an object SQS Message queue class
    :param logger: Track the log info
    :param dynamodb: Instance of AWS dynamoDB 
    """  
    def __init__(self,logger, dynamodb):
        self.dynamodb = dynamodb
        self.table = None
        self.logger = logger

    def connect(self):
        """
        Connect to AWS DynamoDB
        """  
        # Get the service resource.
        try:
            dynamodb = boto3.resource('dynamodb', region_name="us-west-2")
            print("Connected to AWS DynamoDB")
            return dynamodb
        except ClientError as err:
            self.logger.error(
                    "Couldn't connect to AWS Dynamodb service. Here's why: %s: %s", err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    
    def exists(self, table_name):
        """
        Check if a table exists in the DB
        :param table_name: Name of the desired dynamoDB table
        :return: True if exists else False 
        """
        try:
            table = self.dynamodb.Table(table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                self.logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    table_name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            self.table = table
        return exists

    def createTable(self, table_name):
        
        """
        Check if a table in the DB to track messages produced
        :param table_name: Name of the desired dynamoDB table 

        :Attribute messageID (PRIMARY KEY): unique identifier for each message created
        :Attribute QStatus (SECONDARY KEY): Status of message {-1: Created, +1: success, 0: Fail}
        :Attribute sentTo: destination phone number
        :Attribute content: content of the sms
        :Attribute createdDatetime: timestamp when message was created
        :Attribute sendDatetime: timestamp when message was created
        :Attribute createdDatetime: timestamp when message enters the queue
        :Attribute receivedDatetime: timestamp when message was collected by Sender and simulate sending
        :Attribute sendTime: duration of time the message lives in the queue until it is simulated
        """
        if not self.exists(table_name):
            # Create the DynamoDB table.
            try:
                self.table = self.dynamodb.create_table(
                    TableName=table_name,
                    AttributeDefinitions=[
                        {'AttributeName': 'messageID', 'AttributeType': 'S'},
                        {'AttributeName': 'Qstatus', 'AttributeType': 'N'}
                    ],
                    KeySchema=[
                        {'AttributeName': 'messageID', 'KeyType': 'HASH'}
                    ],
                    GlobalSecondaryIndexes=[
                        {
                            'IndexName': 'activity',
                            'KeySchema': [
                                {'AttributeName': 'Qstatus', 'KeyType': 'HASH'},
                            ],
                            'Projection': {'ProjectionType': 'ALL'},
                            'ProvisionedThroughput': {
                                'ReadCapacityUnits': 400,
                                'WriteCapacityUnits': 400
                            }
                        },
                    ],
                    BillingMode='PROVISIONED',
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 500,
                        'WriteCapacityUnits': 500
                    },
                    StreamSpecification={
                        'StreamEnabled': True,
                        'StreamViewType': 'NEW_AND_OLD_IMAGES'
                    },
                )

                # Wait until the table exists.
                self.table.wait_until_exists()

                # Print out some data about the table.
                # print("Table contents:", self.table.item_count)
                
            except ClientError as err:
                    self.logger.error(
                        "Couldn't create table. Here's why: %s: %s", self.table.name,
                        err.response['Error']['Code'], err.response['Error']['Message'])
                    raise
        
    
    def addMessage(self, messageId, phno, messageBody):
        """
        Add message to the table

        :Attribute phno: destination phone number
        :Attribute messageBody: content of the sms
        :Attribute messageID: unique identifier of message
        """
        try:
            # add phno, message pair produced by Producer into the database
            currtime = datetime.now().astimezone(pytz.timezone("Etc/GMT"))
            currtime = str(currtime)
            self.table.put_item(
                Item={
                    'messageID': messageId,
                    'sentTo': phno,
                    'content': messageBody,
                    'Qstatus': -1,
                    'createdDatetime':currtime,
                    'sendDatetime':currtime,
                    'receivedDatetime':currtime,
                    'messageTime':"0"})
            # print(self.table.item_count)
        except ClientError as err:
            self.logger.error(
                "Couldn't add message (%s,%s) to table %s. Here's why: %s: %s",
                phno, messageBody, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def updateStatus(self, messageID, currStatus):
        """
        Add Status of the message being processed
        
        :Attribute messageID: unique identifier of message
        :Attribute currStatus: status update of the message
       
        """
        try:
            self.table.update_item(
                Key={
                    'messageID': messageID,
                },
                UpdateExpression='SET Qstatus = :stat',
                ExpressionAttributeValues={
                    ':stat': currStatus
                }
            )
        except ClientError as err:
            self.logger.error(
                "Couldn't update the status of the message pair (%s,%s). Here's why: %s: %s", phno, messageBody,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def getSendTime(self, reqID):
        """
        Get the time when message of given messageID first entered the queue

        :param reqID: Unique message ID
        :return sendTime: Time when message entered the queue
        """
        msg_sendTime = self.table.query(
            ProjectionExpression="sendDatetime",
            KeyConditionExpression = Key('messageID').eq(reqID))
        sendTime = msg_sendTime['Items'][0]['sendDatetime']
        return sendTime
    
    def getElapsedTime(self, reqID):
        """
        Update messageTime of given messageID

        :param reqID: Unique message ID
        :return elapsedTime: Time to send the message
        """
        try:
            msgTime = self.table.query(
                ProjectionExpression="messageTime",
                KeyConditionExpression = Key('messageID').eq(reqID))
            elapsedTime = msgTime['Items'][0]['messageTime']
            return 
        except ClientError as err:
            self.logger.error("Couldn't get messageTime for messaggeID %s. Here's why: %s: %s", reqID, err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def updateMessageTimer(self, reqID, sendTime, receiveTime):
        """
        Update messageTime of given messageID

        :param reqID: Unique message ID
        :param sendTime: Time when message entered the queue
        :param receiveTime: Time when the message was successfully retrieved and simulated
        """
        try:
            sendTime = sendTime.split("+")[0]

            sendTime = datetime.strptime(sendTime, '%Y-%m-%d %H:%M:%S.%f')
            receiveTime = datetime.strptime(str(receiveTime), '%Y-%m-%d %H:%M:%S')

            print(sendTime, receiveTime)

            elapsedTime = receiveTime - sendTime
            self.table.update_item(
                Key={
                    'messageID': reqID,
                },
                UpdateExpression='SET messageTime = :time',
                ExpressionAttributeValues={
                    ':time': str(elapsedTime)
                }
            )
        except ClientError as err:
            self.logger.error( "Couldn't update the status of the message ID %s. Here's why: %s: %s", reqID, err.response['Error']['Code'], err.response['Error']['Message'])
            raise
           
    def updateTimer(self, reqID, newTime):
        """
        Update receivedDatetime of given messageID

        :param reqID: Unique message ID
        :param newTime: updated received time
        """
        try:
            newTime = datetime.strptime(str(newTime), '%a, %d %b %Y %H:%M:%S %Z')
            print(newTime)
        
            self.table.update_item(
                Key={
                    'messageID': reqID,
                },
                UpdateExpression='SET receivedDatetime = :currTime',
                ExpressionAttributeValues={
                    ':currTime': str(newTime)
                }
            )
            self.logger.info("Successfully updated receive time of messageID {0}".format(reqID))
            sendTime = self.getSendTime(reqID)
            # print(sendTime)
            # print("Updated")
            self.updateMessageTimer(reqID, sendTime, newTime)
        except ClientError as err:
            self.logger.error(
                "Couldn't update receive time of message ID %s. Here's why: %s: %s", reqID,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise


    def deleteMessage(self, reqID):
        """
        Delete meesage from the dynamoDB table 
        
        :param reqID: Unique message ID to be deleted
        """
        # deleting phno, message pair
        try:
            self.table.delete_item(Key={'messageID': reqID})
        except ClientError as err:
            self.logger.error(
                "Couldn't delete message ID %s. Here's why: %s: %s", reqID,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

