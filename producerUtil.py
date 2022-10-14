
from datetime import datetime
import logging
import boto3
from botocore.exceptions import ClientError
import pytz
from boto3.dynamodb.conditions import Key, Attr

"""
status : -1->created, 1-> success, 0->fail
"""

class MessageDB:
    def __init__(self,logger, dynamodb):
        self.dynamodb = dynamodb
        self.table = None
        self.logger = logger

    def connect(self):
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
                    'messageTime':0})
            # print(self.table.item_count)
        except ClientError as err:
            self.logger.error(
                "Couldn't add message (%s,%s) to table %s. Here's why: %s: %s",
                phno, messageBody, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def updateStatus(self, messageID, currStatus):
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
    
    def getStatus(self, reqStatus):
        try:
            totalCount = self.table.query(Key='messageID', Select='COUNT', KeyConditionExpression= Key('Qstatus').eq(reqStatus))
        except ClientError as err:
            self.logger.error(
                "Couldn't execute the query on table %s. Here's why: %s: %s", self.table.name, err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        return totalCount['Count']
    
    def getStatusByID(self, reqID):
        try:
            msg_status = self.table.query(KeyConditionExpression = Key('messageID').eq(reqID))
        except ClientError as err:
            self.logger.error(
                "Couldn't execute the query on table %s. Here's why: %s: %s", self.table.name, err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        return msg_status

    def getSendTime(self, reqID):
        msg_sendTime = self.table.query(
            ProjectionExpression="sendDatetime",
            KeyConditionExpression = Key('messageID').eq(reqID))
        sendTime = msg_sendTime['Items'][0]['sendDatetime']
        return sendTime
    
    def getElapsedTime(self, reqID):
        msgTime = self.table.query(
            ProjectionExpression="messageTime",
            KeyConditionExpression = Key('messageID').eq(reqID))
        elapsedTime = msgTime['Items'][0]['messageTime']
        return elapsedTime

    def updateMessageTimer(self, reqID, sendTime, receiveTime):
        try:

            sendTime = datetime.strptime(sendTime, '%Y-%m-%d %H:%M:%S.%f')

            receiveTime = datetime.strptime(str(receiveTime), '%Y-%m-%d %H:%M:%S.%f')

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
            self.logger.error(
                "Couldn't update the status of the message pair (%s,%s). Here's why: %s: %s", phno, messageBody,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
           
    def updateTimer(self, reqID, newTime):
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
            #sendTime = self.getSendTime(reqID)
            #print()
            print("Updated")
            #self.updateMessageTimer(reqID, sendTime, newTime)
        except ClientError as err:
            self.logger.error(
                "Couldn't update receive time of message %s. Here's why: %s: %s", reqID,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise


    def deleteMessage(self, phno, messageBody):
        # deleting phno, message pair
        try:
            self.table.delete_item(Key={'sentTo': phno, 'content': messageBody})
        except ClientError as err:
            self.logger.error(
                "Couldn't delete message pair (%s,%s). Here's why: %s: %s", phno, messageBody,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    
if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    db = MessageDB(logger, None)
    if not db.dynamodb:
        db.dynamodb = db.connect()
        print(db.createTable("messages"))
        print(db.updateTimer('c1eb8135-428d-43fe-a830-17d392fd8e12', (datetime.now())))
