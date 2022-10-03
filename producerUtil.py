
import time
import logging
import boto3
from botocore.exceptions import ClientError


"""
status : 0 ->created, 1-> success, -1->fail, 3->inqueue
"""

logger = logging.getLogger(__name__)

class MessageDB:
    def __init__(self,dynamodb):
         self.dynamodb = dynamodb
         self.table = None

    def connect(self):
        # Get the service resource.
        dynamodb = boto3.resource('dynamodb')
        return dynamodb
    
    def exists(self, table_name):
        try:
            table = self.dynamodb.Table(table_name)
            table.load()
            exists = True

        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    table_name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            self.table = table
        return exists

    def createDB(self, table_name):

        # Create the DynamoDB table.
        try:
            self.table = self.dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'mssageID', 'KeyType': 'HASH'},
                    {'AttributeName': 'sentBy', 'KeyType': 'RANGE'},
                    {'AttributeName': 'content', 'KeyType': 'HASH'},
                    {'AttributeName': 'status', 'KeyType': 'RANGE'},
                    {'AttributeName': 'createdDatetime', 'KeyType': 'RANGE'},
                    {'AttributeName': 'sendDatetime', 'KeyType': 'RANGE'},
                    {'AttributeName': 'updateDatetime', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'mssageID', 'AttributeType': 'N'},
                    {'AttributeName': 'sentBy', 'AttributeType': 'S'},
                    {'AttributeName': 'content', 'AttributeType': 'S'},
                    {'AttributeName': 'status', 'AttributeType': 'N'},
                    {'AttributeName': 'createdDatetime', 'AttributeType': 'S'},
                    {'AttributeName': 'sendDatetime', 'AttributeType': 'S'},
                    {'AttributeName': 'updateDatetime', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 150,
                    'WriteCapacityUnits': 150
                }
            )
            # Wait until the table exists.
            self.table.wait_until_exists()
            
        except ClientError as err:
                logger.error(
                    "Couldn't create table %s. Here's why: %s: %s", self.table,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
    

        # Print out some data about the table.
        print(self.table.item_count)
    
    def addMessage(self, phno, messagePayload):
        try:
            currtime = time.time()
            self.table.put_item(
                Item={
                    'sentBy': phno,
                    'content': messagePayload,
                    'status': 0,
                    'createdDatetime':currtime,
                    'sendDatetime':currtime,
                    'updateDatetime':currtime})
        except ClientError as err:
            logger.error(
                "Couldn't add message (%s,%s) to table %s. Here's why: %s: %s",
                phno, messagePayload, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def updateMessage(self):
        pass

    def deleteMessage(self):

        pass


