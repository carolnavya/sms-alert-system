
import datetime
import logging
import boto3
from botocore.exceptions import ClientError


"""
status : 0 ->created, 1-> success, -1->fail, 3->inqueue
"""
class MessageDB:
    def __init__(self,dynamodb):
         self.dynamodb = dynamodb
         self.table = None

    def connect(self):
        # Get the service resource.
        dynamodb = boto3.resource('dynamodb')
        print("Connected to AWS DynamoDB")
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

    def createTable(self, table_name):
        if not self.exists(table_name):
            # Create the DynamoDB table.
            try:
                self.table = self.dynamodb.create_table(
                    TableName=table_name,
                    AttributeDefinitions=[
                        {'AttributeName': 'sentTo', 'AttributeType': 'S'},
                        {'AttributeName': 'content', 'AttributeType': 'S'},
                        {'AttributeName': 'status', 'AttributeType': 'N'},
                        # {'AttributeName': 'createdDatetime', 'AttributeType': 'S'},
                        # {'AttributeName': 'sendDatetime', 'AttributeType': 'S'},
                        # {'AttributeName': 'updateDatetime', 'AttributeType': 'S'}
                    ],
                    KeySchema=[
                        {'AttributeName': 'sentTo', 'KeyType': 'HASH'},
                        {'AttributeName': 'content', 'KeyType': 'RANGE'},
                        # {'AttributeName': 'status', 'KeyType': 'RANGE'},
                        # {'AttributeName': 'createdDatetime', 'KeyType': 'RANGE'},
                        # {'AttributeName': 'sendDatetime', 'KeyType': 'RANGE'},
                        # {'AttributeName': 'updateDatetime', 'KeyType': 'RANGE'}
                    ],
                    GlobalSecondaryIndexes=[
                        {
                            'IndexName': 'activity',
                            'KeySchema': [
                                {'AttributeName': 'status', 'KeyType': 'HASH'},
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
                    }
                )

                # Wait until the table exists.
                self.table.wait_until_exists()

                # Print out some data about the table.
                print("Table contents:", self.table.item_count)
                
            except ClientError as err:
                    logger.error(
                        "Couldn't create table %s. Here's why: %s: %s", self.table,
                        err.response['Error']['Code'], err.response['Error']['Message'])
                    raise
        
    
    def addMessage(self, phno, messagePayload):
        try:
            # add phno, message pair produced by Producer into the database
            currtime = datetime.datetime()
            self.table.put_item(
                Item={
                    'sentTo': phno,
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

    def deleteMessage(self, phno, messagePayload):
        # deleting phno, message pair
        try:
            self.table.delete_item(Key={'sentTo': phno, 'content': messagePayload})
        except ClientError as err:
            logger.error(
                "Couldn't delete message pair (%s,%s). Here's why: %s: %s", phno, messagePayload,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    
if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    db = MessageDB(None)
    if not db.dynamodb:
        db.dynamodb = db.connect()
        print(db.createTable("messages"))
        print(db.table.item_count)


