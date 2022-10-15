from botocore.exceptions import ClientError
import boto3
import logging
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime

class TrackerDB:
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
                        {'AttributeName': 'trackerID', 'AttributeType': 'S'},
                    ],
                    KeySchema=[
                        {'AttributeName': 'trackerID', 'KeyType': 'HASH'}
                    ],
                    BillingMode='PROVISIONED',
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 100,
                        'WriteCapacityUnits': 100
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
    
    def addMessage(self):
        try:
            self.table.put_item(
                Item={
                    'trackerID': 'aaaaaaaa-bbbb-1111-2222-abcde1234567',
                    'messagesFailed': 0,
                    'messagesSent': 0,
                    'avgTime': 0,
                    })
        except ClientError as err:
            self.logger.error(
                "Couldn't add message (%s,%s) to table %s. Here's why: %s: %s",
                phno, messageBody, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    
    def getAvgTime(self):
        avgTime = self.table.query(
            ProjectionExpression="avgTime",
            KeyConditionExpression = Key('trackerID').eq('aaaaaaaa-bbbb-1111-2222-abcde1234567'))
        avgTime = avgTime['Items'][0]['avgTime']
        return avgTime
    
    def setAvgTime(self, newVal):
        oldVal = self.getAvgTime()
        print(oldVal, self.table)
        try:
            if newVal == '0':
                mean_time = 0
                return
            elif oldVal == "0" and newVal!="0":
                mean_time = datetime.strptime(newVal,"%H:%M:%S.%f")
                mean_time = mean_time.second+(mean_time.minute*60)
            else:
                pretime = float(oldVal)
                posttime = datetime.strptime(newVal,"%H:%M:%S.%f")
                posttime = posttime.second+(posttime.minute*60)
                # print(posttime)
                mean_time = pretime+posttime/2
            # print("MEAN TIME:", mean_time)
            response = self.table.update_item(
                    Key={
                        'trackerID': 'aaaaaaaa-bbbb-1111-2222-abcde1234567',
                    },
                    UpdateExpression='SET avgTime = :time',
                    ExpressionAttributeValues={
                        ':time': str(mean_time)
                    }
                )
            return response
                
        except ClientError as err:
            self.logger.error(
                "Couldn't update avg Time. Here's why: %s: %s", err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    
    def getMessagesFailed(self):
        try:
            # print("Table Name", self.table)
            n_failed = self.table.query(
                ProjectionExpression="messagesFailed",
                KeyConditionExpression = Key('trackerID').eq('aaaaaaaa-bbbb-1111-2222-abcde1234567'))
            n_failed = n_failed['Items'][0]['messagesFailed']
            return n_failed
        except ClientError as err:
            self.logger.error(
                "Couldn't get messagesFailed. Here's why: %s: %s", err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    
    def setMessagesFailed(self): 
        oldVal = self.getMessagesFailed()
        print("old Fail Count: ", oldVal)
        try:
            response = self.table.update_item(
                    Key={
                        'trackerID': 'aaaaaaaa-bbbb-1111-2222-abcde1234567',
                    },
                    UpdateExpression='SET messagesFailed = :val',
                    ExpressionAttributeValues={
                        ':val': oldVal+1
                    }
                )
            print(response)
            print("Fail count Set to:", (oldVal+1))
            return response
                
        except ClientError as err:
            self.logger.error(
                "Couldn't update number of messages Failed. Here's why: %s: %s", err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    
    def getMessagesSent(self):
        try:
            n_sent = self.table.query(
                ProjectionExpression="messagesSent",
                KeyConditionExpression = Key('trackerID').eq('aaaaaaaa-bbbb-1111-2222-abcde1234567'))
            n_sent = n_sent['Items'][0]['messagesSent']
            return n_sent
        except ClientError as err:
            self.logger.error(
                "Couldn't get total messages sent. Here's why: %s: %s", err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    
    def setMessagesSent(self):
        oldVal = self.getMessagesSent()
        try:
            response = self.table.update_item(
                    Key={
                        'trackerID': 'aaaaaaaa-bbbb-1111-2222-abcde1234567',
                    },
                    UpdateExpression='SET messagesSent = :val',
                    ExpressionAttributeValues={
                        ':val': oldVal+1
                    }
                )
            return response
                
        except ClientError as err:
            self.logger.error(
                "Couldn't update total number of messages sent. Here's why: %s: %s", err.response['Error']['Code'], err.response['Error']['Message'])
            raise