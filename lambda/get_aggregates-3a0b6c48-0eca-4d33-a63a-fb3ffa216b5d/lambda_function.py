import json
import boto3
import traceback
from botocore.exceptions import ClientError
import trackerDB
import logging

def updateTrackerDB(attr, counter):
    logger = logging.getLogger(__name__)
    db = trackerDB.TrackerDB(logger, None)
    if not db.dynamodb:
        db.dynamodb = db.connect()
        db.createTable('tracker')
        db.addMessage()
    if attr == 'failCount':
        oldVal = db.getMessagesFailed()
        print("FAIL COUNT: ", oldVal)
        response = db.setMessagesFailed(oldVal)
    elif attr == "totalCount":
        oldVal = db.getMessagesSent()
        response = db.setMessagesSent(oldVal)
    elif attr == "avgTime":
        oldVal = db.getAvgTime()
        response = db.setAvgTime(oldVal)
    return

def parseStreamArn(streamARN):
    tableName = streamARN.split(':')[5].split('/')[1]
    return(tableName)
    
def lambda_handler(event, context):
    records = event["Records"]
    tableName = parseStreamArn(records[0]['eventSourceARN'])
    print(tableName)
    new_record = []
    
    for record in records:
        event_name = record['eventName'].upper()
        message_id = record['dynamodb']['Keys']['messageID']['S']
        new_message = record['dynamodb']['NewImage']
        old_record = record['dynamodb']['OldImage']

        if (event_name == 'MODIFY'):
            print(event_name, record)
            msg_status = new_message['Qstatus']['N']
            if msg_status == "0":
                updateTrackerDB('failCount', 1)
                updateTrackerDB('totalCount', 1)
            elif msg_status == "1":
                updateTrackerDB('totalCount', 1)
                # updateTrackerDB('avgTime', new_message['messageTime']['S'])
		
    return {
        'statusCode': 200,
        'body': msg_status
    }
