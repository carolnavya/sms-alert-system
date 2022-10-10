import boto3
from botocore.exceptions import ClientError
import logging

class Sender:
    def __init__(self, logger, senderID, waitTime, failureRate):
        self.logger = logger
        self.senderID = senderID
        self.waitTime = waitTime
        self.failureRate = failureRate
    
    def process_message(self,message):
        pass

