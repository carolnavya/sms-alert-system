from phone_gen import PhoneNumber
import random
import string
import src.producerUtil as producerUtil
import src.senderUtil as senderUtil
import logging
import uuid

class Producer():

    def __init__(self,maxMessages, messageLength):
        self.maxMessages = maxMessages
        self.messageLength = messageLength

    # generate random valid phone number for given region code
    def phnoGenerator(self, regionCode):
        # print("Generating phone number")
        return PhoneNumber(regionCode).get_number()

    # generate a SMS message with 100 random characters
    def messageGenerator(self):
        characters = string.ascii_letters + string.digits + string.whitespace
        randomMessage = ''.join(random.choice(characters) for i in range(self.messageLength))
        return randomMessage

    # generate phone number, message pairs 
    def createMessage(self):
        phoneNumber = self.phnoGenerator("USA")
        message = self.messageGenerator()
        # add phno, msg to dynamoDB
        return (phoneNumber,message)
    
    def run(self):
        logger = logging.getLogger(__name__)
        db = producerUtil.MessageDB(logger, None)
        producer = Producer(self.maxMessages, self.messageLength)
        queue = senderUtil.MessageQueue(logger, None)
        if not db.dynamodb:
            db.dynamodb = db.connect()
        if not db.exists("messages"):
            print("Does not exist")
            db.createTable("messages")
        queue.sqs = queue.getSQSInstance()
        queueUrl = queue.getQueueUrl('message-queue.fifo')
        for idx in range(self.maxMessages):
            messageId = str(uuid.uuid4())
            phno, sms = producer.createMessage()
            db.addMessage(messageId, phno, sms)
            queue.addToQueue(queueUrl, messageId)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-max', dest='maxMessages', type=int, help='Add time that each sender has to wait (in seconds) before sending the next message')
    parser.add_argument('-len', dest='messageLength', type=int, help='Add Failure Rate for the sender (percentage %)')
    args = parser.parse_args()
    logger = logging.getLogger(__name__)
    Producer(args.maxMessages, args.messageLength).run()




# if __name__ == '__main__':
#     logger = logging.getLogger(__name__)
#     db = producerUtil.MessageDB(None)
#     producer = Producer(maxMessages=100, messsageSent=0,messageLength=100)
#     for i in range(N):
#         phno, sms = producer.createMessage()
#         print(phno, sms)
#         if not db.dynamodb:
#             db.dynamodb = db.connect()
#         if not db.exists("messages"):
#             print("Does not exist")
#             db.createTable("messages")
#         db.addMessage(phno, sms)
#         if i==8:
#             db.updateStatus(phno,sms,3)
#     print(db.getStatus(0))
