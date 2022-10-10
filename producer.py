from phone_gen import PhoneNumber
import random
import string
import producerUtil
import logging

class Producer():

    def __init__(self,maxMessages,messsageSent, messageLength):
        self.maxMessages = maxMessages
        self.messageSent = messsageSent
        self.messageLength = messageLength

    # generate random valid phone number for given region code
    def phnoGenerator(self, regionCode):
        print("Generating phone number")
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

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    db = producerUtil.MessageDB(None)
    producer = Producer(10,0,100)
    N=10
    for i in range(N):
        phno, sms = producer.createMessage()
        print(phno, sms)
        if not db.dynamodb:
            db.dynamodb = db.connect()
        if not db.exists("messages"):
            print("Does not exist")
            db.createTable("messages")
        db.addMessage(phno, sms)
        if i==8:
            db.updateStatus(phno,sms,3)
    print(db.getStatus(0))
