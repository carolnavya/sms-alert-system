from phone_gen import PhoneNumber
import random
import string

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
        characters = string.ascii_letters + string.digits + string.punctuation
        randomMessage = ''.join(random.choice(characters) for i in range(self.messageLength))
        return randomMessage

    # generate phone number, message pairs 
    def createMessage(self):
        phoneNumber = self.phnoGenerator("USA")
        message = self.messageGenerator()
        # add phno, msg to dynamoDB
        return (phoneNumber,message)

    # add message to SQS queue
    def sendMessgae(self, messagePayload):
        # send message to SQS
        pass

if __name__ == '__main__':
    producer = Producer(10,0,100)
    print(producer.createMessage())