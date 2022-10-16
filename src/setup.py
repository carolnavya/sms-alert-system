import logging
import producerUtil, senderUtil, trackerDB

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    db = producerUtil.MessageDB(logger, None)
    queue = senderUtil.MessageQueue(logger, None)
    tracker = trackerDB.TrackerDB(logger, None)

    if not db.dynamodb:
            db.dynamodb = db.connect()
    if not db.exists("messages"):
        db.createTable("messages")

    queue.sqs = queue.getSQSInstance()
    queueUrl = queue.getQueueUrl('message-queue.fifo')

    if not tracker.dynamodb:
        tracker.dynamodb = db.connect()
    if not tracker.exists("tracker"):
        print("Does not exist")
        tracker.createTable("tracker")
    tracker.addMessage()


