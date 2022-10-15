import boto3
from moto import mock_sqs
from botocore.exceptions import ClientError
import uuid
import time

@mock_sqs
def test_sqs_connection_fail():
    sqs = boto3.client("sqs", region_name="us-west-2")

    try:
        sqs.create_queue(QueueName=str(uuid.uuid4())[0:6], Attributes={"FifoQueue": "true"})
    except ClientError as err:
        assert err.response["Error"]["Code"] == "InvalidParameterValue"
    else:
        raise RuntimeError("Should of raised InvalidParameterValue Exception")

@mock_sqs
def test_create_queue_with_same_attributes():
    sqs = boto3.client("sqs", region_name="us-west-2")

    attributes = {
        'DelaySeconds':'5',
        'FifoQueue': 'true',
        'MessageRetentionPeriod':'86400',
        'ContentBasedDeduplication':'true',
        'ReceiveMessageWaitTimeSeconds': '20',
        'VisibilityTimeout':'30',
    }

    q_name = str(uuid.uuid4())[0:6]+'.fifo'
    sqs.create_queue(QueueName=q_name, Attributes=attributes )
    sqs.create_queue(QueueName=q_name, Attributes=attributes)


@mock_sqs
def test_get_queue_url():
    client = boto3.client("sqs", region_name="us-west-2")
    q_name = str(uuid.uuid4())[0:6]
    client.create_queue(QueueName=q_name)

    response = client.get_queue_url(QueueName=q_name)

    res = response["QueueUrl"].split("/")
    assert res[len(res)-1] == q_name

@mock_sqs
def test_message_send_without_attributes():
    sqs = boto3.resource("sqs", region_name="us-west-2")
    queue = sqs.create_queue(QueueName=str(uuid.uuid4())[0:6])
    msg = queue.send_message(MessageBody="some")
    assert msg.get("MD5OfMessageBody") != None
    assert msg.get("MD5OfMessageAttributes") == None
    messages = queue.receive_messages()
    assert len(messages) == 1

@mock_sqs
def test_message_retention_period():
    sqs = boto3.resource("sqs", region_name="us-west-2")
    queue = sqs.create_queue(
        QueueName=str(uuid.uuid4())[0:6], Attributes={"MessageRetentionPeriod": "3"}
    )
    queue.send_message(
        MessageBody="something",
        MessageAttributes={
            "SOME_Valid.attribute-Name": {
                "StringValue": "1234567456",
                "DataType": "Number",
            }
        },
    )
    messages = queue.receive_messages()
    assert len(messages) == 1
    
    queue.send_message(
        MessageBody="something",
        MessageAttributes={
            "SOME_Valid.attribute-Name": {
                "StringValue": "12345623456",
                "DataType": "Number",
            }
        },
    )
    time.sleep(5)
    messages = queue.receive_messages()
    assert len(messages) == 0

@mock_sqs
def test_send_receive_message_without_attributes():
    sqs = boto3.resource("sqs", region_name="us-west-2")
    conn = boto3.client("sqs", region_name="us-west-2")
    q_resp = conn.create_queue(QueueName=str(uuid.uuid4())[0:6])
    queue = sqs.Queue(q_resp["QueueUrl"])

    body_one = "this is a test message"
    body_two = "this is another test message"

    queue.send_message(MessageBody=body_one)
    queue.send_message(MessageBody=body_two)

    messages = conn.receive_message(QueueUrl=queue.url, MaxNumberOfMessages=2)[
        "Messages"
    ]

    message1 = messages[0]
    message2 = messages[1]

    assert message1["Body"] == body_one
    assert message2["Body"] == body_two

    assert message1.get("MD5OfMessageAttributes") == None
    assert message2.get("MD5OfMessageAttributes") == None

    assert message1.get("Attributes") == None
    assert message2.get("Attributes") == None

@mock_sqs
def test_delete_message_twice_using_same_receipt_handle():
    client = boto3.client("sqs", region_name="us-east-1")
    response = client.create_queue(QueueName=str(uuid.uuid4())[0:6])
    queue_url = response["QueueUrl"]

    client.send_message(QueueUrl=queue_url, MessageBody="body")
    response = client.receive_message(QueueUrl=queue_url)
    receipt_handle = response["Messages"][0]["ReceiptHandle"]

    client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)