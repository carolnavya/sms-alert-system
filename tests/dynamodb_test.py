import pytest
from moto.core import DEFAULT_ACCOUNT_ID
from moto.dynamodb.models import Table
from moto import mock_dynamodb
import boto3
from botocore.exceptions import ClientError


@pytest.fixture
def table():
    return Table(
        "test-messages",
        messageID=DEFAULT_ACCOUNT_ID,
        region="us-west-2",
        schema=[
            {"KeyType": "HASH", "AttributeName": "messageID"}
        ],
        attr=[
            {"AttributeType": "S", "AttributeName": "messageID"}
        ],
    )

@mock_dynamodb
def test_create_table_with_stream_specification():
    db = boto3.client("dynamodb", region_name="us-west-2")

    response = db.create_table(
        TableName="test-streams",
        KeySchema=[{"AttributeName": "messageID", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "messageID", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        StreamSpecification={
            "StreamEnabled": True,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
        },
    )

    assert response["TableDescription"].get("StreamSpecification") != None
    assert response["TableDescription"]["StreamSpecification"] == {"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"}

    assert response["TableDescription"].get("LatestStreamLabel")!=None
    assert response["TableDescription"].get("LatestStreamArn") !=None

    response = db.delete_table(TableName="test-streams")

    assert response["TableDescription"].get("StreamSpecification") != None

@mock_dynamodb
def test_update_map_elements():
    db = boto3.resource("dynamodb", region_name="us-west-2")
    db.create_table(
        TableName="test-messages",
        KeySchema=[{"AttributeName": "messageID", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "messageID", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    record = {
        "messageID": "example_id",
        "QStatus": -1,
    }
    table = db.Table("test-messages")
    table.put_item(Item=record)
    response_updated = table.update_item(
        Key={"messageID": "example_id"},
        UpdateExpression="set QStatus = :val",
        ExpressionAttributeValues={":val":-1},
        ReturnValues="ALL_NEW",
    )
    assert response_updated["Attributes"] == {"messageID": "example_id", "QStatus": -1}




