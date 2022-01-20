import boto3
import json
import uuid
import time
from decimal import Decimal

from boto3.dynamodb.conditions import Key

print('Loading function')


def get_user_info(payload):
    hisab_table = boto3.resource('dynamodb').Table("HisabIdentity")
    item_id = payload['pathParameters']["username"]
    print(f"Finding item in HisabIdentity using {item_id}")
    response = hisab_table.query(
        KeyConditionExpression=Key('id').eq(item_id)
    )
    print(f"Got response {response['Items']}")
    return {
        "statusCode": 200,
        "body": json.dumps(response['Items'], cls=DecimalEncoder)
    }


def handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    return get_user_info(event)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)
