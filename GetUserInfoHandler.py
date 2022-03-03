import boto3
import json
import uuid
import time
from decimal import Decimal

from boto3.dynamodb.conditions import Key

print('Loading function')

hisab_table = boto3.resource('dynamodb').Table("HisabIdentity")


def get_user_info(payload):
    item_id = payload['pathParameters']["username"]
    result = find_by_id(item_id)[0]
    response = {
        'username': result['id'],
        'groups': result['groups'],
        'firstName': result['name']
    }
    return {
        "statusCode": 200,
        "body": json.dumps(response, cls=DecimalEncoder)
    }


def find_by_id(item_id):
    print(f"Finding item in HisabIdentity using {item_id}")
    response = hisab_table.query(
        KeyConditionExpression=Key('id').eq(item_id)
    )
    print(f"Got response {response['Items']}")
    return response['Items']


def add_user(payload):
    body = json.loads(payload['body'])
    print(f"Adding item to HisabIdentity table using {body}")
    response = find_by_id(body['username'])
    if len(response) == 0:
        hisab_table.put_item(
            Item={
                "id": body['username'],
                "secondaryId": "User info",
                "groups": [

                ],
                "createdTime": int(time.time()),
                "name": body['firstName']
            }
        )

        return {
            "statusCode": 200,
            "body": json.dumps("User added successfully!")
        }
    else:
        return {
            "statusCode": 409,
            "body": json.dumps(f"User with username {body['username']} already exists")
        }


def handler(event, context):
    switcher = {
        "GET": get_user_info,
        "POST": add_user,
    }
    func = switcher.get(event['httpMethod'], lambda: 'Invalid method')
    return func(event)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)
