import json
import time
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

print('Loading function')
group_table = boto3.resource('dynamodb').Table("Group")


def find_by_id(group_id):
    print(f"Finding Hisabs in Group using {group_id}")
    response = group_table.query(
        KeyConditionExpression=Key('groupId').eq(group_id)
    )
    print(f"Got response {response['Items']}")
    return response['Items']


def get_hisab(payload):
    group_id = payload['pathParameters']["groupId"]
    print(f"Adding item to Group table using {group_id}")
    response = []
    hisab_list = find_by_id(group_id)
    for hisab in hisab_list:
        response.append({
            "groupId": group_id,
            'name': hisab["name"],
            'amount': hisab["amount"],
            'category': hisab["category"],
            "purchaseDate": hisab["purchaseDate"]
        })

    return {
        "statusCode": 200,
        "body": json.dumps(response, cls=DecimalEncoder)
    }


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def add_hisab(payload):
    body = json.loads(payload['body'])
    group_id = body['groupId']
    print(f"Adding item to Group table using {group_id}")
    group_table.put_item(
        Item={
            "groupId": group_id,
            "createdTime": int(time.time()),
            'name': body["name"],
            'amount': body["amount"],
            'category': body["category"],
            "purchaseDate": body["purchaseDate"]
        }
    )

    response = {
        "groupId": group_id,
        "message": "Hisab added successfully"
    }
    return {
        "statusCode": 200,
        "body": json.dumps(response)
    }


def handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    switcher = {
        "GET": get_hisab,
        "POST": add_hisab,
    }
    func = switcher.get(event['httpMethod'], lambda: 'Invalid method')
    return func(event)
