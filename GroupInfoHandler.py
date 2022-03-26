import boto3
import json
import uuid
import time
from decimal import Decimal

from boto3.dynamodb.conditions import Key, Attr

print('Loading function')
ddb = boto3.resource('dynamodb')
hisab_table = ddb.Table("HisabIdentity")


def find_by_id(item_id):
    print(f"Finding User in HisabIdentity using {item_id}")
    response = hisab_table.query(
        KeyConditionExpression=Key('id').eq(item_id)
    )
    print(f"Got response {response['Items']}")
    if len(response["Items"]) > 0:
        return response["Items"][0]
    else:
        return []


def get_group_for_user(payload):
    username = payload['pathParameters']["username"]
    result = find_by_id(username)
    response = []
    group_id = ""
    group_name = ""
    if len(result) == 0:
        add_user(username)
        new_group = add_group(username)
        group_id = new_group["groupId"]
        group_name = new_group["groupName"]
    else:
        for group_id in result["groups"]:
            group = find_by_id(group_id)
            group_id = group['id']
            group_name = group['name']

    response.append({
        'groupId': group_id,
        'groupName': group_name,

    })
    return {
        "statusCode": 200,
        "body": json.dumps(response, cls=DecimalEncoder)
    }


def add_user(username):
    print(f"Adding item to HisabIdentity table using {username}")

    hisab_table.put_item(
        Item={
            "id": username,
            "secondaryId": "User info",
            "groups": [

            ],
            "createdTime": int(time.time()),

        }
    )

    return {
        "statusCode": 200,
        "body": json.dumps("User added successfully!")
    }


def add_user_to_group(group_id, username):
    hisab_table.update_item(
        Key={
            'id': username,
            'secondaryId': "User info"
        },
        UpdateExpression="set groups= :groups",
        ExpressionAttributeValues={
            ':groups': [group_id]
        },
        ReturnValues="UPDATED_NEW")


def add_group(username):
    print(f"Adding group to HisabIdentity table using")

    group_id = "g_" + str(uuid.uuid4())
    print(f"Creating group with id {group_id}")
    hisab_table.put_item(
        Item={
            "id": group_id,
            "secondaryId": "Group info",
            "users": [
                username
            ],
            "createdTime": int(time.time()),
            "name": 'Personal'
        }
    )

    add_user_to_group(group_id, username)

    response = {
        "groupId": group_id,
        "groupName": 'Personal'
    }
    return response


def handler(event, context):
    switcher = {
        "GET": get_group_for_user,
        "POST": add_group,
    }
    func = switcher.get(event['httpMethod'], lambda: 'Invalid method')
    return func(event)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)
