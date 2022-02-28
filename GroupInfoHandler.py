import boto3
import json
import uuid
import time
from decimal import Decimal

from boto3.dynamodb.conditions import Key, Attr

print('Loading function')
ddb = boto3.resource('dynamodb')
hisab_table = ddb.Table("HisabIdentity")


def find_group_by_groupname(group_name, username):
    print(f"Finding item in HisabIdentity using {group_name}")
    scan_kwargs = {
        'FilterExpression': Attr('name').eq(group_name) & Attr('users').contains(username)
    }
    response = hisab_table.scan(**scan_kwargs)
    print(f"Got response {response['Items']}")
    return response['Items']


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
    if len(result) == 0:
        add_user(username)
    else:
        for group_id in result["groups"]:
            group = find_by_id(group_id)
            response.append({
                'groupId': group['id'],
                'groupName': group['name'],

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
    user = find_by_id(username)
    groups = user['groups']
    groups.append(group_id)
    hisab_table.update_item(
        Key={
            'id': username,
            'secondaryId': "User info"
        },
        UpdateExpression="set groups= :groups",
        ExpressionAttributeValues={
            ':groups': groups
        },
        ReturnValues="UPDATED_NEW")


def add_group(payload):
    body = json.loads(payload['body'])
    print(f"Adding group to HisabIdentity table using {body}")
    username = payload['pathParameters']["username"]
    group_info = find_group_by_groupname(body['groupName'], username)

    if len(group_info) == 0:
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
                "name": body['groupName']
            }
        )

        add_user_to_group(group_id, username)

        response = {
            "groupId": group_id,
            "message": "Group created successfully!"
        }
        return {
            "statusCode": 200,
            "body": json.dumps(response)
        }

    else:
        return {
            "statusCode": 409,
            "body": json.dumps(f"Group with group name {body['groupName']} already exists")
        }


def handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
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
