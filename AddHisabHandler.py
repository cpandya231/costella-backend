import boto3
import json
import uuid
import time

print('Loading function')


def add_hisab(payload):
    group_table = boto3.resource('dynamodb').Table("Group")
    item_id = str(uuid.uuid4())
    print(f"Adding item to Group table using {item_id}")
    group_table.put_item(
        Item={
            "groupId": item_id,
            "createdTime": int(time.time()),
            'name': payload["name"],
            'amount': payload["amount"],
            'category': payload["category"],
            "purchaseDate": payload["purchaseDate"]
        }
    )


def handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    add_hisab(event["payload"])
    return "success"



