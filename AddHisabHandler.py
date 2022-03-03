import calendar
import json
import time
from decimal import Decimal
import datetime
import boto3
from boto3.dynamodb.conditions import Key

print('Loading function')
group_table = boto3.resource('dynamodb').Table("Group")
pattern = "%Y-%m-%d %H:%M:%S"


def find_by_id(group_id, created_date):
    print(f"Finding Hisabs in Group using {group_id}")
    created_date_epoch = get_date_in_epoch(created_date, " 00:00:00")

    next_date_epoch = (datetime.datetime.fromtimestamp(created_date_epoch) + datetime.timedelta(days=1)).timestamp()

    response = group_table.query(
        KeyConditionExpression=Key('groupId').eq(group_id) & Key('createdTime').between(int(created_date_epoch),
                                                                                        int(next_date_epoch))
    )
    print(f"Got response  length {len(response['Items'])}")
    return response['Items']


def get_date_in_epoch(created_date, time_to_append):
    created_date = created_date + time_to_append
    created_date_epoch = calendar.timegm(time.strptime(created_date, pattern))
    return created_date_epoch


def get_hisab(payload):
    group_id = payload['pathParameters']["groupId"]
    created_date = payload['queryStringParameters']["createdDate"]
    print(f"Getting items from Group table using groupId: {group_id} and createdDate: {created_date}")
    response = []
    hisab_list = find_by_id(group_id, created_date)
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
    purchase_time = datetime.datetime.utcnow().strftime(" %H:%M:%S")
    created_time = get_date_in_epoch(body["purchaseDate"], purchase_time)
    group_table.put_item(
        Item={
            "groupId": group_id,
            "createdTime": created_time,
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
    switcher = {
        "GET": get_hisab,
        "POST": add_hisab,
    }
    func = switcher.get(event['httpMethod'], lambda: 'Invalid method')
    return func(event)
