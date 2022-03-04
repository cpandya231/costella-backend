import calendar
import json
import time
from decimal import Decimal
import datetime
import boto3
from dateutil.relativedelta import relativedelta
from boto3.dynamodb.conditions import Key

print('Loading function')
group_table = boto3.resource('dynamodb').Table("Group")
pattern = "%Y-%m-%d %H:%M:%S"


def find_by_date(created_date, group_id):
    created_date_epoch = get_date_in_epoch(created_date, " 00:00:00")
    next_date_epoch = (datetime.datetime.fromtimestamp(created_date_epoch) + datetime.timedelta(days=1)).timestamp()
    return query_group_items(group_id, created_date_epoch, next_date_epoch)


def find_by_month(created_date, group_id):
    created_date_epoch = get_first_date_of_month(created_date)
    next_date_epoch = get_last_date_of_month(created_date)
    return query_group_items(group_id, created_date_epoch, next_date_epoch)


def query_group_items(group_id, created_date_epoch, next_date_epoch):
    response = group_table.query(
        KeyConditionExpression=Key('groupId').eq(group_id) & Key('createdTime').between(int(created_date_epoch),
                                                                                        int(next_date_epoch))
    )
    return response


def get_date_in_epoch(created_date, time_to_append):
    created_date = created_date + time_to_append
    created_date_epoch = calendar.timegm(time.strptime(created_date, pattern))
    return created_date_epoch


def get_first_date_of_month(created_date):
    return_val = datetime.datetime.fromtimestamp(get_date_in_epoch(created_date, " 00:00:00")).replace(day=1)
    return return_val.timestamp()


def get_last_date_of_month(created_date):
    return_val = datetime.datetime.utcfromtimestamp(get_date_in_epoch(created_date, " 23:59:59")) + relativedelta(
        day=31)
    return return_val.timestamp()


def find_by_id(group_id, created_date, search_by):
    print(f"Finding Hisabs in Group using {group_id}")
    # response = find_by_date(created_date, group_id)
    switcher = {
        "DATE": find_by_date,
        "MONTH": find_by_month,
    }
    func = switcher.get(search_by, find_by_date)
    response = func(created_date, group_id)

    print(f"Got response  length {len(response['Items'])}")
    return response['Items']


def get_hisab(payload):
    group_id = payload['pathParameters']["groupId"]
    created_date = payload['queryStringParameters']["createdDate"]
    search_by = ""
    if "searchBy" in payload['queryStringParameters']:
        search_by = payload['queryStringParameters']["searchBy"]

    print(
        f"Getting items from Group table using groupId: {group_id} and createdDate: {created_date} "
        f"and searchBy:{search_by}")
    response = []
    hisab_list = find_by_id(group_id, created_date, search_by)
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
    print(f"Adding item to Group table using groupId {group_id}")
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
