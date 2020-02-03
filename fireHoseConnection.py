import boto3
import json
_aws_access_key = '<inser aws_access_key>'
_aws_secret_key = '<inser aws_secret_key>'
delivery_stream = '<inser delivery_stream>'
region = '<inser region>'


def get_aws_client(_aws_access_key, _aws_secret_key):
    return boto3.client(
        'firehose',
        aws_access_key_id=_aws_access_key,
        aws_secret_access_key=_aws_secret_key,
        region_name=region
    )


def put_records(client, records):

    try:
        response = client.put_record_batch(
            DeliveryStreamName=delivery_stream,
            Records=records
        )
        if response.get('FailedPutCount') != 0:
            print('Some records are missed')

        print('Pushed')
    except Exception as e:
        print('ERROR: {}'.format(e))
        return False

    return True


if __name__ == '__main__':

    current_bytes = 0
    batch_size = 50
    test_json = {'audio': 'such magic much wow'}
    kinesis_records = []
    push_batch = False
    client = get_aws_client(_aws_access_key, _aws_secret_key)
    while True:
        dict_to_bytes = json.dumps(test_json).encode('utf-8')
        kinesis_records.append({'Data':dict_to_bytes})
        current_bytes += len(dict_to_bytes)

        if current_bytes > 100*1000:    #0.25 MB
            push_batch = True

        if len(kinesis_records) == batch_size:
            push_batch = True

        if push_batch:
            if not put_records(client, kinesis_records):
                print('Batch failed to push')

            push_batch = False
            current_bytes = 0
            kinesis_records = []



