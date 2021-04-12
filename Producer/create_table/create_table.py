import boto3
client = boto3.client('dynamodb')
topic_name = "node-004"
try:
    response = client.create_table(
        AttributeDefinitions=[
        {
        'AttributeName': 'time-stamp',
        'AttributeType': 'S'
        },
        ],
    TableName=topic_name,
    KeySchema=[
        {
            'AttributeName': 'time-stamp',
            'KeyType': 'HASH'
        },
    ],
    BillingMode='PAY_PER_REQUEST',
    StreamSpecification={
        'StreamEnabled': False,
    },
    SSESpecification={
        'Enabled': True ,
        'SSEType': 'KMS',
        'KMSMasterKeyId': '57780289-75ee-4f41-bdf8-0d4f43291fae'
    }
    )
    print(response)
except Exception as e:
    print(e)
