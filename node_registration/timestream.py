import boto3
from botocore.config import Config

my_config = Config(
    region_name = 'us-east-1',
    signature_version = 'v4',
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    }
)


def create_table(timestream_client,table_name):
    try:
        response = timestream_client.create_table(
            DatabaseName='Frizzle_Realtime_Database',
            TableName=table_name,
            RetentionProperties={
                'MemoryStoreRetentionPeriodInHours': 24,
                'MagneticStoreRetentionPeriodInDays': 1
            },
            Tags=[
                {
                    'Key': 'string',
                    'Value': 'string'
                },
            ],
            MagneticStoreWriteProperties={
                'EnableMagneticStoreWrites': False        
            }
        )
        return True
    except Exception:
        return False