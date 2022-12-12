
import boto3

def create_publisher_data_table():
    dynamodb = boto3.resource('dynamodb')
    table_name = 'publisher_data123'
    table = dynamodb.create_table(
        TableName = table_name,
        KeySchema = [
            {
                'AttributeName' : 'guid_parameter',
                'KeyType' : 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName' : 'guid_parameter',
                'AttributeType' : 'S'
            }
        ],
    BillingMode='PROVISIONED',
    ProvisionedThroughput={
        'ReadCapacityUnits': 100,
        'WriteCapacityUnits': 100000
    }
    )

create_publisher_data_table()