class DynamoWriter:
    TABLE_NAME = 'publisher_data'
    def __init__(self, table):
        self.table = table


    def write_to_dynamo_table(table, request):
        table.put_item(
            Item=request)