import boto3

class DynamodbHandler:
    def __init__(self,table_name):
        self.db = boto3.resource('dynamodb')
        self.client = boto3.client('dynamodb')
        self.table = self.db.Table(table_name)
    
    def test(self):
        return "Yes its working"
    
    def insert(self,node_sensor_values):
        try:
            row = {i:node_sensor_values[i] for i in node_sensor_values.keys()}
            with self.table.batch_writer() as writer:
                writer.put_item(Item=row)
            return {"Status":"successful"}
        except Exception as e:
            return {"Status":"failed", "reason":e}

    def view_database(self):
        try:
            response = self.client.scan(
                TableName = 'pes_node_2',
            )
            return response
        except Exception as e:
            return {"Status":"Failed","Reason":e}
    # def display_values(self):



