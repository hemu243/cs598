import boto3
import os
import subprocess
import re
import sys


class DynamodbHandler:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb',  aws_access_key_id=os.environ.get("ACCESS_KEY"),
                     aws_secret_access_key=os.environ.get("SECRET_KEY"))

    def create_table(self, name, key, key_type, range, range_type):
        if not key or not name or not range or not key_type or not range_type:
            raise ValueError('required args are missing')

        table = self.dynamodb.create_table(
            TableName=name,
            KeySchema=[
                {
                    'AttributeName': key,
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': range,
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': key,
                    'AttributeType': key_type
                },
                {
                    'AttributeName': range,
                    'AttributeType': range_type
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 4,
                'WriteCapacityUnits': 4
            }
        )
        return table

    def read_from_hdfs(self, table_name, hdfs_file, column_names):
        cat = subprocess.Popen(["hadoop", "fs", "-cat", hdfs_file], stdout=subprocess.PIPE)
        table = self.dynamodb.Table(table_name)
        for line in cat.stdout:
            line = line.decode('utf-8')
            if line.find("WARN util.NativeCodeLoader") > 0:
                continue
            print(line)
            values = re.split('\s+|\t+', line)
            print(values)
            item = dict()
            for index, value in enumerate(values):
                if value:
                     item[column_names[index]] = value
            print(item)
            self._put_item(table, item)

    def _put_item(self, table, item):
        response = table.put_item(
           Item=item
        )
        print(response)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception("Failed to store item={}".format(item))
        return response

table_column_names = {
        'capstone_2_1_airport_carrier_departure': ['airport', 'carrier', 'dep_delay'],
        'capstone_2_2_airport_departure': ['airport', 'airport_to', 'dep_delay'],
        'capstone_2_4_airport_arrival': ['airport', 'carrier', 'arr_delay'],
        'capstone_3_2_best_flight': ['airport_from', 'airport_to', 'given_date', 'am_or_pm', 'carrier', 'flight_num', 'departure_time', 'arr_delay'],
}

if __name__ == '__main__':
    if len(sys.argv) < 3:
        "<table_name> <file_path> path is required"
    table_name = sys.argv[1]
    file_path = sys.argv[2]
    if table_column_names.get(table_name):
        raise ValueError("table_name does not exist")
    handler = DynamodbHandler()


    # cap_2_1 = handler.create_table('capstone_2_1_airport_carrier_departure', 'airport', 'S', 'carrier', 'S')
    # print("Table status:", cap_2_1.table_status)
    #
    # cap_2_2 = handler.create_table('capstone_2_2_airport_departure', 'airport', 'S', 'airport_to', 'S')
    # print("Table status:", cap_2_2.table_status)
    #
    # cap_2_4 = handler.create_table('capstone_2_4_airport_arrival', 'airport', 'S', 'airport_to', 'S')
    # print("Table status:", cap_2_2.table_status)
    #
    # cap_3_2 = handler.create_table('capstone_3_2_best_flight', 'airport_from', 'S', 'airport_to', 'S')
    # print("Table status:", cap_2_2.table_status)

    # handler.read_from_hdfs("capstone_2_1_airport_carrier_departure",
    #                        "/Users/hchoudhary/Documents/Personal Documents/Coursera/MCS-DS-Program/CS598-cc/github/departure_by_carriers_CMI/part-r-00000",
    #                        column_names=['airport', 'carrier', 'dep_delay'])
    #best_flight_YUM_SLC3/part-r-00000
    handler.read_from_hdfs(table_name,
                           file_path,
                           column_names=table_column_names[table_name])


