import argparse
from multiprocessing import Process, SimpleQueue

import boto3


class DynamoDBScanner(Process):
    def __init__(self, scanner_id, total_scanners, source_table_name, target_table_name, results_queue):
        super(DynamoDBScanner, self).__init__()
        self.scanner_id = scanner_id
        self.total_scanners = total_scanners
        self.source_table_name = source_table_name
        self.target_table_name = target_table_name
        self.dynamodb_resource = boto3.resource('dynamodb')
        self.source_table = self.dynamodb_resource.Table(source_table_name)
        self.target_table = self.dynamodb_resource.Table(target_table_name)
        self.results_queue = results_queue

    def run(self):
        print(f"Starting scanner {self.scanner_id} of {self.total_scanners}")
        total_items = 0
        kwargs = {}
        counter = 0
        with self.target_table.batch_writer() as batch:
            while True:
                results = self.source_table.scan(
                    Segment=self.scanner_id,
                    TotalSegments=self.total_scanners,
                    **kwargs
                )
                items = results['Items']
                for item in items:
                    batch.put_item(Item=item)
                    counter += 1
                    if counter >= 1000:
                        counter = 0
                        print(f"Scanner {self.scanner_id} - Written {total_items} items to {self.target_table_name}")

                total_items += len(items)

                if 'LastEvaluatedKey' not in results:
                    break

                kwargs = {'ExclusiveStartKey': results['LastEvaluatedKey']}

        print(f"Scanner {self.scanner_id} - Total {total_items} items written to {self.target_table_name}")
        self.results_queue.put(total_items)


def main(source_table_name, target_table_name, num_threads, create_table, verbose_copy):

    dynamodb_client = boto3.client('dynamodb')
    results_queue = SimpleQueue()

    try:
        source_table_desc = dynamodb_client.describe_table(TableName=source_table_name)

        try:
            target_table_desc = dynamodb_client.describe_table(TableName=target_table_name)
            print(f"Target table {target_table_name} found.")

            if source_table_desc['Table']['KeySchema'] != target_table_desc['Table']['KeySchema']:
                raise Exception("Key Schemas on source and target tables are different.")
        except dynamodb_client.exceptions.ResourceNotFoundException as e:
            print(f"Target table {target_table_name} not found.")
            if create_table:
                try:
                    create_dynamodb_table(
                        dynamodb_client=dynamodb_client,
                        table_name=target_table_name,
                        source_table_desc=source_table_desc['Table'],
                        verbose_copy=verbose_copy
                    )
                except Exception as e:
                    print(f"Something went wrong while creating {target_table_name} :(")
                    raise e
            else:
                print(f"Specified DynamoDB table({target_table_name}) does not exist.")

        print("Starting to sync.")

        scanners = [
            DynamoDBScanner(i, num_threads, source_table_name, target_table_name, results_queue)
            for i in range(0, num_threads)
        ]

        for scanner in scanners:
            scanner.start()

        for scanner in scanners:
            scanner.join()

        total = 0
        for i in range(0, num_threads):
            total += results_queue.get()

    except dynamodb_client.exceptions.ResourceNotFoundException as e:
        print(f"Source table = {source_table_name} does not exist.")
    except Exception as e:
        print(f"Something went wrong while running the script :(")
        raise e


def create_dynamodb_table(dynamodb_client, table_name, source_table_desc, verbose_copy=True):

    print(f"Creating DynamoDB table: {table_name}")
    billing_mode = source_table_desc['BillingModeSummary']['BillingMode']
    key_schema = source_table_desc['KeySchema']
    attr_schema = source_table_desc['AttributeDefinitions']
    provisioned_throughput = {
        key: value
        for key, value in source_table_desc['ProvisionedThroughput'].items()
        if key in ['ReadCapacityUnits', 'WriteCapacityUnits'] if value > 0
    }
    kwargs = {
        'TableName': table_name,
        'KeySchema': key_schema,
        'AttributeDefinitions': attr_schema,
        'BillingMode': billing_mode
    }
    if provisioned_throughput:
        kwargs['ProvisionedThroughput'] = provisioned_throughput

    if verbose_copy:
        params = [('StreamSpecification', 'GlobalSecondaryIndexes', 'LocalSecondaryIndexes')]

        for param in params:
            if param in source_table_desc:
                kwargs[param] = source_table_desc[param]
        if 'SSEDescription' in source_table_desc:
            sse_desc = source_table_desc['SSEDescription']
            kwargs['SSESpecification'] = {
                'Enabled': sse_desc['Status'] == 'ENABLED',
                'SSEType': sse_desc['SSEType'],
                'KMSMasterKeyId': sse_desc['KMSMasterKeyArn']
            }
        kwargs['Tags'] = _get_table_tags(dynamodb_client, source_table_desc['TableArn'])

    dynamodb_client.create_table(**kwargs)

    print(f"Waiting for {table_name} table creation completed.")

    waiter = dynamodb_client.get_waiter('table_exists')
    waiter.wait(
        TableName=table_name,
        WaiterConfig={
            'Delay': 10,
            'MaxAttempts': 100
        }
    )


def _get_table_tags(dynamodb_client, table_arn):
    tags = []
    kwargs = {}
    while True:
        response = dynamodb_client.list_tags_of_resource(
            ResourceArn=table_arn,
            **kwargs
        )
        tags += response['Tags']
        if 'NextToken' not in response:
            break

        kwargs['NextToken'] = response['NextToken']

    tags.append(
        {'Source_Table': table_arn}
    )

    return tags


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Copies one DynamoDB table to another')

    parser.add_argument('-s', '--source', metavar='source', type=str, nargs='?', required=True,
                        help='Source DynamoDB table to be scanned.')

    parser.add_argument('-t', '--target', metavar='target', type=str, nargs='?', required=True,
                        help='Target DynamoDB table name')

    parser.add_argument('-n', '--num-threads', metavar='threads', type=int, nargs='?', default=5,
                        help='Number of parallel threads/processes to scan the DynamoDB table.')

    parser.add_argument('-c', '--create-table', action='store_true',
                        help='Creates the target table if not exists.')

    parser.add_argument('-v', '--verbose-copy', action='store_true',
                        help='Whether additional information such as Streams, Tags, Encryption should be copied.')

    args = parser.parse_args()

    main(args.source, args.target, args.num_threads, args.create_table, args.verbose_copy)
