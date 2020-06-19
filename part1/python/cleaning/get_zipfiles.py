# !/usr/bin/python

import re, json, os
import boto3
import logging

lambda_client = boto3.client('lambda',
                             aws_access_key_id=os.environ.get("ACCESS_KEY"),
                             aws_secret_access_key=os.environ.get("SECRET_KEY"))

logger = logging.getLogger()

if 'DEBUG' in os.environ and os.environ['DEBUG'] == 'true':
    logger.setLevel(logging.DEBUG)
    logger.debug('debug mode enabled.')
else:
    logger.setLevel(logging.INFO)


class ListZipFiles:
    def __init__(self, bucket, prefix):
        self.s3client = boto3.client('s3',
                                     aws_access_key_id=os.environ.get("ACCESS_KEY"),
                                     aws_secret_access_key=os.environ.get("SECRET_KEY"))
        self.bucket = bucket
        self.prefix = prefix

    def execute(self):
        resp = self.s3client.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)
        if resp['KeyCount'] == 0:
            logger.warning('No files found.')
            return []
        files = []
        for f in resp['Contents']:
            logger.info('Processing key=%s.' % f['Key'])
            if re.search('.zip$', f['Key']):
                logger.info('Found zip file %s.' % f['Key'])
                files.append(f['Key'])
        return files


def lambda_handler(event, context):
    """
        event should contain:
        - src-bucketname
        - src-prefix
        - dst-bucketname
        - dst-prefix
        - zip-extract-fun - function name to extract each zip ex - clean_zipfile
    """
    logger.info('Invoking handler.')
    nr_success = 0
    nr_failed = 0
    files = ListZipFiles(event['src-bucketname'], event['src-prefix']).execute()
    for f in files:
        args = {
            'src-bucketname': event['src-bucketname'],
            'key': f,
            'dst-bucketname': event['dst-bucketname'],
            'dst-key-prefix': event['dst-prefix']
        }
        logger.info('Starting async processing of %s...' % f)
        results = lambda_client.invoke_async(
            FunctionName=event['zip-extract-fun'],
            InvokeArgs=json.dumps(args)
        )
        if results['Status'] == 202:
            logger.info('Lambda invoked successfully.')
            nr_success += 1
        else:
            logger.error('Failed to start lambda for %s.' % f)
            nr_failed += 1
        logger.info('Processing of ZIP files started.')
        logger.info('%d lambda started successfully' % nr_success)
        logger.info('%d lambda failed to start.' % nr_failed)

# if __name__ == "__main__":
#     list_files = ListZipFiles('hsc4-clean-data', 'test').execute()
#     print(list_files)
#     print(len(list_files))
