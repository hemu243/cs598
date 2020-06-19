#!/usr/bin/python
import re
import json
import csv
import io
import os
from zipfile import ZipFile
import boto3
import logging


# Covert to convert Latin data to utf decode("iso-8859-1")
logger = logging.getLogger()
if 'DEBUG' in os.environ and os.environ['DEBUG'] == 'true':
    logger.setLevel(logging.DEBUG)
    logger.debug('debug mode enabled.')
else:
    logger.setLevel(logging.INFO)


class ProcessZip:
    def __init__(self, zip_file, src_bucket, src_bucket_prefix, dst_bucket, dst_bucket_prefix, is_local=False):
        self.source_key = zip_file
        self.source_bucket = src_bucket
        self.src_bucket_prefix = src_bucket_prefix
        self.dst_bucket = dst_bucket
        self.dst_bucket_prefix = dst_bucket_prefix
        self.s3client = boto3.client('s3',
                     aws_access_key_id=os.environ.get("ACCESS_KEY"),
                     aws_secret_access_key=os.environ.get("SECRET_KEY"))
        self.is_local = is_local

    def download_and_extract_zipfile(self):
        """
            Downloads a zipfile from S3 and returns a list of csv file(s)
            containing in the zipfile.

        """
        failed_csv_files = []
        if not self.is_local:
            logging.info('Downloading object %s/%s from S3.' % (self.source_bucket, self.source_key))
            image = self.s3client.get_object(
                Bucket=self.source_bucket,
                Key=self.source_key
            )
            logging.info('Opening and retrieving CSV files from %s.' % self.source_key)
            tf = io.BytesIO(image["Body"].read())
            tf.seek(0)
            zipfile = ZipFile(tf, mode='r')
        else:
            zipfile = ZipFile('/Users/hchoudhary/Documents/Personal Documents/Coursera/MCS-DS-Program/CS598-cc/github/hemu243/cs598/data/datat/zip/On_Time_On_Time_Performance_2005_2.zip', mode='r')

        for file in zipfile.namelist():
            output = io.StringIO()
            try:
                with zipfile.open(file, 'r') as infile:
                    reader = csv.reader(io.TextIOWrapper(infile, 'utf-8'))
                    writer = csv.writer(output)
                    for line in reader:
                        writer.writerow(line)
                # Store to s3
                if self.is_local:
                    logging.debug('Putting CSV file %s onto local.' % file)
                    with open(file, 'w') as f:
                        f.writelines(output.getvalue())
                else:
                    logging.debug('Putting CSV file %s onto S3.' % file)
                    self.s3client.put_object(
                    Bucket=self.dst_bucket,
                    Key="{0}/{1}".format(self.dst_bucket_prefix, file),
                    Body=output.getvalue(),
                    ContentType='text/csv')
            except Exception as e:
                logging.exception(e)
                failed_csv_files.append(file)
            finally:
                output.close()
        if not self.is_local:
            tf.close()
        return failed_csv_files

    def execute(self):
        msg = "success"
        failed_files = []
        try:
            failed_files = self.download_and_extract_zipfile()
        except Exception as e:
            logger.exception(e)
            msg = repr(e)
        finally:
            return {
                "key": self.source_key,
                "failed_csv": failed_files,
                "message": msg
            }


def lambda_handler(event, context):
    """
        event is expected as dictionary
        {
            'src-bucketname': <s3-bucket>,
            'key': <location-of-zipfile>,
            'dst-bucketname': <s3-bucket>,
            'dst-key-prefix': <prefix>
        }
    """
    logging.info('Handler invoked for %s/%s' % (event['src-bucketname'], event['key']))
    p = ProcessZip(event['key'], event['src-bucketname'], "", event['dst-bucketname'], event['dst-key-prefix'])
    result = p.execute()
    validation_ok = 1
    if result.get('message') != 'success':
        logger.error("Failed to process zip, file=%s, failed_csv=%s, msg=%s", result['key'], result['failed_csv'],
                     result["message"])
        validation_ok = 0

    return {
        'status': 'OK' if validation_ok == 1 else 'ERROR'
    }

# # Example run
# if __name__ == "__main__":
#     p = ProcessZip("2005/On_Time_On_Time_Performance_2005_2.zip", "src-zips", "", "hsc4-clean-data", "test2", is_local=True)
#     p.execute()


def processed_zip_files(file, src_bucket, dest_bucket, dest_prefix):
    """Returns how many numbers lie within `maximum` and `minimum` in a given `row`"""
    logger.info("calling async process for file=%s", file)
    p = ProcessZip(file, src_bucket, "", dest_bucket, dest_prefix)
    return p.execute()
