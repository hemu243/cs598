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
    def __init__(self, zip_file, src_bucket, src_bucket_prefix, dst_bucket, dst_bucket_prefix):
        self.source_key = zip_file
        self.source_bucket = src_bucket
        self.src_bucket_prefix = src_bucket_prefix
        self.dst_bucket = dst_bucket
        self.dst_bucket_prefix = dst_bucket_prefix
        self.s3client = boto3.client('s3',
                     aws_access_key_id=os.environ.get("ACCESS_KEY"),
                     aws_secret_access_key=os.environ.get("SECRET_KEY"))
        self.SELECTED_FIELDS = [
        'Year', 'Month', 'Quarter', 'DayofMonth', 'DayOfWeek', 'FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
        'CRSDepTime', 'DepTime', 'DepDelay', 'DepDelayMinutes', 'CRSArrTime', 'ArrTime', 'ArrDelay', 'ArrDelayMinutes',
        'Cancelled'
    ]

    def download_and_extract_zipfile(self):
        """
            Downloads a zipfile from S3 and returns a list of csv file(s)
            containing in the zipfile.

        """
        logging.info('Downloading object %s/%s from S3.' % (self.source_bucket, self.source_key))
        image = self.s3client.get_object(
            Bucket=self.source_bucket,
            Key=self.source_key
        )

        failed_csv_files = []
        logging.info('Opening and retrieving CSV files from %s.' % self.source_key)
        tf = io.BytesIO(image["Body"].read())
        tf.seek(0)
        zipfile = ZipFile(tf, mode='r')
        for file in zipfile.namelist():
            if re.search('.csv$', file):
                logging.debug('CSV file %s found in object.' % file)
                with zipfile.open(file, 'r', ) as infile:
                    reader = csv.DictReader(io.TextIOWrapper(infile, 'utf-8'))
                    processed_data = self.process_csvfile(file, reader)
                    if processed_data.get('skipped_nr_lines') > 0:
                        logger.error("failed to process file=%s", file)
                        failed_csv_files.append({
                            "file": file,
                            "skipped_lines": processed_data.get('skipped_nr_lines')
                        })
                    self.write_new_csvfile(file, processed_data.get('data'))
                    logging.info('CSV file %s saved successfully.' %file)
        tf.close()
        zipfile = None
        tf = None
        image = None
        return failed_csv_files

    def process_csvfile(self, csvfile, csvreader):
        """
            Parses contents of a csvfile, filter fields
        """
        logging.info("Processing CSV file %s." % csvfile)
        output_data = []
        nr_lines = 0
        skipped_lines = 0
        for line in csvreader:
            try:
                output_line = {i: line[i] for i in self.SELECTED_FIELDS}
                output_data.append(output_line)
                nr_lines += 1
            except IndexError as e:
                logging.error("Error processing line %s in %s. (%s)" % ('|'.join(line), csvfile, str(e)))
                skipped_lines +=1
        logging.info("Processed %d lines from %s." % (nr_lines, csvfile))
        return {
            'csvfile': csvfile,
            'processed_nr_lines': nr_lines,
            'skipped_nr_lines': skipped_lines,
            'data': output_data
        }

    def write_new_csvfile(self, csvfile, data):
        """
            Will write the processed data as object in an S3 bucket.
        """
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        logging.debug('Putting CSV file %s onto S3.' % csvfile)
        self.s3client.put_object(
            Bucket=self.dst_bucket,
            Key="{0}/{1}".format(self.dst_bucket_prefix, csvfile),
            Body=output.getvalue(),
            ContentType='text/csv'
        )
        output.close()

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

# Example run
# if __name__ == "__main__":
#     p = ProcessZip("2005/On_Time_On_Time_Performance_2005_2.zip", "src-zips", "", "hsc4-clean-data", "test")
#     p.execute()

def processed_zip_files(file, src_bucket, dest_bucket, dest_prefix):
    """Returns how many numbers lie within `maximum` and `minimum` in a given `row`"""
    logger.info("calling async process for file=%s", file)
    p = ProcessZip(file, src_bucket, "", dest_bucket, dest_prefix)
    return p.execute()
