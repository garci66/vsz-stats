#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
import boto3
import botocore
import sys
import os
import pandas as pd
import numpy as np
import re
import uuid
from zipfile import ZipFile
from cStringIO import StringIO
import glob
import logging
import s3fs
import fastparquet as fp
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
JOIN_TABLE='APReportStats'
EVENT_TEMPLATE='{"Records": [{"eventVersion": "2.0","eventSource": "aws:s3","awsRegion": "us-east-2","eventTime": "1970-01-01T00:00:00.000Z","eventName": "ObjectCreated:Put","userIdentity": {"principalId": "AIDAJDPLRKLG7UEXAMPLE"},"requestParameters": {"sourceIPAddress": "127.0.0.1"},"responseElements": {"x-amz-request-id": "C3D13FE58DE4C810","x-amz-id-2": "FMyUVURIY8/IgAtTv8xRjskZQpcIZ9KG4V5Wp6S7S/JRWeUWerMUE5JgHvANOjpD"},"s3": {"s3SchemaVersion": "1.0","configurationId": "testConfigRule","bucket": {"name": "mediatel-vsz-push","ownerIdentity": {"principalId": "A3NL1KOZZKExample"},"arn": "arn:aws:s3:::mediatel-vsz-push"},"object": {"key": "$$KEYNAME$$","size": 1024,"eTag": "d41d8cd98f00b204e9800998ecf8427e","versionId": "096fKKXTRTtl3on89fVO.nfljtsv6qko"}}}]}'

logging.basicConfig(format="[%(asctime)s] %(levelname)s %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

if os.environ.get('MEDIATEL_DEBUG') is not None:
    logger.setLevel(logging.DEBUG)
    logger.info('set log level to DEBUG')

lambda_arn=None

if os.environ.get('MEDIATEL_LAMBDA') is not None:
    #lambda_arn='arn:aws:lambda:us-east-2:233108096105:function:zipToParquet'
    lambda_arn='arn:aws:lambda:us-east-1:233108096105:function:zipToParquet'

TARGET_BUCKET='mediatel-parquet'
if os.environ.get('MEDIATEL_PARQUET_BUCKET') is not None:
    TARGET_BUCKET=os.environ['MEDIATEL_PARQUET_BUCKET']
    logger.debug('set TARGET_BUCKET to {}'.format(TARGET_BUCKET))

IGNORE_SIZE=128000
if os.environ.get('MEDIATEL_IGNORE_SIZE') is not None:
    IGNORE_SIZE=int(os.environ['MEDIATEL_IGNORE_SIZE'])
    logger.debug('set IGNORE_SIZE to {}'.format(IGNORE_SIZE))

PREPEND_PATH=""
if os.environ.get('MEDIATEL_PREPEND_PATH') is not None:
    PREPEND_PATH=os.environ['MEDIATEL_PREPEND_PATH']+'/'
    logger.debug('set PREPEND_PATH to {}'.format(PREPEND_PATH))


s3_client = boto3.client('s3')
s3 = s3fs.S3FileSystem()
s3s3 = boto3.resource('s3')
mybucket=s3s3.Bucket('mediatel-vsz-push')

def s3_key_exists(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            # Something else has gone wrong.
            raise
    else:
        return True


def extract_stats(infile):
    #APREportsStars MUST be the first to be processed!
    tables_to_process=[JOIN_TABLE, 'APReportBinWlan','FlowMessage','APReportBinClient']
    df_array= {}
    good_headers={}

    myopen = s3.open
    nop = lambda *args, **kwargs: None

    for this_table in tables_to_process:
        df_array[this_table]=pd.DataFrame()
        good_headers[this_table]=None

    logger.debug("Processing zip: {}".format(infile))
    for this_table in tables_to_process:
        my_zip = ZipFile(infile)
        file_pat=re.compile("^.+" + this_table + "_.+\.csv")
        frame = pd.DataFrame()
        list_ = []
        total_count=0

        my_files=sorted(filter(file_pat.search, my_zip.namelist()))
        logger.debug("Will iterate to load through: {}".format(my_files))

        for filename in my_files:
            #print "Processing filename: ", filename
            if good_headers[this_table] is None:
                header_files=my_files
                logger.debug("Will iterate to find header through: {}".format(header_files))
                for header_filename in header_files:
                    fake_header_file=StringIO(my_zip.read(header_filename))
                    this_header_row=fake_header_file.readline().rstrip()
                    logger.debug("Trying to find headers in: {}".format(header_filename))
                    #fake_header_file.seek(0) #Rewind file for full read
                    if 'sampleTime' in this_header_row:
                        good_headers[this_table]=this_header_row.split(',')
                        logger.debug("Header found in file: {}".format(header_filename))
                        break
                    else:
                        logger.debug("couldnt find header, trying next file! {}".format(header_filename))
            
            if good_headers[this_table] is None:
                logger.error("Skipping file No header and none found in all files for table: {}".format(this_table))
                break

            my_fake_file=StringIO(my_zip.read(filename))
            this_header_row=my_fake_file.readline()
            my_fake_file.seek(0) #Rewind file for full read
            
            #check total number of lines in file (only in DEBUG run)
            if logger.getEffectiveLevel() == logging.DEBUG:
                for i, l in enumerate(my_fake_file):
                    pass
                total_count=i+1
                logger.debug("File {} with {} lines read via raw read".format(filename, total_count))
                my_fake_file.seek(0)

            if 'sampleTime' in this_header_row:
                logger.debug("reading file with good header: {}".format(filename))
                tdf=pd.read_csv(my_fake_file, header=0, names=good_headers[this_table], index_col=False)
            else:
                logger.debug("reading file with missing header: {}".format(filename))
                tdf=pd.read_csv(my_fake_file, header=None, names=good_headers[this_table], index_col=False)
            if len(tdf)>1:
                logger.debug("File {} with {} rows read by pandas".format(filename, len(tdf)))
                list_.append(tdf)

        if len(list_)>0:
            df = pd.concat(list_)
            df_array[this_table]=df_array[this_table].append(df)

        logger.debug("Finished loading data for table: {}".format(this_table))

        if (this_table==JOIN_TABLE):
            df_aps=df_array[JOIN_TABLE][['ap','deviceName','domain_id','domain_name','zone_name','apgroup_name']]
            df_aps=df_aps.drop_duplicates(subset='ap')

        if 'apMac' in df_array[this_table].columns:
            df_array[this_table].rename(index=str,columns={'apMac':'ap'},inplace=True)
        
        if this_table is not JOIN_TABLE:
            df_array[this_table]=df_array[this_table].merge(df_aps,on='ap',how='left')

        logger.debug("Finished building left_join for table: {}".format(this_table))

        df_array[this_table]=df_array[this_table][df_array[this_table].columns[~df_array[this_table].columns.str.contains('ipv6', case=False, regex=False)]]
        #print "Processing dates and partitions: ", this_table
        df_array[this_table]['sampleTime']=pd.to_datetime(df_array[this_table]['sampleTime'],unit='s')
        df_array[this_table]['partitionYear']=df_array[this_table]['sampleTime'].dt.year
        df_array[this_table]['partitionMonth']=df_array[this_table]['sampleTime'].dt.month
        df_array[this_table]['partitionDay']=df_array[this_table]['sampleTime'].dt.day
        df_array[this_table]['partitionHour']=df_array[this_table]['sampleTime'].dt.hour
        
        parquet_s3_path=TARGET_BUCKET + '/' + PREPEND_PATH + this_table + '.parquet/'

        create_file=s3_key_exists(TARGET_BUCKET, PREPEND_PATH + this_table + '.parquet/_metadata')

        logger.debug("Existing parquet file found for table {}: {}".format(this_table, create_file))
        logger.debug("Saving table: {} with fields: {}".format(this_table, df_array[this_table].columns))
        fp.write(parquet_s3_path, df_array[this_table], file_scheme='hive', append=create_file,
            partition_on=['domain_id','partitionYear','partitionMonth','partitionDay','partitionHour' ],
            open_with=myopen, mkdirs=nop )

        logger.debug("Finished saving table: {}".format(this_table))
        del df_array[this_table]


def trigger_lambda(key):
    lambda_client = boto3.client('lambda', region_name='us-east-1', config=botocore.config.Config(read_timeout=100,retries={'max_attempts': 0}))
    one_event=EVENT_TEMPLATE.replace('$$KEYNAME$$',key)
    if os.environ.get('MEDIATEL_DRYRUN') is None:
        lambda_client.invoke(FunctionName=lambda_arn, Payload=one_event)
    else:
        logger.info('Would have called lambda {} with this event:\n {}'.format(lambda_arn, one_event))



counter=0

start_date = sys.argv[1]
if (len(sys.argv)==3):
    end_date= sys.argv[2]
else:
    end_date=start_date

if (len(end_date)>8):
    freq='H'
    formatstring='%Y%m%d%H'
else:
    freq='D'
    formatstring='%Y%m%d'

logger.info("Processing dates: {} {}".format(start_date, end_date))
dates_to_parse = pd.date_range(start_date,end_date,freq=freq)
for this_date in dates_to_parse:
    for this_object in mybucket.objects.filter(Prefix='home/uploads/' + this_date.strftime(formatstring)):
        if 'GWC2BCQK.zip' in this_object.key:
            counter+=1
            bucket = this_object.bucket_name
            key = this_object.key
            logger.info('Processing file: {} from bucket: {}'.format(key, bucket))
            if lambda_arn is None:
                if os.environ.get('MEDIATEL_DRYRUN') is None:
                    download_path = '/tmp/{}{}'.format(uuid.uuid4(), '.zip')
                    s3_client.download_file(bucket, key, download_path)
                    logger.debug('downloaded from s3://{}/{} as {}'.format(bucket,key, download_path))
                    if (os.stat(download_path).st_size < IGNORE_SIZE):
                        logger.info('Bypassing zip file due to small size. Threshold: {}- will retry later: {} | {}'.format(IGNORE_SIZE, bucket, key))
                        os.remove(download_path)
                        continue
                    extract_stats(download_path)
                    os.remove(download_path)
            else:
                trigger_lambda(key)


logger.info('Processed {} records'.format(counter))



