#!/usr/bin/env python
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
import warnings
import zipToParquet
import signal

stop_loading=False

def signal_handler(signal, frame):
    global stop_loading
    print('You pressed Ctrl+C. Cleaning up! please wait')
    stop_loading=True

signal.signal(signal.SIGINT, signal_handler)

warnings.simplefilter(action='ignore', category=FutureWarning)
counter=0
JOIN_TABLE='APReportStats'

logging.basicConfig(format="[%(asctime)s] %(levelname)s %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

if os.environ.get('MEDIATEL_DEBUG') is not None:
    logger.setLevel(logging.DEBUG)
    logger.info('set log level to DEBUG')

IGNORE_SIZE=128000
if os.environ.get('MEDIATEL_IGNORE_SIZE') is not None:
    IGNORE_SIZE=int(os.environ['MEDIATEL_IGNORE_SIZE'])
    logger.debug('set IGNORE_SIZE to {}'.format(IGNORE_SIZE))

SOURCE_BUCKET='mediatel-push'
if os.environ.get('MEDIATEL_SOURCE_BUCKET') is not None:
    SOURCE_BUCKET=os.environ['MEDIATEL_SOURCE_BUCKET']
    logger.debug('set SOURCE_BUCKET to {}'.format(SOURCE_BUCKET))

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

s3_client = boto3.client('s3')
s3 = s3fs.S3FileSystem()
s3s3 = boto3.resource('s3') 
mybucket=s3s3.Bucket(SOURCE_BUCKET)

logger.info("Processing dates: {} {}".format(start_date, end_date))
dates_to_parse = pd.date_range(start_date,end_date,freq=freq)
for this_date in dates_to_parse:
    if stop_loading:
        break
    for this_object in mybucket.objects.filter(Prefix='home/uploads/' + this_date.strftime(formatstring)):
        if stop_loading:
            break
        if 'GWC2BCQK.zip' in this_object.key:
            counter+=1 
            bucket = this_object.bucket_name
            key = this_object.key
            logger.info('Processing file: {} from bucket: {}'.format(key, bucket))
            if os.environ.get('MEDIATEL_DRYRUN') is None:
                download_path = '/tmp/{}{}'.format(uuid.uuid4(), '.zip')
                s3_client.download_file(bucket, key, download_path)
                logger.debug('downloaded from s3://{}/{} as {}'.format(bucket,key, download_path))
                if (os.stat(download_path).st_size < IGNORE_SIZE):
                    logger.info('Bypassing zip file due to small size. Threshold: {}- will retry later: {} | {}'.format(IGNORE_SIZE, bucket, key))
                    os.remove(download_path)
                    continue
                zipToParquet.extract_stats(download_path)
                os.remove(download_path)

if stop_loading:
    logger.info('Clean early exit - caught ctrl+C')

logger.info('Processed {} records'.format(counter))
