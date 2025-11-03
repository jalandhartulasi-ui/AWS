#!/usr/bin/env python
"""DSA NAS Loader.

AWS Glue script to load NAS data to Redshft

This job will:
 - Connect to Glue
 - Configure Redshift sink
 - Select `Datasource` implementation based on params
 - Create `Transformer` and invoke function sequence to:
    - write all new NAS data for this batch to staging tables

Parameters
----------
BUCKET_ARG : str
    The s3 bucket of NAS data
FILTER_ARG : str
    TODO: Currently not used
GRANULARITY_ARG : str
    The time granularity of the data batch
        - `today`
        - `yesterday`
        - `hour`
        - `all`
        - TODO: bookmark
DATASOURCE_ARG : str
    The name of the source in the CDLZ <source>.<feed> datafeed definition
DATAFEED_ARG : str
    The name of the feed in the CDLZ <source>.<feed> datafeed definition
RELATIONALIZE_ROOT_ARG : str
    Set to True if the data needs to be flattened
SOURCE_TOPIC_ARG : str
    The name of the topic that Kafka Connect consumes from.
    Used when identifying the source path on S3.
RS_DATABASE_ARG : str
    The name of the database in Redshift
RS_CONNECTION_ARG : str
    The name of a Glue Connection configured for Redshift Access
LOCAL_TEST_ARG : str
    Set to True if testing locally

Metrics will be submitted to cloudwatch

"""
import logging
import os
import sys
import boto3

import loader.datasource
import loader.transformer
import loader.sink

from pyspark import SparkContext

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('loader')


def is_localstack():
    """
    Check if we're working with the localstack emulator.

    Returns
    -------
    bool
        True if running with Localstack, False otherwise.
    """
    if 'EC2_ENDPOINT' in os.environ:
        ec2_endpoint = os.environ['EC2_ENDPOINT']

        if 'local' in ec2_endpoint:
            logging.warning('Running under Localstack.')
            return True

    return False


# Get job parameters
args = getResolvedOptions(sys.argv, [
    'BUCKET_ARG',
    'FILTER_ARG',
    'GRANULARITY_ARG',
    'DATASOURCE_ARG',
    'DATAFEED_ARG',
    'RELATIONALIZE_ROOT_ARG',
    'SOURCE_TOPIC_ARG',
    'RS_DATABASE_ARG',
    'RS_CONNECTION_ARG',
    'RS_SCHEMA',
    'TempDir',
    'JOB_NAME',
    'LOCAL_TEST_ARG',
    'REFRESH_STATE',
    'IS_DELETE'
])

logger.info(args)

# connect to Glue
spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark_session = glue_context.spark_session

# get CloudWatch client
cloudwatch = boto3.client('cloudwatch', region_name='eu-west-2')

sink = loader.sink.RedshiftSink(
    args['RS_CONNECTION_ARG'],  # catalog_connection
    args['RS_DATABASE_ARG'],    # database
    args['RS_SCHEMA'],          # schema
    args['TempDir'],            # redshift_tmp_dir
    DynamicFrame,               # dynamic frame
    spark_session,              # spark session
    glue_context                # Glue ctx
)

is_localtesting = bool(args['LOCAL_TEST_ARG'] != "None")

if is_localstack():
    logger.info('is_localstack')
    # configure spark for localstack
    spark_session.conf.set('fs.s3a.endpoint', os.environ['EC2_ENDPOINT'])
    spark_session.conf.set('fs.s3a.path.style.access', 'true')
    # mock redshift using postgres
    sink = loader.sink.PostgresSink(
        'redshift',         # hostname
        '5439',             # port
        'postgres',         # database
        'schema',           # schema
        'postgres',         # username
        'password'          # password
    )
    # configure for localstack cloudwatch
    cloudwatch = boto3.client(
        'cloudwatch',
        region_name='eu-west-2',
        endpoint_url='http://localstack:4566'
    )

# does the data need to be flattened?
relationalize = bool(args['RELATIONALIZE_ROOT_ARG'] != "None")

# find out type of refresh
is_upsert = bool(args['REFRESH_STATE'] == "upsert")

# find out type of refresh
is_delete = bool(args['IS_DELETE'] == "delete")

# find out type of dataset
is_hodh = bool(args['DATASOURCE_ARG'] == "hodh")

# Create datasource
data_feeds = f"{args['DATASOURCE_ARG']}_{args['DATAFEED_ARG']}"
datasource = loader.datasource.Datasource(spark_session, [data_feeds], args)

# Create transformer with datasource impl
transformer = loader.transformer.Transformer(
    datasource,
    glue_context,
    spark_session,
    args
)

if is_hodh:
    transformer.csv_to_parquet()

# invoke transformation sequence
# ----------
# read
if not is_localtesting:
    transformer.read()
   
# relationize (optional)
if relationalize:
    transformer.relationalize()

# transform
if not is_hodh:
    transformer.transform()

# write to Redshift
if relationalize:
    transformer.write_inserts_relationalize(sink)
else:
    transformer.write_inserts(sink)     # write inserts to RS staging

logger.info("applying upserts in redshift")
if is_upsert:
    transformer.rr_apply_upserts(sink)

logger.info("applying deletes in redshift")
if is_delete:
    transformer.rr_apply_deletes(sink)

# log results (metrics)
logger.info(transformer._metrics.data)

# send metrics to CloudWatch
transformer._metrics.submit(cloudwatch, 'NPDL-Loader/'+args['DATASOURCE_ARG'])

logger.info('Process completed.')
