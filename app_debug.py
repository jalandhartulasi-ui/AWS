#!/usr/bin/env python
"""DSA NPDL Loader - debug.

AWS Glue script to debug NPDL Loader

This job will:
 - Connect to Glue
 - Configure Redshift sink
 - Write NDPL data to Redshift for analysis


"""
import sys
import logging

# from awsglue.context import GlueContext
# from awsglue.utils import getResolvedOptions
# from pyspark import SparkContext
# from awsglue.dynamicframe import DynamicFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('debug')

logger.info('start')

# Get job parameters
logger.info(sys.argv)

# # connect to Glue
# spark_context = SparkContext()
# glue_context = GlueContext(spark_context)
# spark_session = glue_context.spark_session

# sink = loader.sink.RedshiftSink(
#     args[loader.parameters.RS_CONNECTION_ARG],  # catalog_connection
#     args[loader.parameters.RS_DATABASE_ARG],    # database
#     '',                            # schema
#     args['TempDir'],                            # redshift_tmp_dir
#     DynamicFrame,                               # dynamic frame
#     glue_context                                # Glue ctx
# )

# df = spark_session.read.format('avro').load('INPUT-PATH-HERE')
# sink.write(df, 'debug')

logger.info('done')
