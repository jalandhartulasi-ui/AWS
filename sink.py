"""Model an S3 sink from Kafka Connect."""
import logging
import boto3
from datetime import datetime
from datetime import timedelta
from awsglue.dynamicframe import DynamicFrame, DynamicFrameReader, DynamicFrameWriter, DynamicFrameCollection
from loader.transforms import table_and_key
from pyspark.sql.types import StructType

# s3://dsa-cdl-police-s3-nas-dev/consolidated/landing-1/Year=2021/Month=05/Day=28/Hour=10/Minute=20/
SINK_ROOT = 's3://{}/consolidated/{}'
SINK_PATH = SINK_ROOT + '/year={}/month={}/day={}/hour={}'
SINK_DAY_PATH = SINK_ROOT + '/year={}/month={}/day={}'

class NPDLSink:
    """Model FDP S3 sinks (kafka connect)."""

    def __init__(self, args, types):
        """
        Construct an NPDLSink object.

        Parameters
        ----------
        args : dict
            Provided by the Glue getResolvedOptions call.
        types : list
            List of NPDL types to read from sink
        """
        self.logger = logging.getLogger('loader.NPDLSink')
        self.logger.info('NPDLSink')
        self.logger.info(types)
        self._bucket = args['BUCKET_ARG']
        self._datasource = args['DATASOURCE_ARG']
        self._granularity = args['GRANULARITY_ARG']
        self._source_topic = args['SOURCE_TOPIC_ARG']

        self._types = types

    # def get_base_object_name(self, pole_type, job_id):
    #     """
    #     Return the base of the S3 object name.

    #     Parameters
    #     ----------
    #     pole_type : str
    #         The type of POLE event (e.g. party).
    #     job_id : str
    #         The identifying string of the job.

    #     Returns
    #     -------
    #     str
    #         A string of the form fdp_polev1_<type>_event_<datasource><job_id>.avro.
    #     """
    #     self.logger.info('get_base_object_name')

    #     base_object_name = f'fdp_pole_v1_{pole_type}_event_{self._datasource}{job_id}.avro'
    #     return base_object_name

    def get_paths(self):
        """Evaluate a path based on granularity flag."""
        self.logger.info('get_paths')

        paths = {}
        today = datetime.utcnow()
        yesterday = today - timedelta(days=1)
        # Glue will run on the hour but look in the previous hours partition
        hour = today - timedelta(hours=1)
        # if current hour is midnight, it will look in the hour of the previous day
        if today.hour == 0:
            # set to yesterday
            day = (today - timedelta(days=1)).day
        else:
            day = today.day

        self.logger.info(f'Bucket={self._bucket}')
        self.logger.info(f'Datasource={self._datasource}')
        self.logger.info(f'Types={self._types}')
        for event in self._types:
            self.logger.info(f'Event={event}')
            if self._granularity == 'yesterday':
                yesterday_path = SINK_DAY_PATH.format(self._bucket, self._source_topic, str(yesterday.year).zfill(2),
                                                str(yesterday.month).zfill(2), str(yesterday.day).zfill(2))
                paths[event] = f"{yesterday_path}/hour=*/minute=*/*.parquet"
            elif self._granularity == 'today':
                today_path = SINK_DAY_PATH.format(self._bucket, self._source_topic, str(today.year).zfill(2),
                                                str(today.month).zfill(2), str(today.day).zfill(2))
                paths[event] = f"{today_path}/hour=*/minute=*/*.parquet"
            elif self._granularity == 'hour':
                paths[event] = SINK_PATH.format(self._bucket, self._source_topic, str(today.year).zfill(2),
                                                str(today.month).zfill(2), str(day).zfill(2), str(hour.hour).zfill(2))
            elif self._granularity == 'all':
                paths[event] = SINK_ROOT.format(self._bucket, self._source_topic)
                self.logger.info(f'paths[event]={paths[event]}')

        self.logger.info(f'paths=[{paths}]')

        return paths


class PostgresSink:
    """PG sink - for test pipeline."""

    def __init__(self, hostname, port, database, schema, username, password):
        """
        Create PostgresSink.

        Parameters
        ----------
        hostname : str
            The postgres hostname
        port : str
            The postgres port
        database : str
            The postgres database
        schema : str
            The postgres schema to write to
        username : str
            The postgres username
        password : str
            The postgres password
        """
        self.logger = logging.getLogger('loader.PostgresSink')
        self.logger.info('PostgresSink')
        self.logger.info(f'REDSHIFT USERNAME ******** [{username}')
        self._jdbc_url = f'jdbc:postgresql://{hostname}:{port}/{database}'
        self._properties = {
            'user': username,
            'password': password
        }
        self._schema = schema

    def write(self, data_frame, table_name):
        """
        Write dataframe to table.

        Parameters
        ----------
        data_frame : DataFrame
            The dataframe to write
        table_name : str
            The database table to write to
        """
        self.logger.info(f'write [{table_name}')

        data_frame.write.mode('append').jdbc(
            self._jdbc_url,
            f'{self._schema}.{table_name}',
            properties=self._properties
        )


class RedshiftSink:
    """Redshift Sink - using AWS Glue connection."""

    def __init__(self, catalog_connection, database, schema, tmp_dir, DynamicFrame, spark, glue_context):
        """Create RedshiftSink."""
        self.logger = logging.getLogger('loader.RedshiftSink')
        self.logger.info('RedshiftSink')
        self.logger.info(f'REDSHIFT CATALOG ******** [{catalog_connection}')
        self._catalog_connection = catalog_connection
        self._database = database
        self._schema = schema
        self._tmp_dir = tmp_dir
        self._spark = spark
        self._DynamicFrame = DynamicFrame
        self._glue_context = glue_context
        
        
    def read(self, table_name):
        
        redshiftTmpDir = self._tmp_dir
                       
        self.logger.info('create a redshift_conn dynamic frames from Redshift table.')

        redshift_conn = self._glue_context.create_dynamic_frame.from_options(
                     connection_type="redshift",
                     connection_options={
                         "useConnectionProperties": "true",
                         "dbtable": f'{self._schema}.{table_name}',
                         "connectionName": self._catalog_connection,
                         "redshiftTmpDir": redshiftTmpDir
                     },
                     transformation_ctx="redshift_conn",
                    )

        #redshift_conn.toDF().show()            
        df = redshift_conn.toDF()

        self.logger.info('completed redshift_conn for dynamic frames from Redshift table.')
        return df

    def rr_write_deletes(self, data_frame,table_name, str_list, table_key):
        """
        Write dataframe to table.

        Parameters
        ----------
        data_frame : DataFrame
            The dataframe to write
        table_name : str
            The database table to write to
        """
        self.logger.info(f'Regular refresh rr_write_deletes [{table_name}]')
        
        pre_query = f"""delete from {self._schema}.{table_name}  
                               where {table_key} in ({str_list});"""
        
        self.logger.info(f"{pre_query}")

        post_query = f"""drop table if exists {self._schema}.stage_pnc_nphord_nominal;"""

        self.logger.info(f"{post_query}")

        # Define an empty schema (or use a minimal one if needed)
        empty_schema = StructType([])
        
        # Create an empty DataFrame
        empty_df = self._spark.createDataFrame([], schema=empty_schema)
        
        # Convert to DynamicFrame
        empty_dyf = self._DynamicFrame.fromDF(empty_df, self._glue_context, "empty")
        
        #dynamic_df = self._DynamicFrame.fromDF(df_empty, self._glue_context, "empty_dynamic_frame")
        dynamic_df = self._DynamicFrame.fromDF(data_frame, self._glue_context, table_name)        
        self.logger.info(f'catalog_connection={self._catalog_connection}')
        self.logger.info(f'database={self._database}')
        self.logger.info(f'schema.table={self._schema}.{table_name}')
        self.logger.info(f'tmp_dir={self._tmp_dir}')
        self.logger.info('About to write dynamic frames into Redshift.')
        delete_to_redshift = self._glue_context.write_dynamic_frame.from_jdbc_conf(
                       frame=dynamic_df,
                       catalog_connection=self._catalog_connection,
                       connection_options={
                           'database': self._database,
                           'dbtable': f'{self._schema}.stage_pnc_nphord_nominal',
                           "preactions": pre_query,
                           "postactions": post_query
                       },
                       redshift_tmp_dir=self._tmp_dir,
                       transformation_ctx="delete_to_redshift"
        )
        self.logger.info('Regular refresh rr_write_deletes done.')

    def rr_write_upserts(self, data_frame, table_name, str_list, table_key):
        """
        Write dataframe to table.

        Parameters
        ----------
        data_frame : DataFrame
            The dataframe to write
        table_name : str
            The database table to write to
        """
        self.logger.info(f'Regular refresh Write [{table_name}]')
        
        pre_query = f"""delete from {self._schema}.{table_name}  
                               where {table_key} in ({str_list});"""
        
        self.logger.info(f"{pre_query}")

        post_query = f"""begin;
                        insert into {self._schema}.{table_name} select distinct * from {self._schema}.stage_{table_name};
                        drop table {self._schema}.stage_{table_name}; end;"""
        
        #post_query = f""" """

        self.logger.info(f"{post_query}")
        
        dynamic_df = self._DynamicFrame.fromDF(data_frame, self._glue_context, table_name)        
        self.logger.info(f'catalog_connection={self._catalog_connection}')
        self.logger.info(f'database={self._database}')
        self.logger.info(f'schema.table={self._schema}.{table_name}')
        self.logger.info(f'tmp_dir={self._tmp_dir}')
        self.logger.info('About to write dynamic frames into Redshift.')
        upsert_to_redshift = self._glue_context.write_dynamic_frame.from_jdbc_conf(
                       frame=dynamic_df,
                       catalog_connection=self._catalog_connection,
                       connection_options={
                           'database': self._database,
                           'dbtable': f'{self._schema}.stage_{table_name}',
                           "preactions": pre_query,
                           "postactions": post_query
                       },
                       redshift_tmp_dir=self._tmp_dir,
                       transformation_ctx="upsert_to_redshift"
        )
        self.logger.info('Regular refresh DFs written.')

    def rr_write_to_stage(self, data_frame, table_name):
        """
        Write dataframe to table.

        Parameters
        ----------
        data_frame : DataFrame
            The dataframe to write
        table_name : str
            The database table to write to
        """
        self.logger.info(f'Stage table write [stage_{table_name}]')
        
        pre_query = f"""drop table if exists {self._schema}.stage_{table_name};
                       create table {self._schema}.stage_{table_name} as select * from {self._schema}.{table_name} where 1=2;"""
        
        self.logger.info(f"{pre_query}")

        post_query = f"""begin; grant all on table {self._schema}.stage_{table_name} to veerepj;
                        end;"""
        
        self.logger.info(f"{post_query}")
        
        dynamic_df = self._DynamicFrame.fromDF(data_frame, self._glue_context, f'stage_{table_name}')
        data_frame.show(data_frame.count(),truncate=False)
                
        self.logger.info(f'catalog_connection={self._catalog_connection}')
        self.logger.info(f'database={self._database}')
        self.logger.info(f'schema.table={self._schema}.stage_{table_name}')
        self.logger.info(f'tmp_dir={self._tmp_dir}')
        self.logger.info('About to write dynamic frames into Redshift for stage table.')
        stage_to_redshift = self._glue_context.write_dynamic_frame.from_jdbc_conf(
                       frame=dynamic_df,
                       catalog_connection=self._catalog_connection,
                       connection_options={
                           'database': self._database,
                           'dbtable': f'{self._schema}.stage_{table_name}',
                           "preactions": pre_query,
                           "postactions": post_query
                       },
                       redshift_tmp_dir=self._tmp_dir,
                       transformation_ctx="stage_to_redshift"
        )
        self.logger.info('stage table DFs written.')

    def rr_write(self, data_frame, table_name ,table_key):
        """
        Write dataframe to table.

        Parameters
        ----------
        data_frame : DataFrame
            The dataframe to write
        table_name : str
            The database table to write to
        """
        self.logger.info(f'Regular refresh Write [{table_name}]')
        
        pre_query = f"""drop table if exists {self._schema}.stage_{table_name};
                       create table {self._schema}.stage_{table_name} as select * from {self._schema}.{table_name} where 1=2;"""
        
        self.logger.info(f"{pre_query}")

        post_query = f"""begin;delete from {self._schema}.{table_name} using {self._schema}.stage_{table_name} 
                               where {self._schema}.stage_{table_name}.{table_key} = {self._schema}.{table_name}.{table_key};
                        insert into {self._schema}.{table_name} select * from {self._schema}.stage_{table_name};
                        drop table {self._schema}.stage_{table_name}; end;"""
        
        self.logger.info(f"{post_query}")
        
        if table_name == "pnc_nphord_nominal":
            df_rr = data_frame.drop(data_frame.input_filename)
            dynamic_df = self._DynamicFrame.fromDF(df_rr, self._glue_context, table_name)
            df_rr.show(truncate=False)
        else:
            dynamic_df = self._DynamicFrame.fromDF(data_frame, self._glue_context, table_name)
            data_frame.show(truncate=False)
                
        self.logger.info(f'catalog_connection={self._catalog_connection}')
        self.logger.info(f'database={self._database}')
        self.logger.info(f'schema.table={self._schema}.{table_name}')
        self.logger.info(f'tmp_dir={self._tmp_dir}')
        self.logger.info('About to write dynamic frames into Redshift.')
        upsert_to_redshift = self._glue_context.write_dynamic_frame.from_jdbc_conf(
                       frame=dynamic_df,
                       catalog_connection=self._catalog_connection,
                       connection_options={
                           'database': self._database,
                           'dbtable': f'{self._schema}.stage_{table_name}',
                           "preactions": pre_query,
                           "postactions": post_query
                       },
                       redshift_tmp_dir=self._tmp_dir,
                       transformation_ctx="upsert_to_redshift"
        )
        self.logger.info('Regular refresh DFs written.')

    def write(self, data_frame, table_name):
        """
        Write dataframe to table.

        Parameters
        ----------
        data_frame : DataFrame
            The dataframe to write
        table_name : str
            The database table to write to
        """
        self.logger.info(f'Write [{table_name}]')

        dynamic_df = self._DynamicFrame.fromDF(data_frame, self._glue_context, table_name)
        self.logger.info(f'catalog_connection={self._catalog_connection}')
        self.logger.info(f'database={self._database}')
        self.logger.info(f'schema.table={self._schema}.{table_name}')
        self.logger.info(f'tmp_dir={self._tmp_dir}')
        self.logger.info('About to write dynamic frames into Redshift.')
        self._glue_context.write_dynamic_frame.from_jdbc_conf(
            frame=dynamic_df,
            catalog_connection=self._catalog_connection,
            connection_options={
                'database': self._database,
                'dbtable': f'{self._schema}.{table_name}'
            },
            redshift_tmp_dir=self._tmp_dir,
            transformation_ctx=table_name
        )
                
        self.logger.info('DFs written.')
    
    def drop_table(self,table):
        """
        Get Redshift connection details (e.g., username, password) from Glue connections
        """
        response = glue_client.get_connection(Name=self._catalog_connection)
    
        # Extract connection properties
        conn_props = response["Connection"]["ConnectionProperties"]
        host = conn_props['JDBC_CONNECTION_URL'].split("//")[1].split(":")[0]
        port = int(conn_props['JDBC_CONNECTION_URL'].split(":")[-1].split("/")[0])
        database = conn_props['JDBC_CONNECTION_URL'].split("/")[-1]
        username = conn_props.get("USERNAME")
        password = conn_props.get("PASSWORD")
        redshift_jdbc_url = conn_props.get("JDBC_CONNECTION_URL")

        # Step 2: Connect to Redshift using redshift_connector (secure and compatible) 
        conn = red-shift_connector.connect(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password
            )
        
        # Step 3: Execute DROP TABLE
        cursor = conn.cursor()
        drop_stmt = f"DROP TABLE IF EXISTS {self._schema}.{table};"
        cursor.execute(drop_stmt)
        conn.commit()

        self.logger.info(f"Table {self._schema}.{table} dropped successfully.")

        cursor.close()
        conn.close()
