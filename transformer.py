"""Transformer."""
import logging
import loader.sink as sink
import boto3
from pyspark.sql.functions import lit,col
import pyspark.sql.functions as f
from pyspark.sql.window import Window

from datetime import datetime
from datetime import timedelta
from tempfile import NamedTemporaryFile
from functools import reduce
from pyspark.sql.functions import col,regexp_replace,lit,current_timestamp,input_file_name,count,count_distinct
from pyspark.sql.types import IntegerType,BooleanType,DateType,StructType,StructField,StringType
from loader.transforms import \
    hodh_class_of_disposal, hodh_class_of_event, hodh_class_of_location, hodh_disposal, \
    hodh_event_inv_aggravating_factor, hodh_event_inv_location, hodh_location, \
    hodh_material_item, hodh_offence_detail, hodh_organisation, hodh_person_relation, \
    hodh_person, hodh_role_played ,table_and_key

class TransformerMetrics:
    """
    Capture metrics for submission to AWS CloudWatch.

    Methods
    -------
    put_count_metric(put_count_metric(self, name, value, dimensions=[])
        Add a metric

    """

    def __init__(self):
        """Create a TransformerMetrics object."""
        self.logger = logging.getLogger('loader.TransformerMetrics')
        self.logger.info('TransformerMetrics')
        self.data = []

    def put_count_metric(self, name, value, dimensions=[]):
        """
        Append to the count metrics.

        Parameters
        ----------
        name : str
            The name of the metric
        value : numeric
            The value of the metric
        dimensions : list of dict
            Dimensions in format {'Name': 'entity', 'Value': 'address'}

        """
        self.logger.info(f'put_count_metric [{name}] [{value} [{dimensions}]]')
        self.data.append({
            'MetricName': name,
            'Dimensions': dimensions,
            'Unit': 'Count',
            'Value': value
        })

    def _submit(self, data, cloudwatch, namespace):
        """
        Submit to CloudWatch.

        Parameters
        ----------
        cloudwatch :
            The boto3 `cloudwatch` client
        namespace : str
            The cloudwatch namespace to user
        """
        self.logger.info('_submit')
        cloudwatch.put_metric_data(
            MetricData=data,
            Namespace=namespace
        )

    def submit(self, cloudwatch, namespace):
        """
        Submit to CloudWatch.

        Parameters
        ----------
        cloudwatch :
            The boto3 `cloudwatch` client
        namespace : str
            The cloudwatch namespace to user
        """
        self.logger.info(f'submit [{namespace}]')
        # AWS limit of 20 metrics per put call - split into chunks
        chunks = 18
        for i in range(0, len(self.data), chunks):
            self._submit(self.data[i:i + chunks], cloudwatch, namespace)


class Transformer:
    """
    Transform data from CDLZ NPDL to Redshift NPDL.

    Methods
    -------
    stop()
        Stop the transformer

    read()
        Read source data

    transform()
        Hook to transform data

    relationalize()
        Hook to flatten nested formats

    write_inserts()
         Hook to write_inserts

    # filter()
    #     Hook to filter data

    # generate_deletes()
    #     Hook to generate_deletes

    # write_deletes()
    #     Hook to write_deletes

    # only_latest()
    #     Hook to exclude old data within a batch

    """

    def __init__(self, datasource, glue_context, spark, args):
        """
        Construct Transformer.

        Parameters
        ----------
        datasource : Datasource
            datasource implementation transformer will use

        glue_context : AWS Glue context

        spark : SparkSession
            AWS Glue session

        args : dict
            Arguments supplied to the AWS Glue job

        """
        self.logger = logging.getLogger('loader.Transformer')
        self.logger.info(f'Transformer [{args}]')
        self._datasource = datasource
        self._spark = spark
        self._glue_context = glue_context
        self._bucket = args['BUCKET_ARG']
        self._datasource_arg = args['DATASOURCE_ARG']
        self._datafeed_arg = args['DATAFEED_ARG']
        self._source_topic = args['SOURCE_TOPIC_ARG']
        self._job_name = args['JOB_NAME']
        self._schema = args['RS_SCHEMA']
        self._tmp_dir = args['TempDir']
        self._refresh_state = args['REFRESH_STATE']
        self._delete = args['IS_DELETE']
        self._granularity = args['GRANULARITY_ARG']

        assert args['BUCKET_ARG'], 'missing BUCKET_ARG argument'
        assert args['DATASOURCE_ARG'], 'missing DATASOURCE_ARG argument'
        assert args['DATAFEED_ARG'], 'missing DATAFEED_ARG argument'
        assert args['FILTER_ARG'], 'missing FILTER_ARG argument'
        assert args['GRANULARITY_ARG'], 'missing GRANULARITY_ARG argument'
        assert args['RELATIONALIZE_ROOT_ARG'], 'missing RELATIONIZE_ROOT_ARG argument'
        self._args = args

        # init metrics
        self._metrics = TransformerMetrics()

        # create NPDL sink from args
        self.logger.info(f'Datasource types={datasource.types}')
        self._sink = sink.NPDLSink(self._args, datasource.types)

        # dict to hold all spark dataframe references of delta data - delta frames
        # passed between transformer and datasource as a working context
        self._delta_frames = {
            # default location for read data frames
            'read': {},
            # default location for delete data frames
            'delete': {},
            # default location for relationalized data frames
            'relationalize': {}
        }

    def stop(self):
        """Stop spark."""
        self.logger.info('Stop')
        if self._spark is not None:
            self._spark.stop()
            
    def csv_to_parquet(self):
        self.logger.info('Starting CSV to Parquet job.')
    
        # Construct input/output paths
        input_loc = f"s3://{self._args['BUCKET_ARG']}/{self._args['DATAFEED_ARG']}.csv"
        output_loc = f"s3://{self._args['BUCKET_ARG']}/consolidated/{self._args['SOURCE_TOPIC_ARG']}/"
        glue_job_run_id = self._args['JOB_RUN_ID']
        datafeed = self._datafeed_arg
    
        self.logger.info(f"Reading CSV from: {input_loc}")
        
        # Read CSV
        try:
            csv_df = self._spark.read.option('delimiter', ',').option('header', 'true').csv(input_loc)
        except Exception as e:
            self.logger.error(f"Failed to read CSV: {e}")
            raise
    
        # Mapping of datafeeds to transform functions
        transformation_map = {
            "class_of_disposal": hodh_class_of_disposal.class_of_disposal,
            "class_of_event": hodh_class_of_event.class_of_event,
            "class_of_location": hodh_class_of_location.class_of_location,
            "disposal": hodh_disposal.disposal,
            "event_inv_aggravating_factor": hodh_event_inv_aggravating_factor.event_inv_aggravating_factor,
            "event_inv_location": hodh_event_inv_location.event_inv_location,
            "location": hodh_location.location,
            "material_item": hodh_material_item.material_item,
            "offence_detail": hodh_offence_detail.offence_detail,
            "organisation": hodh_organisation.organisation,
            "person": hodh_person.person,
            "person_relation": hodh_person_relation.person_relation,
            "role_played": hodh_role_played.role_played
            # Add others if necessary
        }
    
        # Decide transformation
        try:
            if datafeed in transformation_map:
                df_final = transformation_map[datafeed](csv_df, glue_job_run_id)
                self.logger.info(f"Applied transformation for: {datafeed}")
            else:
                # Identity pass-through for lookup tables
                self.logger.info(f"No transformation function found. Using raw CSV for: {datafeed}")
                df_final = csv_df
    
            # Optional: validate non-empty DataFrame
            if df_final.rdd.isEmpty():
                raise ValueError(f"DataFrame is empty for {datafeed}")
    
            df_final.show(truncate=False)
    
            # Write to Parquet
            self.logger.info(f"Writing Parquet to: {output_loc}")
            df_final.write.option("header", "true").mode("overwrite").parquet(output_loc)
            self.logger.info("Write complete.")
    
        except Exception as e:
            self.logger.error(f"Error processing {datafeed}: {str(e)}")
            raise

    
    def nominal_fks(self,table_name,sink):
        
        self.logger.info('nominal foreign keys')
        df_target = sink.read(table_name)
        df_target.createOrReplaceTempView('nominal')
        df_stage = sink.read("stage_"+table_name)

        df_nominal_fks = df_target.join(df_stage, df_stage.pncid == df_target.pncid) \
                                  .select(df_target.pncid,df_target.occupation_fk,df_target.informationmarker_fk,
                                          df_target.caseheader_fk,df_target.operationalinformation_fk)
        #df_nominal_fks.show(truncate=False)
        return df_nominal_fks
    
    def caseheader_fks(self,table_name,df_nominal_fks,sink):
        
        self.logger.info('caseheader foreign keys')
        df_caseheader = sink.read(table_name)
        df_caseheader.createOrReplaceTempView('caseheader')
        
        df_caseheader_nominals = df_nominal_fks.select('caseheader_fk').dropDuplicates()
        df_caseheader_nominals.createOrReplaceTempView('caseheader_fks')
        df_caseheader_fks = self._spark.sql("""select offence_fk,condition_fk,method_fk,custody_fk,bailconditions_fk,breachofbail_fk from caseheader
                                            where nominal_id in (select caseheader_fk from caseheader_fks)""")
        return df_caseheader_fks
    
    def offence_fks(self,table_name,df_caseheader_fks,sink):
        
        self.logger.info('offence foreign keys')
        df_offence = sink.read(table_name)
        df_offence.createOrReplaceTempView('offence')

        df_offence_nominals = df_caseheader_fks.select('offence_fk').dropDuplicates()
        df_offence_nominals.createOrReplaceTempView('offence_fks')

        df_offence_fks = self._spark.sql("""select distinct disposal_fk,cooffender_fk,subsequentappearance_fk from offence
                                         where caseheader_id in (select offence_fk from offence_fks)""")
        return df_offence_fks
    
    def method_fks(self,table_name,df_caseheader_fks,sink):
        
        self.logger.info('method foreign keys')
        df_method = sink.read(table_name)
        df_method.createOrReplaceTempView('method')

        df_method_nominals = df_caseheader_fks.select('method_fk').dropDuplicates()
        df_method_nominals.createOrReplaceTempView('method_fks')

        df_method_fks = self._spark.sql("""select distinct mokeywordlocation_fk,mokeywordvictim_fk from method
                                        where caseheader_id in (select method_fk from method_fks)""")
        return df_method_fks
    
    def custody_fks(self,table_name,df_caseheader_fks,sink):
        
        self.logger.info('custody foreign keys')
        df_custody = sink.read(table_name)
        df_custody.createOrReplaceTempView('custody')

        df_custody_nominals = df_caseheader_fks.select('custody_fk').dropDuplicates()
        df_custody_nominals.createOrReplaceTempView('custody_fks')
        
        df_custody_fks = self._spark.sql("""select distinct periodininstitution_fk from custody
                                         where caseheader_id in (select custody_fk from custody_fks)""")
        return df_custody_fks
    
    def periodininstitution_fks(self,table_name,df_custody_fks,sink):
        
        self.logger.info('periodininstitution foreign keys')
        df_periodininstitution = sink.read(table_name)
        df_periodininstitution.createOrReplaceTempView('periodininstitution')

        df_periodininstitution_nominals = df_custody_fks.select('periodininstitution_fk').dropDuplicates()
        df_periodininstitution_nominals.createOrReplaceTempView('periodininstitution_fks')
        
        df_periodininstitution_fks = self._spark.sql("""select distinct releasedetails_fk from periodininstitution
                                                     where custody_id in (select periodininstitution_fk from periodininstitution_fks)""")
        return df_periodininstitution_fks

    def operationalinformation_fks(self,table_name,df_nominal_fks,sink):

        df_operationalinformation = sink.read(table_name)
        df_operationalinformation.createOrReplaceTempView('operationalinformation')

        df_operationalinformation_nominals = df_nominal_fks.select('operationalinformation_fk').dropDuplicates()
        df_operationalinformation_nominals.createOrReplaceTempView('operationalinformation_fks')

        if df_operationalinformation.isEmpty():
            #raise FileExistsError("Empty File")
            self.logger.info('Empty File Read')
        else:            
            df_operationalinformation_fks = self._spark.sql("""select distinct oiconditions_fk from operationalinformation
                                                  where nominal_id in (select operationalinformation_fk from operationalinformation_fks)""")

    def nominal_fk_upsert(self,table_name,df_fks,sink):
        
        if df_fks.isEmpty():
            self.logger.info('Dataframe is empty')
        else:
            key_list = '\''+'\',\''.join(map(str, list(df_fks.select(table_name+'_fk').dropDuplicates().toPandas()[table_name+'_fk'])))+'\''
            table_key = table_and_key.get_table_key("pnc_nphord_"+table_name)
            df_stage = sink.read("stage_pnc_nphord_"+table_name)
            if len(key_list) != 0:
                sink.rr_write_upserts(df_stage,"pnc_nphord_"+table_name, key_list,table_key)
    
    
    def rr_apply_upserts(self, sink):
        def count_updates(df, fk_col, table_name):
            df_with_table = df.withColumn("table", lit(table_name))
            df_with_table.createOrReplaceTempView(table_name)            
            return self._spark.sql(f"SELECT table, COUNT({fk_col}) AS update_count FROM {table_name} GROUP BY table")
    
        def batch_upsert(upserts):
            for table, df in upserts:
                self.logger.info(f'write upserts for {table}')
                self.nominal_fk_upsert(table, df, sink)
    
        # Extract FK DataFrames
        df_nominal_fks = self.nominal_fks("pnc_nphord_nominal", sink)
        df_caseheader_fks = self.caseheader_fks("pnc_nphord_caseheader", df_nominal_fks, sink)
        df_operationalinformation_fks = self.operationalinformation_fks("pnc_nphord_operationalinformation", df_nominal_fks, sink)
        df_offence_fks = self.offence_fks("pnc_nphord_offence", df_caseheader_fks, sink)
        df_method_fks = self.method_fks("pnc_nphord_method", df_caseheader_fks, sink)
        df_custody_fks = self.custody_fks("pnc_nphord_custody", df_caseheader_fks, sink)
        df_periodininstitution_fks = self.periodininstitution_fks("pnc_nphord_periodininstitution", df_custody_fks, sink)
    
        # Count summary setup
        counts = [
            count_updates(df_nominal_fks, "pncid", "nominal"),
            count_updates(df_nominal_fks, "occupation_fk", "occupation"),
            count_updates(df_nominal_fks, "informationmarker_fk", "informationmarker"),
            count_updates(df_nominal_fks, "caseheader_fk", "caseheader"),
            count_updates(df_nominal_fks, "operationalinformation_fk", "operationalinformation"),
    
            count_updates(df_caseheader_fks, "offence_fk", "offence"),
            count_updates(df_caseheader_fks, "condition_fk", "condition"),
            count_updates(df_caseheader_fks, "method_fk", "method"),
            count_updates(df_caseheader_fks, "custody_fk", "custody"),
            count_updates(df_caseheader_fks, "bailconditions_fk", "bailconditions"),
            count_updates(df_caseheader_fks, "breachofbail_fk", "breachofbail"),

            #count_updates(df_operationalinformation_fks, "oiconditions_fk", "oiconditions"),
    
            count_updates(df_offence_fks, "disposal_fk", "disposal"),
            count_updates(df_offence_fks, "cooffender_fk", "cooffender"),
            count_updates(df_offence_fks, "subsequentappearance_fk", "subsequentappearance"),
    
            count_updates(df_method_fks, "mokeywordlocation_fk", "mokeywordlocation"),
            count_updates(df_method_fks, "mokeywordvictim_fk", "mokeywordvictim"),
    
            count_updates(df_custody_fks, "periodininstitution_fk", "periodininstitution"),
            count_updates(df_periodininstitution_fks, "releasedetails_fk", "releasedetails"),
        ]
    
        # Union all counts and write CSV summary
        union_df = counts[0]
        for df in counts[1:]:
            union_df = union_df.union(df)
    
        union_df.show()
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M")
        filename = f"pnc_updates_{timestamp}.csv"
        final_path = f"{self._tmp_dir}/report_summary/updates/{filename}"
        union_df.coalesce(1).write.mode("overwrite").option("header", True).csv(final_path)

        #union_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{self._tmp_dir}/report_summary/updates/pnc_updates.csv")
    
        # Perform Upserts
        upserts = [
            ("releasedetails", df_periodininstitution_fks),
            ("periodininstitution", df_custody_fks),
            ("custody", df_caseheader_fks),
            ("mokeywordlocation", df_method_fks),
            ("mokeywordvictim", df_method_fks),
            ("method", df_caseheader_fks),
            ("disposal", df_offence_fks),
            ("cooffender", df_offence_fks),
            ("subsequentappearance", df_offence_fks),
            ("offence", df_caseheader_fks),
            ("bailconditions", df_caseheader_fks),
            ("breachofbail", df_caseheader_fks),
            ("condition", df_caseheader_fks),
            #("oiconditions", df_operationalinformation_fks),
            ("occupation", df_nominal_fks),
            ("informationmarker", df_nominal_fks),
            ("caseheader", df_nominal_fks),
            ("operationalinformation", df_nominal_fks),
        ]
    
        batch_upsert(upserts)
    
        # Final upsert into sink
        pncid_list = "','".join(map(str, df_nominal_fks.select("pncid").dropDuplicates().toPandas()["pncid"]))
        pncid_list = f"'{pncid_list}'"
        table_key = table_and_key.get_table_key("pnc_nphord_nominal")
        df_stage = sink.read("stage_pnc_nphord_nominal")
        sink.rr_write_upserts(df_stage, "pnc_nphord_nominal", pncid_list, table_key)

    
    def nominal_delete_fks(self,table_name,df_pncids,sink):
        
        self.logger.info('nominal foreign keys for deletes')
        df_target = sink.read(table_name)

        unmatched_df_target = df_pncids.join(df_target, on="pncid", how="left_anti")
        self.logger.info('Unmatched pncids from Nominal table')
        unmatched_df_target.show()

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M")
        filename = f"unmatched_pnc_deletes_{timestamp}.csv"
        final_path = f"{self._tmp_dir}/report_summary/deletes/{filename}"
        unmatched_df_target.coalesce(1).write.mode("overwrite").option("header", True).csv(final_path)

        df_nominal_delete_fks = df_target.join(df_pncids, df_pncids.pncid == df_target.pncid) \
                                  .select(df_target.pncid,df_target.occupation_fk,df_target.informationmarker_fk,
                                          df_target.caseheader_fk,df_target.operationalinformation_fk)
        return df_nominal_delete_fks
    
    def nominal_fk_delete(self,table_name,df_fks,sink,df_flatten):
        
        if df_fks.isEmpty():
            self.logger.info('Dataframe is empty')
        else:
            key_list = '\''+'\',\''.join(map(str, list(df_fks.select(table_name+'_fk').dropDuplicates().toPandas()[table_name+'_fk'])))+'\''
            table_key = table_and_key.get_table_key("pnc_nphord_"+table_name)
            #df_stage = sink.read("stage_pnc_nphord_"+table_name)
            #df = self._spark.sql(f"""delete from {self._schema}.{table_name}  
            #                   where {table_key} in ({str_list});""")
            if len(key_list) != 0:
                self.logger.info('write deletes function initiated from nominal_fk_delete')
                sink.rr_write_deletes(df_flatten,"pnc_nphord_"+table_name, key_list, table_key)

    def rr_apply_deletes(self, sink):

        def log_and_extract_fk(step_name, table_name, base_df, extract_func):
            self.logger.info(f"[FK Extract] {step_name}")
            return extract_func(table_name, base_df, sink)
    
        def create_count_df(df, table_name, fk_column):
            if fk_column in df.columns:
                return df.withColumn("table", lit(table_name)) \
                         .groupBy("table") \
                         .count() \
                         .withColumnRenamed("count", "delete_count")
            else:
                return None
        
        # Construct input/output paths
        ROOT = 's3://{}/consolidated/{}'
        DAY_PATH = ROOT + '/year={}/month={}/day={}'

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

        #s3://dsa-cdl-police-s3-pnc-notprod/consolidated/landing-63/year=YYYY/month=MM/day=DD/hour=HH/minute=M/

        if self._granularity == 'yesterday':
            yesterday_path = DAY_PATH.format(self._bucket, self._source_topic, str(yesterday.year).zfill(2),
                                                  str(yesterday.month).zfill(2), str(yesterday.day).zfill(2))
            input_path = f"{yesterday_path}/hour=*/minute=*/*.parquet"
        elif self._granularity == 'today':
            today_path = DAY_PATH.format(self._bucket, self._source_topic, str(today.year).zfill(2),
                                               str(today.month).zfill(2), str(today.day).zfill(2))
            input_path = f"{today_path}/hour=*/minute=*/*.parquet"

        glue_job_run_id = self._args['JOB_RUN_ID']
        datafeed = self._datafeed_arg
    
        self.logger.info(f"Reading Delete file from: {input_path}")
        
        # Read Deletes
        #        try:
        #    delete_df = self._spark.read.option('delimiter', ',').option('header', 'true').csv(input_loc)
        # Exception as e:
        #    self.logger.error(f"Failed to read CSV: {e}")
        #    raise

        self.logger.info('Attempting to read delete file from bucket.')
        df_pncids = self._spark.read.format('parquet').load(input_path)
        self.logger.info("Data deletion done")

        df_pncids.printSchema()
        df_flatten = df_pncids.select(col("body.PNCID").alias("pncid"))
        df_flatten.show()
    
        df_nominal_fks = log_and_extract_fk("Nominal Delete FKs", "pnc_nphord_nominal", df_flatten, self.nominal_delete_fks)
        df_operationalinformation_fks = log_and_extract_fk("OperationalInformation FKs", "pnc_nphord_operationalinformation", df_nominal_fks, self.operationalinformation_fks)
        df_caseheader_fks = log_and_extract_fk("CaseHeader FKs", "pnc_nphord_caseheader", df_nominal_fks, self.caseheader_fks)
        df_offence_fks = log_and_extract_fk("Offence FKs", "pnc_nphord_offence", df_caseheader_fks, self.offence_fks)
        df_method_fks = log_and_extract_fk("Method FKs", "pnc_nphord_method", df_caseheader_fks, self.method_fks)
        df_custody_fks = log_and_extract_fk("Custody FKs", "pnc_nphord_custody", df_caseheader_fks, self.custody_fks)
        df_period_fks = log_and_extract_fk("PeriodInInstitution FKs", "pnc_nphord_periodininstitution", df_custody_fks, self.periodininstitution_fks)
    
        # Collect all delete counts
        counts = [
            create_count_df(df_nominal_fks, "nominal", "pncid"),
            create_count_df(df_nominal_fks, "occupation", "occupation_fk"),
            create_count_df(df_nominal_fks, "informationmarker", "informationmarker_fk"),
            create_count_df(df_nominal_fks, "caseheader", "caseheader_fk"),
            create_count_df(df_nominal_fks, "operationalinformation", "operationalinformation_fk"),
            create_count_df(df_period_fks, "oiconditions", "oiconditions_fk"),
            create_count_df(df_caseheader_fks, "offence", "offence_fk"),
            create_count_df(df_caseheader_fks, "condition", "condition_fk"),
            create_count_df(df_caseheader_fks, "method", "method_fk"),
            create_count_df(df_caseheader_fks, "custody", "custody_fk"),
            create_count_df(df_caseheader_fks, "bailconditions", "bailconditions_fk"),
            create_count_df(df_caseheader_fks, "breachofbail", "breachofbail_fk"),
            create_count_df(df_offence_fks, "disposal", "disposal_fk"),
            create_count_df(df_offence_fks, "cooffender", "cooffender_fk"),
            create_count_df(df_offence_fks, "subsequentappearance", "subsequentappearance_fk"),
            create_count_df(df_method_fks, "mokeywordlocation", "mokeywordlocation_fk"),
            create_count_df(df_method_fks, "mokeywordvictim", "mokeywordvictim_fk"),
            create_count_df(df_custody_fks, "periodininstitution", "periodininstitution_fk"),
            create_count_df(df_period_fks, "releasedetails", "releasedetails_fk"),
        ]
    
        # Filter out None entries (missing columns)
        counts = [df for df in counts if df is not None]
    
        # Merge and write report
        summary_df = reduce(lambda df1, df2: df1.unionByName(df2), counts)
        summary_df.show()
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M")
        filename = f"pnc_deletes_{timestamp}.csv"
        final_path = f"{self._tmp_dir}/report_summary/deletes/{filename}"
        summary_df.coalesce(1).write.mode("overwrite").option("header", True).csv(final_path)
        #summary_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{self._tmp_dir}/report_summary/deletes/pnc_deletes.csv")
    
        # Apply deletes
        delete_plan = [
            ("releasedetails", df_period_fks),
            ("periodininstitution", df_custody_fks),
            ("custody", df_caseheader_fks),
            ("mokeywordlocation", df_method_fks),
            ("mokeywordvictim", df_method_fks),
            ("method", df_caseheader_fks),
            ("disposal", df_offence_fks),
            ("cooffender", df_offence_fks),
            ("subsequentappearance", df_offence_fks),
            ("offence", df_caseheader_fks),
            ("bailconditions", df_caseheader_fks),
            ("breachofbail", df_caseheader_fks),
            ("condition", df_caseheader_fks),
            ("oiconditions", df_operationalinformation_fks),
            ("operationalinformation", df_nominal_fks),
            ("occupation", df_nominal_fks),
            ("informationmarker", df_nominal_fks),
            ("caseheader", df_nominal_fks),
        ]
    
        for table, df_fk in delete_plan:
            self.logger.info(f"[Delete] Applying deletes for {table}")
            self.nominal_fk_delete(table, df_fk, sink, df_flatten)
    
        # Handle nominal separately (delete by pncid)
        self.logger.info(f"[Delete] Applying deletes for nominal")
        pncids = [row["pncid"] for row in df_nominal_fks.select("pncid").distinct().collect()]
        if pncids:
            key_list = ",".join(f"'{p}'" for p in pncids)
            table_key = table_and_key.get_table_key("pnc_nphord_nominal")
            sink.rr_write_deletes(df_flatten, "pnc_nphord_nominal", key_list, table_key)

    def read(self):
        """Read source data into delta frames."""
        # call sink to get S3 paths for configured entities
        self.logger.info('Read')
        paths = self._sink.get_paths()
        self.logger.info(f'paths={paths}')

        for path in paths:
            try:
                # read the Avro data
                self.logger.info(f'read=[{paths[path]}]')

                self.logger.info('Attempting to read from bucket.')
                self._delta_frames['read'][path] = self._spark \
                    .read \
                    .format('parquet') \
                    .load(paths[path])
                self.logger.info("Data read")
                #   .load("s3://dsa-cdl-police-s3-nas-dev/consolidated/landing-1/Year=2021/Month=05/Day=28/Hour=10/Minute=20/")

                self._delta_frames['read'][path].printSchema()
                self._delta_frames['read'][path].show()
                self.logger.info(f"Read count: {self._delta_frames['read'][path].count()}")
                self._delta_frames['read'][path].createOrReplaceTempView(path)

            except Exception as err:
                # log failure to read for metrics to report
                self.logger.error(err)
                self._metrics.put_count_metric('read_count', 0, [{'Name': 'entity', 'Value': path}])
                self._metrics.put_count_metric('nop_read_error', 1, [{'Name': 'entity', 'Value': path}])


    # def filter(self):
    #     """Filter data in delta_frames using datasource impl."""
    #     self.logger.info('filter')
    #     if self._is_read:
    #         self._datasource.filter(self._delta_frames)
    #         self._is_filter = True
    #     else:
    #         print('WARN: Filter called before data read')

    def transform(self):
        """Transform read data for Redshift."""
        if self._refresh_state == "upsert" or self._refresh_state == "bulk":
            self.logger.info('transform')
            self._datasource.transform(self._delta_frames)

    def relationalize(self):
        """Flatten read data for Redshift."""
        if self._refresh_state == "upsert" or self._refresh_state == "bulk":
            self.logger.info('relationalize')
            self._datasource.relationalize(
                self._delta_frames,
                self._glue_context)

    # def generate_deletes(self):
    #     """Generate delete lists in delta_frames using datasource impl."""
    #     self.logger.info('generate_deletes')
    #     if self._is_read:
    #         self._datasource.generate_deletes(self._delta_frames)
    #     else:
    #         self.logger.warning('generate_deletes called before data read')

    # def write_deletes(self, sink):
    #     """Write delete dataframes to redshift."""
    #     self.logger.info('write_deletes')
    #     if self._is_read:
    #         for e in self._delta_frames['delete']:
    #             self.logger.info(f'delete delta_frame found [{e}]')
    #             df = self._delta_frames['delete'][e]
    #             self.logger.info(f"write delete [{e}_delete_{self._args['DATAFEED_ARG']}]")
    #             sink.write(df, f"{e}_delete_{self._args['DATAFEED_ARG']}")
    #     else:
    #         self.logger.warning('write_deletes called before data read')

    def write_inserts(self, sink):
        """Write read dataframes to redshift."""
        if self._delete != "delete":
            for e in self._delta_frames['read']:
                self.logger.info(f'insert delta_frame found [{e}]')
                df = self._delta_frames['read'][e]

                # count df here for metrics/alarms
                self._metrics.put_count_metric('read_count', df.count(), [{'Name': 'entity', 'Value': e}])

                self.logger.info(f"write insert [{e}]")
                #self.logger.info(df.show())
                sink.write(df, f"{e}")

                self.logger.info(f"rr write insert [{e}]")
                #self.logger.info(df.show())
                #sink.rr_write(df, f"{e}")

                self.logger.info(f"read table - [{e}] from redshift")
                #sink.read(f"{e}")

                # drop if cached
                df.unpersist()

    def write_inserts_relationalize(self, sink):
        """Write read dataframes to redshift."""

        if self._refresh_state == "bulk" or self._refresh_state == "upsert":
            for entity in self._delta_frames['relationalize']:

                self.logger.info(f'insert delta_frame found [{entity}]')
                for key in self._delta_frames['relationalize'][entity]['df']:

                    df = self._delta_frames['relationalize'][entity][key]
                    
                    # Define a window partitioned by all columns
                    window = Window.partitionBy(df.columns)

                    # Add a count column over the window
                    df_with_count = df.withColumn("dup_count", f.count("*").over(window))

                    self.logger.info('Dropping duplicates before')
                    # All duplicates
                    df_duplicates = df_with_count.filter(f.col("dup_count") > 1).drop("dup_count")
                    df_duplicates.show(truncate=False)

                    # Unique rows only
                    self.logger.info('Dropping duplicates after')
                    #df_unique = df_with_count.filter(f.col("dup_count") == 1).drop("dup_count")
                    df = df.dropDuplicates()
                    df.show(truncate=False)

                    # count df here for metrics/alarms
                    self._metrics.put_count_metric('read_count', df.count(), [{'Name': 'entity', 'Value': key}])

                    self.logger.info(f"bulk write insert [{key}]")
                    if self._refresh_state == "bulk":
                        sink.write(df, f"{self._args['DATASOURCE_ARG']}_{self._args['DATAFEED_ARG']}_{key}")
                    
                    table_key = table_and_key.get_table_key(f"{self._args['DATASOURCE_ARG']}_{self._args['DATAFEED_ARG']}_{key}")

                    self.logger.info(f"weekly rr write stage insert [{key}]")
                    if self._refresh_state == "upsert":
                        sink.rr_write_to_stage(df, f"{self._args['DATASOURCE_ARG']}_{self._args['DATAFEED_ARG']}_{key}")

                    # drop if cached
                    df.unpersist()

    # def only_latest(self):
    #     """Generate delete lists in delta_frames using datasource impl."""
    #     self.logger.info('only_latest')

    #     if self._is_read:
    #         self._datasource.only_latest(self._delta_frames)
    #     else:
    #         self.logger.warning('only_latest called before data read')
