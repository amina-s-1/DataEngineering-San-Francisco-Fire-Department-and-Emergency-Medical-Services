# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class Upserter:
    def __init__(self, merge_query, temp_view_name):
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name 
        
    def upsert(self, df_micro_batch, batch_id):
        df_micro_batch.createOrReplaceTempView(self.temp_view_name)
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)

# COMMAND ----------

class Gold():
    def __init__(self, env,start_date, end_date):
        self.Conf = Config() 
        self.checkpoint_base = self.Conf.base_dir_checkpoint
        self.catalog = env
        self.start_date = start_date
        self.end_date = end_date
        self.db_name = self.Conf.db_name
        self.maxFilesPerTrigger = self.Conf.maxFilesPerTrigger
        spark.sql(f"USE {self.catalog}.{self.db_name}")





    def _await_queries(self, once):
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()

    def get_incident(self,start_date , end_date ):
        from pyspark.sql import functions as F
        
        return (spark.readStream
                .option("ignoreDeletes", True)
                .table(f"{self.catalog}.{self.db_name}.incident_log")
                .where((F.col("call_date") >= self.start_date) & (F.col("call_date") <= self.end_date))
        )
                
    def get_incident_volume(self, df_incident, once=True, processing_time="15 seconds"):
        from pyspark.sql import functions as F
        import time
        start = int(time.time())
        df_incident_volume = (df_incident.groupBy("neighborhoods_analysis_boundaries",F.year("call_date").alias("year"),F.month("call_date").alias("month"))
                                .agg(F.count("*").alias("incident_count")) 
                                
                                )
        
        
        stream_writer = (df_incident_volume.writeStream
                                 .outputMode("complete")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/incident_volume_neighbourhood")
                                 .queryName("incident_volume_neighbourhood")
                        )       
        
        if once == True:
            return (stream_writer.trigger(availableNow=True)
                    .toTable(f"{self.catalog}.{self.db_name}.incident_volume_neighbourhood"))
        else:
            return (stream_writer.trigger(processingTime=processing_time)
                    .toTable(f"{self.catalog}.{self.db_name}.incident_volume_neighbourhood"))
        print(f"Completed incident_volume_neighbourhood consumtion {int(time.time()) - start} seconds")

    def get_avg_response(self, df_incident, once=True, processing_time="15 seconds"):
        import time
        start = int(time.time())
        from pyspark.sql import functions as F
        df_incident_response =( df_incident.groupBy("call_type_group", "priority",F.year("call_date").alias("year"),F.month("call_date").alias("month"))
                                        .agg(F.avg(F.unix_timestamp('available_dttm') - F.unix_timestamp('received_dttm')).alias('avg_response_time_seconds'), F.avg("number_of_alarms").alias("avg_alarms")))
        
        
        stream_writer = (df_incident_response.writeStream
                                 .outputMode("complete")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/incident_avg_response")
                                 .queryName("incident_avg_response")
                        )       

        if once == True:
            return (stream_writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.incident_avg_response"))
        else:
            return (stream_writer.trigger(processingTime=processing_time).toTable(f"{self.catalog}.{self.db_name}.incident_avg_response"))
        

    def result(self,once=True, processing_time="5 seconds", start_date = "2024-08-01", end_date = "2024-08-07"):
        import time
        start = int(time.time())
        print(f"\nExecuting gold layer result ...")
        
        df_incident = self.get_incident(start_date, end_date)
       
        self.get_incident_volume(df_incident,once, processing_time)
        self._await_queries(once)
        self.get_avg_response(df_incident,once, processing_time)
        self._await_queries(once)
        print(f"Completed gold layer consumtion {int(time.time()) - start} seconds")


    
