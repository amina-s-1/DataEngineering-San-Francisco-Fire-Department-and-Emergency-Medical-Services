# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class Silver():
    def __init__(self, env):        
        self.Conf = Config()
        self.checkpoint_base = self.Conf.base_dir_checkpoint
        self.catalog = env
        self.db_name = self.Conf.db_name
        spark.sql(f"USE {self.catalog}.{self.db_name}")

    def upsert (self, incident_df, batch_id):
        incident_df.createOrReplaceTempView("incident_df_temp_view")
        merge_statement = f"""
        MERGE INTO {self.catalog}.{self.db_name}.incident_log a
        USING incident_df_temp_view b ON a.incident_number =  b.incident_number AND
        a.unit_id = a.unit_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        incident_df._jdf.sparkSession().sql(merge_statement)
    def _await_queries(self, once):
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()

    def upsert_incident(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        df_delta =  (spark.readStream
                .option("startingVersion", startingVersion)
                .option("ignoreDeletes", True)
                .table(f"{self.catalog}.{self.db_name}.incident_bz")
                .select("incident_year","incident_number","call_number","call_type","unit_id","neighborhoods_analysis_boundaries","call_date","call_type_group","priority","dispatch_dttm","on_scene_dttm","unit_type","received_dttm","available_dttm","number_of_alarms")
                .withColumn("call_type_group", F.coalesce(F.col("call_type_group"), F.lit("Others")))
                .withWatermark("received_dttm", "30 seconds")
                .dropDuplicates(["incident_number","unit_id"])
                   )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(self.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/incident_log")
                                 .queryName("invoce_logs_upsert_stream")
                        )       
        
        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
        #print(f"Completed silver layer consumtion {int(time.time()) - start} seconds")


    def upsert_sl(self, once=True, processing_time="5 seconds"):
        import time
        start = int(time.time())
        print(f"\nExecuting silver layer upsert ...")
        self.upsert_incident(once, processing_time)
        self._await_queries(once)
        print(f"Completed silver layer consumtion {int(time.time()) - start} seconds")
        



