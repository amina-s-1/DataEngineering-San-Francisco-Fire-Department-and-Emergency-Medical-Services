# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class Bronze():
    def __init__(self, env):        
        self.Conf = Config()
        self.dir_data = self.Conf.base_dir_data
        self.checkpoint = self.Conf.base_dir_checkpoint
        self.catalog = env
        self.db_name = self.Conf.db_name
        spark.sql(f"USE {self.catalog}.{self.db_name}") 

    
        
        
    # Define schema for the dataset
    def getschema(self):
        from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, DateType, TimestampType
        return (StructType([
                StructField("Call Number", LongType(), True),
                StructField("Unit ID", StringType(), True),
                StructField("Incident Number", IntegerType(), True),
                StructField("Call Type", StringType(), True),
                StructField("Call Date", StringType(), True),
                StructField("Watch Date", StringType(), True),
                StructField("Received DtTm", StringType(), True),
                StructField("Entry DtTm", StringType(), True),
                StructField("Dispatch DtTm", StringType(), True),
                StructField("Response DtTm", StringType(), True),
                StructField("On Scene DtTm", StringType(), True),
                StructField("Transport DtTm", StringType(), True),
                StructField("Hospital DtTm", StringType(), True),
                StructField("Call Final Disposition", StringType(), True),
                StructField("Available DtTm", StringType(), True),
                StructField("Address", StringType(), True),
                StructField("City", StringType(), True),
                StructField("Zipcode of Incident", StringType(), True),
                StructField("Battalion", StringType(), True),
                StructField("Station Area", StringType(), True),
                StructField("Box", StringType(), True),
                StructField("Original Priority", StringType(), True),
                StructField("Priority", StringType(), True),
                StructField("Final Priority", StringType(), True),
                StructField("ALS Unit", StringType(), True),
                StructField("Call Type Group", StringType(), True),
                StructField("Number of Alarms", IntegerType(), True),
                StructField("Unit Type", StringType(), True),
                StructField("Unit sequence in call dispatch", IntegerType(), True),
                StructField("Fire Prevention District", StringType(), True),
                StructField("Supervisor District", IntegerType(), True),
                StructField("Neighborhooods - Analysis Boundaries", StringType(), True),
                StructField("RowID", StringType(), True),
                StructField("case_location", StringType(), True),
                StructField("data_as_of", StringType(), True),
                StructField("data_loaded_at", StringType(), True)
        ]))
        
        

    def incident_load(self):
        from pyspark.sql.functions import  current_timestamp,input_file_name
        
        return (spark.readStream
                        .format("csv")
                        .schema(self.getschema())
                        .option("header", "true")
                        .load(self.dir_data)
                        .withColumn("load_time", current_timestamp()) 
                        .withColumn("source_file", input_file_name())
                        .drop("received_dttm")
                    )
        
    
    #Renaming the column name 
    def rename_column(self,invoice_df):   
        columns = invoice_df.columns
        cleaned_columns = []
        for column in columns:
            cleaned_column = column.lower().replace(' ', '_')
            cleaned_columns.append(cleaned_column)
        return (invoice_df.toDF(*cleaned_columns))
    
    def time_tf(self,invoice_df):
        from pyspark.sql.functions import  col,date_format, year, month, day, to_timestamp,from_unixtime,unix_timestamp



        df = (invoice_df.withColumn("call_date", from_unixtime(unix_timestamp(col("call_date"), "MM/dd/yyyy"),'yyyy-MM-dd').cast("date"))
                .withColumn("watch_date", from_unixtime(unix_timestamp(col("watch_date"), "MM/dd/yyyy"),'yyyy-MM-dd').cast("date"))
                .withColumn("received_dttm", from_unixtime(unix_timestamp(col("received_dttm"), "MM/dd/yyyy hh:mm:ss a"),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
                .withColumn("entry_dttm", from_unixtime(unix_timestamp(col("entry_dttm"),  "MM/dd/yyyy hh:mm:ss a"),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
                .withColumn("dispatch_dttm", from_unixtime(unix_timestamp(col("dispatch_dttm"),  "MM/dd/yyyy hh:mm:ss a"),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
                .withColumn("response_dttm", from_unixtime(unix_timestamp(col("response_dttm"),  "MM/dd/yyyy hh:mm:ss a"),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
                .withColumn("on_scene_dttm", from_unixtime(unix_timestamp(col("on_scene_dttm"),  "MM/dd/yyyy hh:mm:ss a"),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
                .withColumn("transport_dttm", from_unixtime(unix_timestamp(col("transport_dttm"),  "MM/dd/yyyy hh:mm:ss a"),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
                .withColumn("hospital_dttm", from_unixtime(unix_timestamp(col("hospital_dttm"),  "MM/dd/yyyy hh:mm:ss a"),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
                .withColumn("available_dttm", from_unixtime(unix_timestamp(col("available_dttm"),  "MM/dd/yyyy hh:mm:ss a"),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
                .withColumn("data_as_of", from_unixtime(unix_timestamp(col("data_as_of"),  "MM/dd/yyyy hh:mm:ss a"),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
                .withColumn("data_loaded_at", from_unixtime(unix_timestamp(col("data_loaded_at"), "MM/dd/yyyy hh:mm:ss a"),'yyyy-MM-dd HH:mm:ss').cast("timestamp")))
        return (df.withColumn("incident_year", year("call_date"))
                    .withColumnRenamed("neighborhooods_-_analysis_boundaries", "neighborhoods_analysis_boundaries"))
    
    
                 
               

        
    
    def appendIncident (self, invoice_df, once=True, processing_time="5 seconds"):
        sQuery = (invoice_df.writeStream 
                            .format("delta") 
                            .option("checkpointLocation",f"{self.checkpoint}/incident_bz")
                            .outputMode("append") )
        if once == True:
            return (sQuery.trigger(availableNow =True)
                     .toTable(f"{self.catalog}.{self.db_name}.incident_bz"))
        else:
            return (sQuery.trigger(processingTime = processing_time)
                     .toTable(f"{self.catalog}.{self.db_name}.incident_bz"))



    def process(self,once=True, processing_time="5 seconds"):
        print("Bronze Layer initiated")
        import time
        start = int(time.time())
        readinvoice_df = self.incident_load()
        columnRename_df = self.rename_column(readinvoice_df)
        timeTf_df = self.time_tf(columnRename_df)
        
        self.appendIncident(timeTf_df,once, processing_time)
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()
        
        print(f"Completed bronze layer consumtion {int(time.time()) - start} seconds")
         





        

