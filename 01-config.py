# Databricks notebook source
class Config():    
    def __init__(self):      
        self.base_dir_data = spark.sql("describe external location `fd-raw`").select("url").collect()[0][0]
        #self.base_dir_landing= spark.sql("describe external location `fire_project_landingzone`").select("url").collect()[0][0]       
        ##display(self.base_dir_data)
        self.base_dir_checkpoint = spark.sql("describe external location `fd-checkpoint`").select("url").collect()[0][0]
        self.base_dir_database = spark.sql("describe external location `fd-database-location`").select("url").collect()[0][0]
        self.db_name = "fire_db"
        self.maxFilesPerTrigger = 1000



