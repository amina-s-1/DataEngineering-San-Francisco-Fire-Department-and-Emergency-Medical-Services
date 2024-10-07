# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class SetupHelper():   
    def __init__(self, env):
        Conf = Config()
        #self.base_dir_landing = Conf.base_dir_landing 
        self.base_dir_checkpoint = Conf.base_dir_checkpoint 
        self.base_dir_data = Conf.base_dir_data 
        self.database_location = Conf.base_dir_database
        self.catalog = env
        self.db_name = Conf.db_name
        self.initialized = False
        
    def create_db(self):
        spark.catalog.clearCache()
        print(f"Creating the database {self.catalog}.{self.db_name}...", end='')
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.db_name} MANAGED LOCATION '{self.database_location}'")
        spark.sql(f"USE {self.catalog}.{self.db_name}")
        self.initialized = True
        print("Done")

    def create_incident(self):
        if(self.initialized):
            print(f"Creating Incident table...", end='')
            
            spark.sql(f"""
                            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.incident_bz (
                                call_number BIGINT,
                                unit_id VARCHAR(255),
                                incident_number INT,
                                call_type VARCHAR(255),
                                call_date Date,
                                watch_date Date,
                                received_dttm TIMESTAMP,
                                entry_dttm TIMESTAMP,
                                dispatch_dttm TIMESTAMP,
                                response_dttm TIMESTAMP,
                                on_scene_dttm TIMESTAMP,
                                transport_dttm TIMESTAMP,
                                hospital_dttm TIMESTAMP,
                                call_final_disposition VARCHAR(255),
                                available_dttm TIMESTAMP,
                                address VARCHAR(255),
                                city VARCHAR(255),
                                zipcode_of_incident VARCHAR(255),
                                battalion VARCHAR(255),
                                station_area VARCHAR(255),
                                box VARCHAR(255),
                                original_priority VARCHAR(255),
                                priority VARCHAR(50),
                                final_priority VARCHAR(255),
                                als_unit VARCHAR(255),
                                call_type_group VARCHAR(255),
                                number_of_alarms INT,
                                unit_type VARCHAR(255),
                                unit_sequence_in_call_dispatch INT,
                                fire_prevention_district VARCHAR(255),
                                supervisor_district INT,
                                neighborhoods_analysis_boundaries VARCHAR(255),
                                rowid VARCHAR(255),
                                case_location VARCHAR(255),
                                data_as_of TIMESTAMP,
                                data_loaded_at TIMESTAMP,
                                load_time TIMESTAMP,
                                source_file VARCHAR(255),
                                incident_year INT                             
                        )PARTITIONED BY (incident_year)                  
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_incidentlogs(self):
        if(self.initialized):
            print(f"Creating incident_log table...", end='')            
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.incident_log (
                    incident_year INT,
                    incident_number INT,
                    call_number BIGINT,
                    call_type VARCHAR(255),
                    unit_id VARCHAR(255),
                    neighborhoods_analysis_boundaries VARCHAR(255),
                    call_date Date,
                    call_type_group VARCHAR(255),
                    priority VARCHAR(50),
                    dispatch_dttm TIMESTAMP,
                    on_scene_dttm TIMESTAMP,
                    unit_type VARCHAR(255),
                    received_dttm TIMESTAMP,
                    available_dttm TIMESTAMP,
                    number_of_alarms INT
                )PARTITIONED BY (incident_year) 
                """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
    
    def create_incidentneighbourhood(self):
        if(self.initialized):
            print(f"Creating incident_volume_neighbourhood table...", end='')            
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.incident_volume_neighbourhood(
                    year INT,
                    month INT,
                    incident_count BIGINT,
                    neighborhoods_analysis_boundaries VARCHAR(255)
                )
                """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_incidentresponse(self):
        if(self.initialized):
            print(f"Creating incident_avg_response table...", end='')            
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.incident_avg_response (
                year INT,
                month INT,
                call_type_group VARCHAR(255),
                priority VARCHAR(50),
                avg_response_time_seconds DOUBLE,
                avg_alarms DOUBLE
            
            )
            """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")


    def setup(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup ...")
        self.create_db()       
        self.create_incident()
        self.create_incidentlogs()
        self.create_incidentneighbourhood()
        self.create_incidentresponse()
        print(f"Setup completed in {int(time.time()) - start} seconds")

    def validate(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup validation ...")
        assert spark.sql(f"SHOW DATABASES IN {self.catalog}") \
                    .filter(f"databaseName == '{self.db_name}'") \
                    .count() == 1, f"The database '{self.catalog}.{self.db_name}' is missing"
        print(f"Found database {self.catalog}.{self.db_name}: Success")
        self.assert_table("incident_bz")   
        self.assert_table("incident_log") 
        self.assert_table("incident_volume_neighbourhood") 
        self.assert_table("incident_avg_response") 
            

    def cleanup(self): 
        if spark.sql(f"SHOW DATABASES IN {self.catalog}").filter(f"databaseName == '{self.db_name}'").count() == 1:
            print(f"Dropping the database {self.catalog}.{self.db_name}...", end='')
            spark.sql(f"DROP DATABASE {self.catalog}.{self.db_name} CASCADE")
            print("Done")
        #print(f"Deleting {self.base_dir_data}...", end='')
        #dbutils.fs.rm(self.base_dir_data, True)
        #print("Done")
        print(f"Deleting {self.base_dir_checkpoint}...", end='')
        dbutils.fs.rm(self.base_dir_checkpoint, True)
        print("Done")    
        #print(f"Deleting {self.landing_zone}...", end='')
        #dbutils.fs.rm(self.landing_zone, True)
        #print("Done")

