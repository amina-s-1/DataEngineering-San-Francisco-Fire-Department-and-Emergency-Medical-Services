# Databricks notebook source
# MAGIC %run ./02-setup

# COMMAND ----------

# MAGIC %run ./03-Bronze

# COMMAND ----------

# MAGIC %run ./04-Silver

# COMMAND ----------

# MAGIC %run ./05-Gold

# COMMAND ----------

dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
dbutils.widgets.text("RunType", "once", "Set once to run as a batch")
dbutils.widgets.text("ProcessingTime", "5 seconds", "Set the microbatch interval")


# COMMAND ----------

dbutils.widgets.text("StartTime", "2024-08-01", "Set the start date")
dbutils.widgets.text("EndTime", "2024-08-02", "Set the end date")

# COMMAND ----------

env = dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
once = True if dbutils.widgets.get("RunType")=="once" else False
processing_time = dbutils.widgets.get("ProcessingTime")
if once:
    print(f"Starting sbit in batch mode.")
else:
    print(f"Starting sbit in stream mode with {processing_time} microbatch.")

# COMMAND ----------

env = dbutils.widgets.get("Environment")
start_date = dbutils.widgets.get("StartTime")
end_date = dbutils.widgets.get("EndTime")

# COMMAND ----------

Sh = SetupHelper(env)

Sh.setup()


# COMMAND ----------

Bz = Bronze(env)
Bz.process()

# COMMAND ----------

Sl = Silver(env)
Sl.upsert_sl()

# COMMAND ----------

Gl = Gold(env, start_date, end_date)
Gl.result()

# COMMAND ----------

# MAGIC %md
# MAGIC ###To check the result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from dev.fire_db.incident_avg_response

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * FROM dev.fire_db.incident_bz where call_date between '2018-01-01' and '2018-01-02'

# COMMAND ----------

#clean up
#sh = SetupHelper(env)
#sh.cleanup()
