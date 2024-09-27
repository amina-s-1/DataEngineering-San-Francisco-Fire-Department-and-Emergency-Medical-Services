# Incident Data Pipeline Project

This project implements a data processing pipeline using Apache Spark on Databricks. The goal is to process incident-related data in three stages (Bronze, Silver, Gold), representing different data refinement and aggregation levels. The project is designed to provide insights such as incident volumes and average response times.

## Project Structure

### 1. **Config.py**
   - The configuration file sets up essential paths and variables:
     - `base_dir_data`: Location of raw data.
     - `base_dir_checkpoint`: Location for checkpointing streaming data.
     - `db_name`: The database name used for storing tables.
   - This is used throughout the pipeline to ensure consistent data paths.

### 2. **Setup.py**
   - The setup helper initializes the environment and creates the necessary database and tables.
   - Creates the following tables:
     - `incident_bz` (Bronze layer)
     - `incident_log` (log data)
     - `incident_volume_neighbourhood` (aggregated neighborhood data)
     - `incident_avg_response` (aggregated average response time)
   - Supports validation of table creation and cleanup of the database and checkpoint locations.

### 3. **Bronze.py**
   - Ingests raw incident data into the `incident_bz` table, defining the schema for the raw dataset.
   - The raw data includes fields like call numbers, dates, incident numbers, and other emergency service information.

### 4. **Silver.py**
   - Transforms and cleans the raw data from the Bronze layer.
   - Converts date strings into timestamp formats and filters out any incomplete records.
   - Writes the cleaned data back into tables for further analysis.

### 5. **Gold.py**
   - Generates insights from the cleaned data.
   - Aggregates data such as:
     - Incident volume per neighborhood.
     - Average response times based on call type and priority.
   - Results are stored in the `incident_volume_neighbourhood` and `incident_avg_response` tables.

### 6. **Run.py**
   - Executes the pipeline from start to finish, triggering the setup, Bronze, Silver, and Gold layers.
   - Allows configuration of batch or streaming modes via widgets:
     - `Environment`: Catalog name (e.g., "dev").
     - `RunType`: Specifies batch (`once`) or streaming.
     - `ProcessingTime`: Micro-batch interval for streaming mode.
     - `StartTime` and `EndTime`: Specify the time range for data processing.

## Usage

1. Set up your Databricks environment.
2. Configure the required external locations and catalog names in the `01-Config.py` file.
3. Run the `02-Setup.py` to create the necessary database and tables.
4. Run the pipeline:
   - Use `06-Run.py` to run the full pipeline, including Bronze, Silver, and Gold layers.
   - Configure parameters via the widgets provided in the notebook to control processing time and mode (batch/streaming).
     
## Configuration

The project requires a configuration file that sets up the necessary environment variables, such as:
- `base_dir_data`: Base directory for data storage.
- `base_dir_checkpoint`: Directory for checkpointing streaming data.
- `db_name`: The name of the database where tables will be stored.

Ensure that the configuration file is correctly set up before running the scripts.

## Data Flow Architecture

![image](https://github.com/user-attachments/assets/eaa7748a-bab4-43a3-83db-513c795aa4c9)


### Example Commands to Check Results:

```sql
-- Check average response times
SELECT * from dev.fire_db.incident_avg_response;

-- Check raw incident data filtered by date
SELECT * FROM dev.fire_db.incident_bz WHERE call_date BETWEEN '2018-01-01' AND '2018-01-02';


