# Incident Data Pipeline Project

This project implements a data processing pipeline using Apache Spark on Databricks. The goal is to process incident-related data in three stages (Bronze, Silver, Gold), representing different data refinement and aggregation levels. The project is designed to provide insights such as incident volumes and average response times.

## Project Structure

### Bronze Layer
The Bronze layer is responsible for:
- Ingesting raw data into the system.
- Defining the schema for the dataset, which includes fields such as:
  - Call Number
  - Unit ID
  - Incident Number
  - Call Date, Received Date, Dispatch Date, etc.
- Saving the raw data into the initial tables for further processing.

### Silver Layer
The Silver layer is responsible for:
- Cleaning and transforming the raw data.
- Converting string representations of dates into timestamp formats.
- Filtering and removing any null or incomplete data entries.
- Preparing a structured dataset for analysis and aggregation.

### Gold Layer
The Gold layer is responsible for:
- Aggregating data to generate useful insights.
- Calculating the incident volume per neighborhood.
- Computing average response times by call type and priority.
- Storing the aggregated results in output tables for reporting and visualization.

## Usage

1. Set up your Databricks environment.
2. Run the `Bronze.py` script to ingest raw data.
3. Run the `Silver.py` script to clean and transform the data.
4. Run the `Gold.py` script to generate reports and insights.

## Configuration

The project requires a configuration file that sets up the necessary environment variables, such as:
- `base_dir_data`: Base directory for data storage.
- `base_dir_checkpoint`: Directory for checkpointing streaming data.
- `db_name`: The name of the database where tables will be stored.

Ensure that the configuration file is correctly set up before running the scripts.

## License

This project is licensed under the MIT License. See the `LICENSE` file for more information.
