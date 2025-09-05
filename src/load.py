import pandas as pd
from datetime import datetime
from typing import Dict
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import GoogleAPIError

# Function to check if a string is a valid JSON
def is_valid_json(json_str):
    import json
    try:
        json_str = str(json_str)
        json.loads(json_str)
        try:
            int(json_str)
            return False
        except:
            try:
                float(json_str)
                return False
            except ValueError:
                return True
    except ValueError:
        return False

def CreateAndLoad_BQ(data_dict: Dict[str, pd.DataFrame], bq: bigquery.Client, 
                    project_id: str, dataset_id: str, prefix: str = None, deleted_if_exist: bool = False, load_data: bool = False) -> None:
    """
    Create tables in BigQuery for each DataFrame in the provided dictionary.
    Only append data if the maximum date in the existing table is older than today, unless deleted_if_exist is True.

    Args:
        data_dict (Dict[str, pd.DataFrame]): Dictionary with table names as keys and pandas DataFrames as values.
        bq (bigquery.Client): BigQuery client instance for interacting with BigQuery.
        project_id (str): Google Cloud project ID.
        dataset_id (str): BigQuery dataset ID where the tables will be created.
        prefix (str, optional): Prefix to add to each table name. Defaults to None.
        deleted_if_exist (bool, optional): If True, delete the table if it already exists. Defaults to False.
        load_data (bool, optional): If True, load data into the created tables. Defaults to False.

    Raises:
        google.api_core.exceptions.GoogleAPIError: If an error occurs during table creation.
        TypeError: If the data for a label is not a pandas DataFrame.
        KeyError: If a data type for a column is not supported.
        Exception: For any other unexpected errors.
    """

    type_data_to_bq = {
    'object':   'STRING',
    'bool':     'BOOLEAN',
    'int32':    'INTEGER',
    'Int32':    'INTEGER',
    'int64':    'INTEGER',
    'Int64':    'INTEGER',
    'float32':  'FLOAT',
    'float64':  'FLOAT',
    'datetime64[ns]': 'DATETIME',
    'datetime64[us]' : 'DATETIME'
    }

    for label, data in data_dict.items():
        try:
            # Ensure data is a DataFrame
            if not isinstance(data, pd.DataFrame):
                raise TypeError(f"The data for label '{label}' is not a pandas DataFrame.")

            # Infer data types
            data_dict_type = dict(zip(data.columns, data.dtypes.astype(str)))

            # Generate Schema to create table in BigQuery
            data_schema = []
            for key, value in data_dict_type.items():
                try:
                    if value == 'object':
                        if data[key].apply(lambda x: is_valid_json(x)).all():
                            field_type = 'STRING'
                        else:
                            field_type = type_data_to_bq.get(value, 'STRING')  # Default to STRING for unsupported types
                    else:
                        field_type = type_data_to_bq.get(value, 'STRING')
                except KeyError:
                    raise KeyError(f"Data type '{value}' for column '{key}' is not supported.")

                data_schema.append(bigquery.SchemaField(key, field_type, mode='NULLABLE'))

            # Build the table_id
            table_name = prefix + label if prefix else label
            table_id = f'{project_id}.{dataset_id}.{table_name}'

            # Check if deleted_if_exist is True, if so, delete the table
            if deleted_if_exist:
                try:
                    bq.get_table(table_id)  # Check if the table exists
                    bq.delete_table(table_id)  # Delete the table
                    print(f"\nTable: {table_id} Deleted.")
                except NotFound:
                    pass  # Ignore if the table doesn't exist

            # Now check if the table exists
            try:
                bq.get_table(table_id)  # Try to get the table info
                print(f"Table {table_id} already exists. Checking max date before loading data.")
                table = bq.get_table(table_id)  # Get table details using the same client

                if not deleted_if_exist:
                    # Query to get the maximum date from the table
                    query = f"""
                        SELECT MAX(assignment_date) AS max_date
                        FROM `{table_id}`
                    """
                    query_job = bq.query(query)
                    result = query_job.result()
                    max_date = next(result).max_date  # Get the maximum date

                    # Compare with today's date
                    today = pd.to_datetime(datetime.today().date())
                    if max_date == today:
                        print(f"Table {table_id} has data for today. No new data will be appended.")
                        continue  # Skip to the next table if max date is today

                    print(f"Max date is {max_date}. New data will be appended.")

            except NotFound:
                print(f"\nTable: {table_id} does not exist. Creating new table.")
                # Create the table if it does not exist
                table = bigquery.Table(table_id, schema=data_schema)
                table = bq.create_table(table)
                print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

            if load_data:
                # Set job configuration
                job_config = bigquery.LoadJobConfig(
                    schema            = data_schema,
                    write_disposition = 'WRITE_APPEND'  # Ensure data is appended to the table
                )

                # Load data to BigQuery
                print("\n* Load Data ...")
                job = bq.load_table_from_dataframe(data, table_id, job_config=job_config)
                job.result()  # Wait for the job to complete

                # Get the table information
                table = bq.get_table(table_id)
                print(f"\t- Loaded table... {table_id}")
                print(f"\t- {table.num_rows} rows, {len(table.schema)} columns")

        except (TypeError, KeyError) as e:
            print(f"Error processing label '{label}': {e}")
            continue  # Proceed to the next item

        except GoogleAPIError as e:
            print(f"GoogleAPIError while creating table '{table_id}': {e}")
            raise  # Re-raise the exception to notify the calling code

        except Exception as e:
            print(f"An unexpected error occurred for label '{label}': {e}")
            raise  # Re-raise unexpected exceptions