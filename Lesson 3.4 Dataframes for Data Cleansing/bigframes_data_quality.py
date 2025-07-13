import bigframes.pandas as bpd
from google.cloud import bigquery
import datetime

# Replace with your BigQuery project ID
project_id = 'bq-data-ml-engineering-459914'  # Use the project ID you provided
dataset_name = 'retail_dataset'  # Name of your dataset

def generate_descriptive_statistics_bigframes(project_id, dataset_name, table_name):
    """
    Generates and prints descriptive statistics for a table in BigQuery using bigframes.

    Args:
        project_id (str): The ID of the BigQuery project.
        dataset_name (str): The name of the dataset.
        table_name (str): The name of the table.
    """
    try:
        # Construct the full table ID
        table_id = f"{project_id}.{dataset_name}.{table_name}"

        # Load the table into a bigframes DataFrame
        bf_df = bpd.read_gbq(table_id)

        print(f"\nDescriptive Statistics for table: {table_name}")
        print("-" * (len(table_name) + 30))  # Adjust separator length

        # Generate and print descriptive statistics using bigframes
        print(bf_df.describe())

    except Exception as e:
        print(f"Error processing table {table_name}: {e}")



def get_table_schema(client, project_id, dataset_name, table_name):
    """
    Retrieves the schema of a table in BigQuery.

    Args:
        client: BigQuery client object.
        project_id (str): The ID of the BigQuery project.
        dataset_name (str): The name of the dataset.
        table_name (str): The name of the table.

    Returns:
        list: A list of SchemaField objects representing the table's schema,
              or None if the table does not exist.
    """
    try:
        table_id = f"{project_id}.{dataset_name}.{table_name}"
        table = client.get_table(table_id)
        return table.schema
    except Exception as e:
        print(f"Error getting schema for table {table_name}: {e}")
        return None

def print_table_schema(schema, table_name):
    """
    Prints the schema of a table in a user-friendly format.

    Args:
        schema (list): A list of SchemaField objects representing the table's schema.
        table_name (str): The name of the table.
    """
    if schema is None:
        print(f"Could not retrieve schema for table: {table_name}")
        return

    print(f"\nSchema for table: {table_name}")
    print("-" * (len(table_name) + 14))  # Adjust the length of the separator
    for field in schema:
        print(f"  Name: {field.name}, Type: {field.field_type}, Mode: {field.mode}")

def perform_data_quality_checks(project_id, dataset_name, table_name):
    """
    Performs data quality checks on a table in BigQuery using bigframes.

    Args:
        project_id (str): The ID of the BigQuery project.
        dataset_name (str): The name of the dataset.
        table_name (str): The name of the table.
    """
    try:
        # Construct the full table ID
        table_id = f"{project_id}.{dataset_name}.{table_name}"

        # Load the table into a bigframes DataFrame
        bf_df = bpd.read_gbq(table_id)

        print(f"\nData Quality Checks for table: {table_name}")
        print("-" * (len(table_name) + 30))

        # 1. Check for missing data
        print("\nMissing Data:")
        missing_values = bf_df.isnull().sum()
        print(missing_values)

        # 2. Check for duplicate rows
        print("\nDuplicate Rows:")
        duplicate_rows = bf_df.duplicated().sum()
        print(f"Number of duplicate rows: {duplicate_rows}")

        # 3. Check for out-of-range values for dates
        date_columns = [col for col in bf_df.columns if 'date' in col.lower()]  # Identify potential date columns
        if date_columns:
            print("\nOut-of-Range Values for Dates:")
            for date_column in date_columns:
                # Define a reasonable range for dates (adjust as needed)
                min_date = datetime.datetime(1900, 1, 1)
                max_date = datetime.datetime(2100, 1, 1)
                out_of_range_dates = bf_df[(bf_df[date_column] < min_date) | (bf_df[date_column] > max_date)]
                print(f"Column: {date_column}")
                print(f"  Number of out-of-range dates: {len(out_of_range_dates)}")
                print(out_of_range_dates)  # Print the out-of-range values
        else:
            print("\nOut-of-Range Values for Dates: No date columns found to check.")

    except Exception as e:
        print(f"Error processing table {table_name}: {e}")
def main():
    """
    Main function to orchestrate the process of connecting to BigQuery,
    and generating descriptive statistics using bigframes.
    """
    # Initialize BigFrames session
    bpd.options.bigquery.project = project_id # set the project_id

    client = bigquery.Client(project=project_id)

    table_names = ['products', 'sales_transactions', 'stores']
    for table_name in table_names:
        # Get and print the schema
        table_schema = get_table_schema(client, project_id, dataset_name, table_name)
        print_table_schema(table_schema, table_name)
        generate_descriptive_statistics_bigframes(project_id, dataset_name, table_name)
        perform_data_quality_checks(project_id, dataset_name, table_name)

if __name__ == "__main__":
    main()
