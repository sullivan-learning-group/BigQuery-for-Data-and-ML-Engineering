

export PROJECT_ID="bq-data-ml-engineering"  # Replace with your project ID
export DATASET_NAME="retail_dataset" # Replace with your dataset name
export TEMP_BUCKET="slg-bq-data-ml-engineering"      # Replace with your bucket
gcloud config set project $PROJECT_ID


gcloud dataproc clusters create slg-bq-data-ml-dataproc \
    --region us-central1 \
    --master-machine-type n1-standard-2 \
    --worker-machine-type n1-standard-2 \
    --num-workers 2 

# note, you may need to change the zone to the one where your cluster was created
export CLUSTER_NAME="slg-bq-data-ml-dataproc"
gcloud compute ssh $CLUSTER_NAME-m --zone us-central1-f

export PROJECT_ID="bq-data-ml-engineering"  # Replace with your project ID
export DATASET_NAME="retail_dataset" # Replace with your dataset name
export TEMP_BUCKET="slg-bq-data-ml-engineering"      # Replace with your bucket

export PROJECT_ID="bq-data-ml-engineering"  
export DATASET_NAME="retail_dataset" 
export TEMP_BUCKET="slg-bq-data-ml-engineering"      

---- To query using PySpark

Start a PySpark sessions with the command:
> pyspark
# After you have started your PySpark session, execute the following commands
# within pyspark

project_id = "bq-data-ml-engineering"
dataset_name = "retail_dataset"
table_name = "products"
bigquery_table = f"{project_id}.{dataset_name}.{table_name}"

# Load the BigQuery table into a PySpark DataFrame
products_df = spark.read.format("bigquery").option("table", bigquery_table).load()

# Now 'products_df' is a DataFrame containing the data from your BigQuery table.
# You can perform further operations on this DataFrame, such as:

# Display the schema
products_df.printSchema()

# Show the first 10 rows
products_df.show(10)

# Run a SQL query against the DataFrame (you need to register it as a temporary view first)
products_df.createOrReplaceTempView("products_view")
query_results_df = spark.sql("SELECT product_id, product_name FROM products_view WHERE category = 'Electronics'")
query_results_df.show()