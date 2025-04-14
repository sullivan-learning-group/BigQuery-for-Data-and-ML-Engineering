

export PROJECT_ID="bq-data-ml-engineering"  # Replace with your project ID
export DATASET_NAME="retail_dataset" # Replace with your dataset name
export TEMP_BUCKET="slg-bq-data-ml-engineering"      # Replace with your bucket
gcloud config set project $PROJECT_ID


gcloud dataproc clusters create slg-bq-data-ml-dataproc \
    --region us-central1 \
    --master-machine-type n1-standard-2 \
    --worker-machine-type n1-standard-2 \
    --num-workers 2 

export CLUSTER_NAME="cluster-a159"
gcloud compute ssh $CLUSTER_NAME-m --zone us-central1-a

export PROJECT_ID="bq-data-ml-engineering"  # Replace with your project ID
export DATASET_NAME="retail_dataset" # Replace with your dataset name
export TEMP_BUCKET="slg-bq-data-ml-engineering"      # Replace with your bucket


--- To query using SQL in Spark
spark-sql --packages=com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0 \
    --conf spark.sql.dialect=standard \
    --conf spark.hadoop.fs.gs.project.id=$PROJECT_ID \
    --conf spark.hadoop.fs.gs.system.bucket=$TEMP_BUCKET \
    --conf spark.hadoop.fs.gs.working.dir=/

CREATE TEMPORARY VIEW products_bq
USING bigquery
OPTIONS (
  table = 'bq-data-ml-engineering.retail_dataset.products'
);

SELECT * FROM products_bq LIMIT 10;

---- To query using PySpark
# Assume you have already started your PySpark session (SparkSession object named 'spark')

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