-- Step 2: Evaluate the Model Performance

SELECT
  *
FROM
  ML.EVALUATE(MODEL `bq-data-ml-engineering-459914.timeseries_data.store_revenue_forecast_model`);