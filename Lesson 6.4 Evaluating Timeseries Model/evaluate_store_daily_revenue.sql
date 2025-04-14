-- Step 2: Evaluate the Model Performance

SELECT
  *
FROM
  ML.EVALUATE(MODEL `bq-data-ml-engineering.timeseries_dataset.store_revenue_forecast_model`);