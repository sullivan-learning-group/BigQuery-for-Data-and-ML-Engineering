-- Step 2: Evaluate the Linear Regression Model

SELECT
  *
FROM
  ML.EVALUATE(MODEL `bq-data-ml-engineering-459914.ml_dataset.housing_price_model_linear`);