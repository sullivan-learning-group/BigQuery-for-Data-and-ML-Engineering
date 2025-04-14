-- Step 1: Create a Linear Regression Model to Predict House Price

CREATE OR REPLACE MODEL `bq-data-ml-engineering.ml_dataset.housing_price_model_linear`
OPTIONS(
  model_type = 'LINEAR_REG',          -- Specifies a linear regression model
  input_label_cols = ['Price'],      -- The column we want to predict
  enable_global_explain = TRUE       -- Provides feature importance scores later
) AS
SELECT
  -- Features (Input Columns used for prediction)
  Bedrooms,
  Bathrooms,
  SquareFeet,
  YearBuilt,
  GarageSpaces,
  LotSize,
  CAST(ZipCode AS STRING) AS ZipCode, -- Treat ZipCode as a category (important for linear models)
  CrimeRate,
  SchoolRating,

  -- Label (Target Column to predict)
  Price
FROM
  `bq-data-ml-engineering.ml_dataset.housing_prices`; -- Your source table