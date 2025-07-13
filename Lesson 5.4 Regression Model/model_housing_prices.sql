-- Step 1: Create a Linear Regression Model to Predict House Price


CREATE OR REPLACE MODEL `bq-data-ml-engineering-459914.ml_dataset.housing_price_model_linear`
OPTIONS(
  model_type = 'LINEAR_REG',         
  input_label_cols = ['Price'],     
  enable_global_explain = TRUE       
) AS
SELECT
  Bedrooms,
  Bathrooms,
  SquareFeet,
  YearBuilt,
  GarageSpaces,
  LotSize,
  CAST(ZipCode AS STRING) AS ZipCode, 
  CrimeRate,
  SchoolRating,
  Price
FROM
  `bq-data-ml-engineering-459914.ml_dataset.housing_prices`;