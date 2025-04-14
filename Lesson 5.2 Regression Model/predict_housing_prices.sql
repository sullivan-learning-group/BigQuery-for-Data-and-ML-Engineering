-- Step 3: Make Predictions using the Linear Regression Model

SELECT
  *,
  predicted_Price  -- The output column with the model's prediction
FROM
  ML.PREDICT(MODEL `bq-data-ml-engineering.ml_dataset.housing_price_model_linear`,
             ( -- Subquery providing the data to predict on
               SELECT
                 -- Ensure all feature columns used during training are present
                 Bedrooms,
                 Bathrooms,
                 SquareFeet,
                 YearBuilt,
                 GarageSpaces,
                 LotSize,
                 CAST(ZipCode AS STRING) AS ZipCode, -- Use the same data type as training
                 CrimeRate,
                 SchoolRating,
                 Price -- Including the original price for comparison (optional)
               FROM
                 `bq-data-ml-engineering.ml_dataset.housing_prices`
               LIMIT 10 -- Example: Predict on the first 10 rows
             )
            );