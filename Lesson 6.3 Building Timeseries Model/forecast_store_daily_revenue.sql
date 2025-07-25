-- Generate the 3-Day Forecast for each store

SELECT
  *
FROM
  ML.FORECAST(MODEL `bq-data-ml-engineering-459914.timeseries_data.store_revenue_forecast_model`,
              STRUCT(3 AS horizon, 0.95 AS confidence_level)); -- horizon must match training or be less