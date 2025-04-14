-- Step 1: Create the Time Series Forecasting Model

CREATE OR REPLACE MODEL `bq-data-ml-engineering.timeseries_dataset.store_revenue_forecast_model`
OPTIONS(
  model_type = 'ARIMA_PLUS',              -- Specifies the ARIMA+ model for time series
  time_series_timestamp_col = 'date',    -- Column representing the date/time
  time_series_data_col = 'revenue',      -- Column with the value to forecast
  time_series_id_col = 'store_id',       -- Column identifying each unique time series
  horizon = 3,                           -- Number of time steps to forecast (3 days)
  auto_arima = TRUE,                     -- Automatically find best ARIMA order (p,d,q)
  data_frequency = 'DAILY',              -- Frequency of the time series data
  holiday_region = 'US',                 -- Incorporate US holidays automatically
  decompose_time_series = TRUE           -- Decompose into trend, seasonality, residual
  -- You could add 'feature_cols = ['is_promo', 'is_holiday']' if you know these future values
  -- and want to explicitly use them as regressors. Requires knowing values for the forecast period.
) AS
SELECT
  date,
  store_id,
  revenue
FROM
  `bq-data-ml-engineering.timeseries_dataset.store_daily_revenue`; -- Your source table