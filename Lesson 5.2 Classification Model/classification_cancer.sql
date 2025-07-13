-- 1. EXPLORE THE DATA
-- Check the structure and distribution of the dataset
SELECT
  diagnosis,
  COUNT(*) AS count
FROM
  `bq-data-ml-engineering-459914.ml_dataset.synthetic_cancer`
GROUP BY
  diagnosis;

-- Check for missing values
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(diagnosis IS NULL) AS missing_diagnosis,
  COUNTIF(radius_mean IS NULL) AS missing_radius_mean
  -- Add more columns as needed
FROM
  `bq-data-ml-engineering-459914.ml_dataset.synthetic_cancer`;

-- Check feature distributions by diagnosis
SELECT
  diagnosis,
  AVG(radius_mean) AS avg_radius_mean,
  AVG(texture_mean) AS avg_texture_mean,
  AVG(perimeter_mean) AS avg_perimeter_mean,
  AVG(area_mean) AS avg_area_mean,
  AVG(smoothness_mean) AS avg_smoothness_mean
FROM
  `bq-data-ml-engineering-459914.ml_dataset.synthetic_cancer`
GROUP BY
  diagnosis;

-- 2. DATA PREPARATION
-- Create a training and evaluation set (80% training, 20% evaluation)
CREATE OR REPLACE TABLE
  `bq-data-ml-engineering-459914.ml_dataset.cancer_train` AS
SELECT
  *,
  IF(diagnosis = 'M', 1, 0) AS label
FROM
  `bq-data-ml-engineering-459914.ml_dataset.synthetic_cancer`
WHERE
  RAND() < 0.8;

CREATE OR REPLACE TABLE
  `bq-data-ml-engineering-459914.ml_dataset.cancer_eval` AS
SELECT
  *,
  IF(diagnosis = 'M', 1, 0) AS label
FROM
  `bq-data-ml-engineering-459914.ml_dataset.synthetic_cancer`
WHERE
  id NOT IN (
  SELECT
    id
  FROM
    `bq-data-ml-engineering-459914.ml_dataset.cancer_train`);

-- 3. CREATE LOGISTIC REGRESSION MODEL
-- Standard logistic regression
CREATE OR REPLACE MODEL
  `bq-data-ml-engineering-459914.ml_dataset.cancer_logistic_model`
OPTIONS
  (model_type='LOGISTIC_REG',
   input_label_cols=['label'],
   data_split_method='NO_SPLIT',
   l1_reg=0.01,
   l2_reg=0.01) AS
SELECT
  -- Exclude id and diagnosis (using label instead), and any empty columns
  label,
  radius_mean,
  texture_mean,
  perimeter_mean,
  area_mean,
  smoothness_mean,
  compactness_mean,
  concavity_mean,
  `concave points_mean` AS concave_points_mean,
  symmetry_mean,
  fractal_dimension_mean,
  radius_se,
  texture_se,
  perimeter_se,
  area_se,
  smoothness_se,
  compactness_se,
  concavity_se,
  `concave points_se` AS concave_points_se,
  symmetry_se,
  fractal_dimension_se,
  radius_worst,
  texture_worst,
  perimeter_worst,
  area_worst,
  smoothness_worst,
  compactness_worst,
  concavity_worst,
  `concave points_worst` AS concave_points_worst,
  symmetry_worst,
  fractal_dimension_worst
FROM
  `bq-data-ml-engineering.ml_dataset.cancer_train`;


-- 4. MODEL EVALUATION
-- Evaluate logistic regression model
SELECT
  *
FROM
  ML.EVALUATE(MODEL `bq-data-ml-engineering-459914.ml_dataset.cancer_logistic_model`,
    (
    SELECT
      label,
      radius_mean,
      texture_mean,
      perimeter_mean,
      area_mean,
      smoothness_mean,
      compactness_mean,
      concavity_mean,
      `concave points_mean` AS concave_points_mean,
      symmetry_mean,
      fractal_dimension_mean,
      radius_se,
      texture_se,
      perimeter_se,
      area_se,
      smoothness_se,
      compactness_se,
      concavity_se,
      `concave points_se` AS concave_points_se,
      symmetry_se,
      fractal_dimension_se,
      radius_worst,
      texture_worst,
      perimeter_worst,
      area_worst,
      smoothness_worst,
      compactness_worst,
      concavity_worst,
      `concave points_worst` AS concave_points_worst,
      symmetry_worst,
      fractal_dimension_worst
    FROM
      `bq-data-ml-engineering-459914.ml_dataset.cancer_eval`));


-- 5. GET FEATURE IMPORTANCE (for interpretability)
-- For Logistic Regression
SELECT
  *
FROM
  ML.WEIGHTS(MODEL `bq-data-ml-engineering-459914.ml_dataset.cancer_logistic_model`,
    STRUCT(true AS standardize));


-- 6. USE THE MODEL FOR PREDICTION
-- Make predictions with the best model (after evaluation)
SELECT
  *
FROM
  ML.PREDICT(MODEL `bq-data-ml-engineering-459914.ml_dataset.cancer_logistic_model`,
    (
      -- This subquery selects the data you want to make predictions on.
      -- ML.PREDICT is smart and will pass through non-feature columns like id and diagnosis.
      SELECT
        id,
        diagnosis,
        radius_mean,
        texture_mean,
        perimeter_mean,
        area_mean,
        smoothness_mean,
        compactness_mean,
        concavity_mean,
        `concave points_mean` AS concave_points_mean,
        symmetry_mean,
        fractal_dimension_mean,
        radius_se,
        texture_se,
        perimeter_se,
        area_se,
        smoothness_se,
        compactness_se,
        concavity_se,
        `concave points_se` AS concave_points_se,
        symmetry_se,
        fractal_dimension_se,
        radius_worst,
        texture_worst,
        perimeter_worst,
        area_worst,
        smoothness_worst,
        compactness_worst,
        concavity_worst,
        `concave points_worst` AS concave_points_worst,
        symmetry_worst,
        fractal_dimension_worst
      FROM
        `bq-data-ml-engineering-459914.ml_dataset.cancer_eval`
    )
  )
LIMIT 10;

-- 7. CONFUSION MATRIX
-- Get confusion matrix to understand model performance better
SELECT
  *
FROM
  ML.CONFUSION_MATRIX(MODEL `bq-data-ml-engineering-459914.ml_dataset.cancer_logistic_model`,
    (
    SELECT
      label,
      radius_mean,
      texture_mean,
      perimeter_mean,
      area_mean,
      smoothness_mean,
      compactness_mean,
      concavity_mean,
      `concave points_mean` AS concave_points_mean,
      symmetry_mean,
      fractal_dimension_mean,
      radius_se,
      texture_se,
      perimeter_se,
      area_se,
      smoothness_se,
      compactness_se,
      concavity_se,
      `concave points_se` AS concave_points_se,
      symmetry_se,
      fractal_dimension_se,
      radius_worst,
      texture_worst,
      perimeter_worst,
      area_worst,
      smoothness_worst,
      compactness_worst,
      concavity_worst,
      `concave points_worst` AS concave_points_worst,
      symmetry_worst,
      fractal_dimension_worst
    FROM
      `bq-data-ml-engineering-459914.ml_dataset.cancer_eval`));

-- 8. ROC CURVE ANALYSIS
-- Generate ROC curve points for model comparison
SELECT
  *
FROM
  ML.ROC_CURVE(MODEL `bq-data-ml-engineering-459914.ml_dataset.cancer_logistic_model`,
    (
    SELECT
      label,
      radius_mean,
      texture_mean,
      perimeter_mean,
      area_mean,
      smoothness_mean,
      compactness_mean,
      concavity_mean,
      `concave points_mean` AS concave_points_mean,
      symmetry_mean,
      fractal_dimension_mean,
      radius_se,
      texture_se,
      perimeter_se,
      area_se,
      smoothness_se,
      compactness_se,
      concavity_se,
      `concave points_se` AS concave_points_se,
      symmetry_se,
      fractal_dimension_se,
      radius_worst,
      texture_worst,
      perimeter_worst,
      area_worst,
      smoothness_worst,
      compactness_worst,
      concavity_worst,
      `concave points_worst` AS concave_points_worst,
      symmetry_worst,
      fractal_dimension_worst
    FROM
      `bq-data-ml-engineering-459914.ml_dataset.cancer_eval`));