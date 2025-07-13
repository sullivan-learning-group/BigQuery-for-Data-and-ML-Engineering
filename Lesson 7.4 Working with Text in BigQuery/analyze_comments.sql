-- Step 1: Create the Remote Model for Text Generation
-- This model connects to a generative AI service (e.g., Gemini) via the specified connection.
-- Replace 'gemini-1.0-pro' if you want to use a different generative model available through Vertex AI.
CREATE OR REPLACE MODEL `bq-data-ml-engineering.genai_dataset.comment_sentiment_model`
  REMOTE WITH CONNECTION `bq-data-ml-engineering.us.bigquery_vertex_ai`
OPTIONS (REMOTE_SERVICE_TYPE = 'CLOUD_AI_NATURAL_LANGUAGE_V1');

-- Step 2: Perform Sentiment Analysis using ML.GENERATE_TEXT
-- This query uses the generative model to analyze the sentiment of each comment based on a prompt.
CREATE OR REPLACE TABLE `bq-data-ml-engineering.genai_dataset.comment_analysis` AS
SELECT
  comment_id,
  timestamp,
  text_content,
  ml_understand_text_result.document_sentiment.score AS sentiment_score,
  ml_understand_text_result.document_sentiment.magnitude AS sentiment_magnitude,
  ml_understand_text_status AS status
FROM ML.UNDERSTAND_TEXT(
  MODEL `bq-data-ml-engineering.genai_dataset.comment_sentiment_model`,
  (
    SELECT 
      comment_id,
      timestamp,
      `comment` AS text_content  
    FROM `bq-data-ml-engineering.genai_dataset.customer_comments`
  ),
  STRUCT('analyze_sentiment' AS nlu_option)
);

