-- Script to create the customer_comments table in BigQuery and insert sample data.


-- 1. Create the table
CREATE OR REPLACE TABLE `bq-data-ml-engineering.genai_dataset.customer_comments` (
  timestamp TIMESTAMP OPTIONS(description="Timestamp of the comment submission"),
  comment_id INT64 NOT NULL OPTIONS(description="Unique identifier for the comment"),
  comment STRING OPTIONS(description="Text of the customer comment, up to 2500 characters")
);

-- 2. Insert synthetic data

INSERT INTO `bq-data-ml-engineering.genai_dataset.customer_comments` (timestamp, comment_id, comment)
VALUES
  -- Positive Comments
  (TIMESTAMP("2025-04-10 09:15:00 UTC"), 1, "Wow, the Quantum Leap X1 gadget is amazing! Exceeded all my expectations. Setup was a breeze."),
  (TIMESTAMP("2025-04-10 11:05:30 UTC"), 2, "Absolutely love my new Stellar Smoothie Maker! Makes perfect drinks every time. Highly recommend."),
  (TIMESTAMP("2025-04-11 14:00:15 UTC"), 3, "The Apex Apparel jacket fits perfectly and the quality is top-notch for the price. Very satisfied!"),
  (TIMESTAMP("2025-04-11 16:30:00 UTC"), 4, "My Nova Notebook Slim is incredibly fast and lightweight. Best laptop I've owned."),
  (TIMESTAMP("2025-04-12 08:55:10 UTC"), 5, "Serenity Skincare lotion has done wonders for my skin! So glad I tried it."),
  (TIMESTAMP("2025-04-12 13:20:00 UTC"), 6, "These Fusion Footwear sneakers are super comfortable, even after walking all day."),
  (TIMESTAMP("2025-04-13 10:00:00 UTC"), 7, "The Zenith Appliances smart fridge is a game-changer for our kitchen."),

  -- Negative Comments
  (TIMESTAMP("2025-04-13 11:45:00 UTC"), 8, "Very disappointed with the Quantum Leap X1. It stopped working after just two days. Seeking a refund."),
  (TIMESTAMP("2025-04-14 09:30:50 UTC"), 9, "The Stellar Smoothie Maker leaks whenever I use it. Poor design."),
  (TIMESTAMP("2025-04-14 15:05:00 UTC"), 10, "My Apex Apparel shirt shrunk significantly after the first wash. Not happy."),
  (TIMESTAMP("2025-04-15 10:10:10 UTC"), 11, "The Nova Notebook Slim's battery life is much shorter than advertised. Frustrating."),
  (TIMESTAMP("2025-04-15 12:00:00 UTC"), 12, "Serenity Skincare cream caused an allergic reaction. Be careful if you have sensitive skin."),
  (TIMESTAMP("2025-04-15 17:00:45 UTC"), 13, "The sole came off my Fusion Footwear boots within a week. Terrible quality."),
  (TIMESTAMP("2025-04-16 08:30:00 UTC"), 14, "Received my Echo Electronics speaker but it sounds distorted at high volume."),

  -- Neutral Comments
  (TIMESTAMP("2025-04-16 09:00:00 UTC"), 15, "The Quantum Leap X1 arrived on time as described."),
  (TIMESTAMP("2025-04-16 10:00:00 UTC"), 16, "Using the Stellar Smoothie Maker according to the instructions. It blends things."),
  (TIMESTAMP("2025-04-16 11:00:00 UTC"), 17, "The Apex Apparel sweater is the color shown online."),
  (TIMESTAMP("2025-04-16 12:00:00 UTC"), 18, "My Orbit Office Supplies stapler works as expected."),
  (TIMESTAMP("2025-04-16 12:15:00 UTC"), 19, "Received the Radiant Razor today. Looks like the picture."),
  (TIMESTAMP("2025-04-16 12:30:00 UTC"), 20, "The Zenith Appliances toaster has standard features.");

