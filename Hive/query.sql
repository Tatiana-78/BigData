ADD JAR /usr/lib/hive/lib/hive-hcatalog-core-3.1.2.jar;
USE ${hiveconf:db};

DROP TABLE IF EXISTS bus_reviews;
CREATE EXTERNAL TABLE bus_reviews
(
    business_id STRING,
    name STRING,
    city STRING,
    stars FLOAT,
    review_count INT
    )
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/data/yelp/business';


DROP TABLE IF EXISTS review34;
CREATE EXTERNAL TABLE review34
(
    review_id STRING,
    business_id STRING,
    stars FLOAT
    )
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/data/yelp/review';


WITH neg_reviews AS (
      SELECT business_id, count(*) as cnt
      FROM review34
      WHERE stars < 3.0
      GROUP BY business_id
      )
SELECT business_id, city, cnt
FROM (
  SELECT neg_reviews.business_id, bus_reviews.city, neg_reviews.cnt,  
    row_number() OVER (PARTITION BY bus_reviews.city ORDER BY neg_reviews.cnt DESC) as top10
  FROM neg_reviews JOIN bus_reviews ON bus_reviews.business_id = neg_reviews.business_id) negative
WHERE top10 <= 10;
