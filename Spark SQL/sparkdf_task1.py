from pyspark import SparkContext, SparkConf
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql import Window

config = (
    SparkConf()
        .setAppName("chernova_neg_reviews")
        .setMaster("yarn")
)

sc = SparkContext(conf=config)

OUT_PATH=sys.argv[1]

review = (spark.read.json("/data/yelp/review"))
review = review.select("business_id", "stars")
review_filtered = review.filter(review.stars < 3)
review_filtered = review_filtered.groupby('business_id').count().distinct()
review_filtered = review_filtered.select("business_id", f.col('count').alias('negative_cnt'))

business = (spark.read.json("/data/yelp/business"))
business_filtered = business.select('business_id', 'city')
spark.sql("SET spark.sql.autoBroadcastJoinThreshold = 10")
business_review = review_filtered.join(f.broadcast(business_filtered.drop('stars')), on='business_id', how='inner')
business_review.createOrReplaceTempView("bus_review")
sqlDF = spark.sql("SELECT business_id, city, negative_cnt FROM (SELECT business_id, city, negative_cnt, rank() OVER (PARTITION BY city ORDER BY negative_cnt DESC) as rev_rank FROM bus_review) WHERE rev_rank <= 10")

sqlDF.write.csv("business_review_counts.tsv", sep='\t', mode='overwrite')
