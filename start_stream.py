from pyspark.sql import SparkSession
from config import RUNTIME, SERVER_PORT, SOURCE_TOPIC, DATADIR
import time

spark = SparkSession \
        .builder \
        .master('local[6]') \
        .appName('Stream w/ Pyspark') \
        .getOrCreate()

df = spark.read.parquet(DATADIR + 'parquet/stream_df.parquet')
df.createOrReplaceTempView('stream')

start_time = time.time()
last_run = 0

while last_run <= RUNTIME:
    now = time.time() - start_time
    result = spark.sql(f'SELECT * FROM stream WHERE Stream_Time BETWEEN {last_run} AND {now} ORDER BY Stream_Time ASC')
    last_run = now

    if not result.isEmpty():
        result \
                .selectExpr("CAST(ID AS STRING)", "to_json(struct(*)) AS value") \
                .write \
                .format('kafka') \
                .option('kafka.bootstrap.servers',SERVER_PORT) \
                .option('topic',SOURCE_TOPIC) \
                .save()
