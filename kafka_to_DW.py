from config import RUNTIME
from pyspark_schema import schema
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType
from config import SOURCE_TOPIC, SERVER_PORT
from pyspark.sql.functions import date_format, from_json, col, monotonically_increasing_id, broadcast, to_timestamp, lit, date_format

locationDimensionCols = ['Number','Street','Side','City','County','State','Zipcode','Country','Timezone']
weatherDimensionCols = ['Wind_Direction','Weather_Condition','Airport_Code']
environmentDimensionCols = ['Amenity','Bump','Crossing','Give_Way','Junction','No_Exit','Railway','Roundabout','Station','Stop','Traffic_Calming','Traffic_Signal','Turning_Loop']
TimeOfDayDimensionCols = ['Sunrise_Sunset','Civil_Twilight','Nautical_Twilight','Astronomical_Twilight']

spark = SparkSession \
        .builder \
        .master('local[6]') \
        .appName('Kafka to PostgreSQL') \
        .getOrCreate()

df = spark \
        .readStream \
        .format('kafka') \
        .option("kafka.bootstrap.servers", SERVER_PORT) \
        .option("subscribe", SOURCE_TOPIC) \
        .option("startingOffsets", "latest") \
        .option('failOnDataLoss','false') \
        .load() \
        .select('value')

spark.sparkContext.setLogLevel('ERROR')

df = df.withColumn('json', df.value.cast(StringType())) \
        .withColumn('jsonData', from_json(col('json'), schema)) \
        .drop('json','value')

new_df = df.select('jsonData.*')

def send(df, table):
    df.write.format('jdbc') \
            .option("url", "jdbc:postgresql://localhost:5432/stream_df") \
            .option("dbtable", table) \
            .option("user", "root") \
            .option("password", "root") \
            .option("driver", "org.postgresql.Driver") \
            .mode('append') \
            .save()
    pass

def receive(table):
    return spark.read.format('jdbc') \
            .option("url", "jdbc:postgresql://localhost:5432/stream_df") \
            .option("dbtable", table) \
            .option("user", "root") \
            .option("password", "root") \
            .option("driver", "org.postgresql.Driver") \
            .load()

def load(df, batchId):

    locationDimensionOld = receive('LocationDimension')
    weatherDimensionOld = receive('WeatherDimension')
    environmentDimensionOld = receive('EnvironmentDimension')
    timeOfDayDimensionOld = receive('TimeOfDayDimension')

    # facts = df.drop(*(locationDimensionCols + weatherDimensionCols + TimeOfDayDimensionCols + environmentDimensionCols)) 
    locationDf = df.select(*locationDimensionCols).withColumn('ID',monotonically_increasing_id() + 1 + (locationDimensionOld.agg({"ID":"max"}).collect()[0][0] or 0))
    weatherDf = df.select(*weatherDimensionCols).withColumn('ID',monotonically_increasing_id() + 1 + (weatherDimensionOld.agg({"ID":"max"}).collect()[0][0] or 0))
    environmentDf = df.select(*environmentDimensionCols).withColumn('ID',monotonically_increasing_id() + 1 + (environmentDimensionOld.agg({"ID":"max"}).collect()[0][0] or 0))
    timeOfDayDf = df.select(*TimeOfDayDimensionCols).withColumn('ID',monotonically_increasing_id() + 1 + (timeOfDayDimensionOld.agg({"ID":"max"}).collect()[0][0] or 0))

    # The upserted dimensions
    locationDimension = locationDimensionOld.union(locationDf).dropDuplicates(locationDimensionCols)
    weatherDimension = weatherDimensionOld.union(weatherDf).dropDuplicates(weatherDimensionCols)
    environmentDimension = environmentDimensionOld.union(environmentDf).dropDuplicates(environmentDimensionCols)
    timeOfDayDimension = timeOfDayDimensionOld.union(timeOfDayDf).dropDuplicates(TimeOfDayDimensionCols)

    # Only the new columns
    locationDf = locationDimension.subtract(locationDimensionOld)
    weatherDf = weatherDimension.subtract(weatherDimensionOld)
    environmentDf = environmentDimension.subtract(environmentDimensionOld)
    timeOfDayDf = timeOfDayDimension.subtract(timeOfDayDimensionOld)

    send(locationDf,'LocationDimension')
    send(weatherDf,'WeatherDimension')
    send(environmentDf, 'EnvironmentDimension')
    send(timeOfDayDf, 'TimeOfDayDimension')

    facts = df.join(broadcast(locationDimension.withColumnRenamed('ID','locationID')),locationDimensionCols).drop(*locationDimensionCols)\
                .join(broadcast(weatherDimension.withColumnRenamed('ID','weatherID')),weatherDimensionCols).drop(*weatherDimensionCols) \
                .join(broadcast(environmentDimension.withColumnRenamed('ID','environmentID')),environmentDimensionCols).drop(*environmentDimensionCols) \
                .join(broadcast(timeOfDayDimension.withColumnRenamed('ID','TimeOfDayID')),TimeOfDayDimensionCols).drop(*TimeOfDayDimensionCols) \
                .withColumn('Start_Date', date_format('Start_Time',"yyyyMMdd").cast(IntegerType())) \
                .withColumn('End_Date', date_format('End_Time',"yyyyMMdd").cast(IntegerType())) \
                .withColumn('Weather_Date', date_format('Weather_Timestamp',"yyyyMMdd").cast(IntegerType()))

    send(facts.select('*').filter("Event = 'Start'").drop('Event') \
                        .withColumn('End_Time',to_timestamp(lit('1970-01-01 00:00:00'),'yyyy-MM-dd HH:mm:ss')) \
                        .withColumn('End_Date',date_format('End_Time',"yyyyMMdd").cast(IntegerType())),'AccidentsFacts')

    send(facts.select('*').filter("Event = 'End'").drop('Event'),'StagingFacts')
    pass

REFRESH_RATE = RUNTIME / (6 * 12 * 4) # weekly

query = new_df \
        .writeStream \
        .outputMode('append') \
        .trigger(processingTime = '20 seconds') \
        .foreachBatch(load) \
        .start().awaitTermination()
