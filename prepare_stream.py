from pyspark.sql.functions import timestamp_seconds, unix_timestamp, split, lit, year, to_timestamp, when
from pyspark.sql.types import FloatType, LongType, TimestampType, ByteType, IntegerType, DoubleType, BooleanType
from pyspark.sql import SparkSession
from config import *
from os import rename, listdir

spark = SparkSession \
        .builder \
        .master('local[6]') \
        .appName('Transform to prepare stream w/ Pyspark') \
        .config('spark.local.dir', DATADIR + 'temp/') \
        .getOrCreate()

main_df = spark.read.csv(DATADIR + 'US_Accidents_Dec21_updated.csv', header = True)
main_df = main_df.withColumn('Start_Time_Unix', unix_timestamp(split(main_df['Start_Time'], '\.').getItem(0)))
main_df = main_df.withColumn('End_Time_Unix', unix_timestamp(split(main_df['End_Time'], '\.').getItem(0)))

bool_columns = ['Amenity','Bump','Crossing','Give_Way','Junction','No_Exit','Railway','Roundabout'
                ,'Station','Stop','Traffic_Calming','Traffic_Signal','Turning_Loop']

for column in bool_columns:
    main_df = main_df.withColumn(column, main_df[column].cast(BooleanType()))

float_columns = ['Precipitation(in)','Wind_Speed(mph)' , 'Temperature(F)', 'Wind_Chill(F)'
                , 'Pressure(in)','Humidity(%)' , 'Visibility(mi)']

float_columns_renamed = ['Precipitation_in','Wind_Speed_mph' , 'Temperature_F', 'Wind_Chill_F'
                , 'Pressure_in','Humidity_Percent' , 'Visibility_mi']

main_df = main_df.fillna('-1', subset = float_columns + ['Number', 'Zipcode'])

for column,renamed in zip(float_columns, float_columns_renamed):
    main_df = main_df.withColumn(renamed, main_df[column].cast(FloatType()))
    
string_nulls = ['Wind_Direction','Weather_Condition','Airport_Code', 'City', 'Street' 
               ,'Astronomical_Twilight', 'Nautical_Twilight', 'Civil_Twilight', 'Sunrise_Sunset']

main_df = main_df.fillna('Not Available', subset = string_nulls)
main_df = main_df.fillna('2022-01-01 00:00:00', subset = 'Weather_Timestamp')

main_df = main_df.drop(*float_columns)

main_df = (main_df.withColumn('ID', split(main_df['ID'],'-').getItem(1).cast(IntegerType()))
                .withColumn('Severity', main_df['Severity'].cast(ByteType())) 
                .withColumn('Start_Time', main_df['Start_Time'].cast(TimestampType())) 
                .withColumn('End_Time', main_df['End_Time'].cast(TimestampType())) 
                .withColumn('Start_Lat', main_df['Start_Lat'].cast(DoubleType())) 
                .withColumn('End_Lat', main_df['End_Lat'].cast(DoubleType())) 
                .withColumn('Start_Lng', main_df['Start_Lng'].cast(DoubleType()))  
                .withColumn('End_Lng', main_df['End_Lng'].cast(DoubleType()))  
                .withColumn('Distance_mi', main_df['Distance(mi)'].cast(DoubleType()))
                .drop('Distance(mi)')
                .withColumn('Number', main_df['Number'].cast(IntegerType()))
                .withColumn('Weather_Timestamp', main_df['Weather_Timestamp'].cast(TimestampType()))
            )

temp_df = main_df.select(lit('Start'),'ID','Start_Time_Unix') \
                    .union(main_df.select(lit('End'),'ID','End_Time_Unix')) \
                    .withColumnRenamed('Start','Event') \
                    .withColumnRenamed('Start_Time_Unix','Time_Unix')

earliest = temp_df.agg({'Time_Unix':'min'}).collect()[0][0]
latest = temp_df.agg({'Time_Unix':'max'}).collect()[0][0]

temp_df = temp_df.withColumn('Stream_Time',
                        (temp_df['Time_Unix'].cast(FloatType())- earliest) * RUNTIME / (latest - earliest))

temp_df = temp_df.withColumnRenamed('ID','temp_id')
to_delete = ('Start_Time_Unix','End_Time_Unix','Time_Unix','temp_id')

stream_df = temp_df.join(main_df, temp_df.temp_id == main_df.ID) \
                    .drop(*to_delete) \
                    .orderBy('Stream_Time')

stream_df = stream_df.repartition(50)

target_folder = DATADIR + 'parquet/'
stream_df.coalesce(1).write.parquet(target_folder)

filename = listdir(target_folder)[0]
rename(target_folder + filename, target_folder + 'stream_df.parquet')
