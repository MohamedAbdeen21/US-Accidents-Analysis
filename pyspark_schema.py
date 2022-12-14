from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ByteType, BooleanType, TimestampType, DoubleType 

schema = StructType([
    StructField('Event',StringType() , False),
    StructField('Stream_Time', DoubleType() , False),
    StructField('ID', IntegerType() , False),
    StructField('Severity',ByteType() , False),
    StructField('Start_Time',TimestampType() , False),
    StructField('End_Time',TimestampType() , False),
    StructField('Start_Lat',DoubleType() , False),
    StructField('Start_Lng',DoubleType() , False),
    StructField('End_Lat',DoubleType() , False),
    StructField('End_Lng',DoubleType() , False),
    StructField('Distance_mi',DoubleType() , False),
    StructField('Description',StringType() , False),
    StructField('Number',StringType() , False),
    StructField('Street',StringType() , False),
    StructField('Side',StringType() , False),
    StructField('City',StringType() , False),
    StructField('County',StringType() , False),
    StructField('State',StringType() , False),
    StructField('Zipcode',StringType() , False),
    StructField('Country',StringType() , False),
    StructField('Timezone',StringType() , False),
    StructField('Airport_Code',StringType() , False),
    StructField('Weather_Timestamp',TimestampType() , False),
    StructField('Temperature_F',DoubleType() , False),
    StructField('Wind_Chill_F',DoubleType() , False),
    StructField('Humidity_Percent',DoubleType() , False),
    StructField('Pressure_in',DoubleType() , False),
    StructField('Visibility_mi',DoubleType() , False),
    StructField('Wind_Direction',StringType() , False),
    StructField('Wind_Speed_mph',DoubleType() , False),
    StructField('Precipitation_in',DoubleType() , False),
    StructField('Weather_Condition',StringType() , False),
    StructField('Amenity',BooleanType() , False),
    StructField('Bump',BooleanType() , False),
    StructField('Crossing',BooleanType() , False),
    StructField('Give_Way',BooleanType() , False),
    StructField('Junction',BooleanType() , False),
    StructField('No_Exit',BooleanType() , False),
    StructField('Railway',BooleanType() , False),
    StructField('Roundabout',BooleanType() , False),
    StructField('Station',BooleanType() , False),
    StructField('Stop',BooleanType() , False),
    StructField('Traffic_Calming',BooleanType() , False),
    StructField('Traffic_Signal',BooleanType() , False),
    StructField('Turning_Loop',BooleanType() , False),
    StructField('Sunrise_Sunset',StringType() , False),
    StructField('Civil_Twilight',StringType() , False),
    StructField('Nautical_Twilight',StringType() , False),
    StructField('Astronomical_Twilight',StringType(), False)
    ])
