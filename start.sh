#!/bin/bash

# kafka
gnome-terminal -- bash ~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties 

# Sometimes kafka server needs to wait a bit for zookeeper before starting
sleep 20

gnome-terminal -- bash ~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties

# postgres and pgadmin
gnome-terminal -- /bin/sh -c 'cat ~/US_Accidents_Analysis/Data/password.txt | sudo -S -k systemctl start docker && cd ~/postgres && cat ~/US_Accidents_Analysis/Data/password.txt | sudo -S -k docker compose up'

# superset
gnome-terminal -- /bin/sh -c 'superset run -p 8088 --with-threads --reload --debugger'

sleep 3
# start-postgres-pipeline
gnome-terminal -- /bin/sh -c "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 --driver-class-path ~/US_Accidents_Analysis/Data/postgresql-42.4.1.jar ~/US_Accidents_Analysis/kafka_to_DW.py"

# start-stream
gnome-terminal -- /bin/sh -c "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 ~/US_Accidents_Analysis/start_stream.py"
