from diagrams import Diagram, Cluster, Edge
from diagrams.custom import Custom
from diagrams.onprem.analytics import Spark, Superset
from diagrams.onprem.queue import Kafka
from diagrams.onprem.database import Postgresql
from diagrams.programming.flowchart import Database

with Diagram(filename='diagram'):

    with Cluster("Transform and Stream"):
        spark = Spark()
        csv = Custom('','./csv.png')
        parquet = Custom('','./parquet.png')

    with Cluster('ETL', direction='TB', graph_attr={"labeljust":"R"}):
        with Cluster('Extract'):
            kafka = Kafka()
            sparkss = Custom(label='',icon_path='./sparkstreaming.png')
        with Cluster('Transform'):
            sparkss2 = Custom(label='',icon_path='./sparkstreaming.png')
        with Cluster('Load', direction="TB"):
            pg = Postgresql('Warehouse')

    with Cluster('Visualization'):
        superset = Superset()  

    csv >> spark >> parquet
    parquet >> sparkss >> kafka
    kafka >> sparkss2 >> pg
    pg >> superset
