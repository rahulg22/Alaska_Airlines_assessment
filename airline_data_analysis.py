import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.types import *
from pyspark.sql import functions as F
import getpass
username = getpass.getuser()
spark = SparkSession.builder.appName(f'{username} | example code').master("yarn").getOrCreate()
airports = 'hdfs://m01.itversity.com:9000/user/itv001283/Airports.csv'

airports_df = spark. \
    read. \
    format('csv'). \
    option('sep', ','). \
    option("header", "true"). \
    schema('''Origin_airport STRING,
              Destination_airport STRING,
              Origin_city STRING,
              Destination_city STRING,
              Passengers INT,
              Seats INT,
              Flights INT,
              Distance INT,
              Fly_date STRING,
              Origin_population INT,
              Destination_population INT,
              Org_airport_lat STRING,
              Org_airport_long STRING,
              Dest_airport_lat STRING,
              Dest_airport_long STRING
            '''). \
    load(airports)
airports_df.select("Origin_airport","Destination_airport","Passengers","Seats").show(15)

+--------------+-------------------+----------+-----+
|Origin_airport|Destination_airport|Passengers|Seats|
+--------------+-------------------+----------+-----+
|           MHK|                AMW|        21|   30|
|           EUG|                RDM|        41|  396|
|           EUG|                RDM|        88|  342|
|           EUG|                RDM|        11|   72|
|           MFR|                RDM|         0|   18|
|           MFR|                RDM|        11|   18|
|           MFR|                RDM|         2|   72|
|           MFR|                RDM|         7|   18|
|           MFR|                RDM|         7|   36|
|           SEA|                RDM|         8|   18|
|           SEA|                RDM|       453| 3128|
|           SEA|                RDM|       784| 2720|
|           SEA|                RDM|       749| 2992|
|           SEA|                RDM|        11|   18|
|           PDX|                RDM|       349|  851|
+--------------+-------------------+----------+-----+
only showing top 15 rows


# Aggregating the Data : Passengers visit each individual airports
origin_Psngr = airports_df.groupBy("Origin_airport",   "Origin_city").agg(F.sum("Passengers").alias('Passengers_Travelling')).orderBy(desc('Passengers_Travelling'))
origin_Psngr.show()
+--------------+------------------+---------------------+
|Origin_airport|       Origin_city|Passengers_Travelling|
+--------------+------------------+---------------------+
|           ATL|       Atlanta, GA|            577124268|
|           ORD|       Chicago, IL|            529018110|
|           DFW|        Dallas, TX|            457153720|
|           LAX|   Los Angeles, CA|            393005676|
|           PHX|       Phoenix, AZ|            295857703|
|           LAS|     Las Vegas, NV|            270590248|
|           DTW|       Detroit, MI|            250983023|
|           MSP|   Minneapolis, MN|            245197238|
|           SFO| San Francisco, CA|            243779917|
|           IAH|       Houston, TX|            228367851|
|           MCO|       Orlando, FL|            225173860|
|           SEA|       Seattle, WA|            212354281|
|           EWR|        Newark, NJ|            209126657|
|           STL|     St. Louis, MO|            204489078|
|           CLT|     Charlotte, NC|            202809528|
|           LGA|      New York, NY|            198520952|
|           BOS|        Boston, MA|            189822544|
|           PHL|  Philadelphia, PA|            185946976|
|           SLC|Salt Lake City, UT|            152044294|
|           DCA|    Washington, DC|            145055210|
+--------------+------------------+---------------------+
only showing top 20 rows

# Aggregating the Data : Incoming Passengers visit each individual airports
dest_Psngr = airports_df.groupBy("Destination_airport", "Destination_city").agg(F.sum("Passengers").alias('Incoming_Passengers')).orderBy(desc('Incoming_Passengers'))
dest_Psngr.show()

+-------------------+------------------+-------------------+
|Destination_airport|  Destination_city|Incoming_Passengers|
+-------------------+------------------+-------------------+
|                ATL|       Atlanta, GA|          577954147|
|                ORD|       Chicago, IL|          528648148|
|                DFW|        Dallas, TX|          458322527|
|                LAX|   Los Angeles, CA|          389476602|
|                PHX|       Phoenix, AZ|          295580444|
|                LAS|     Las Vegas, NV|          269145891|
|                DTW|       Detroit, MI|          251467874|
|                MSP|   Minneapolis, MN|          245774036|
|                SFO| San Francisco, CA|          242283245|
|                IAH|       Houston, TX|          229105403|
|                MCO|       Orlando, FL|          228121902|
|                SEA|       Seattle, WA|          211924762|
|                EWR|        Newark, NJ|          209083721|
|                STL|     St. Louis, MO|          205024203|
|                CLT|     Charlotte, NC|          203665905|
|                LGA|      New York, NY|          197477822|
|                BOS|        Boston, MA|          190600918|
|                PHL|  Philadelphia, PA|          186855761|
|                SLC|Salt Lake City, UT|          152120434|
|                DCA|    Washington, DC|          145359903|
+-------------------+------------------+-------------------+
only showing top 20 rows

# Aggregating the Data : Total Incoming and outging Passengers visit each individual airports
ToTal = origin_Psngr.join(dest_Psngr, origin_Psngr.Origin_airport == dest_Psngr.Destination_airport, how = 'outer')
ToTal = ToTal.select(ToTal.Origin_airport,\
                     ToTal.Origin_city,\
                    (ToTal.Passengers_Travelling + ToTal.Incoming_Passengers).alias('Airport_Passengers_Traffic')).orderBy(desc('Airport_Passengers_Traffic'))
ToTal.show()

+--------------+------------------+--------------------------+
|Origin_airport|       Origin_city|Airport_Passengers_Traffic|
+--------------+------------------+--------------------------+
|           ATL|       Atlanta, GA|                1155078415|
|           ORD|       Chicago, IL|                1057666258|
|           DFW|        Dallas, TX|                 915476247|
|           LAX|   Los Angeles, CA|                 782482278|
|           PHX|       Phoenix, AZ|                 591438147|
|           LAS|     Las Vegas, NV|                 539736139|
|           DTW|       Detroit, MI|                 502450897|
|           MSP|   Minneapolis, MN|                 490971274|
|           SFO| San Francisco, CA|                 486063162|
|           IAH|       Houston, TX|                 457473254|
|           MCO|       Orlando, FL|                 453295762|
|           SEA|       Seattle, WA|                 424279043|
|           EWR|        Newark, NJ|                 418210378|
|           STL|     St. Louis, MO|                 409513281|
|           CLT|     Charlotte, NC|                 406475433|
|           LGA|      New York, NY|                 395998774|
|           BOS|        Boston, MA|                 380423462|
|           PHL|  Philadelphia, PA|                 372802737|
|           SLC|Salt Lake City, UT|                 304164728|
|           DCA|    Washington, DC|                 290415113|
+--------------+------------------+--------------------------+
only showing top 20 rows
