# * Joining Data Sets:  join multiple Data Sets using Spark based APIs.


# * Prepare Datasets for Joins
Pepare Dataset to join and get the details related to airports (origin and destination).

# * Make sure airport-codes is in HDFS.
%%sh
hdfs dfs -ls /public/airlines_all/airport-codes

from pyspark.sql import SparkSessionAnalyze the Dataset to confirm if there is header and also how the data is structured.
from pyspark.sql.functions import lit, count
import getpass
username = getpass.getuser()

spark = SparkSession. \
    builder. \
    config('spark.ui.port', '0'). \
    enableHiveSupport. \
    appName(f'{username} | Python - Joining Data Sets'). \
    master('yarn'). \
    getOrCreate()

spark.conf.set('spark.sql.shuffle.partitions', '2')

# * Analyzing Dataset to confirm if there is header or not and also how the data is structured.
spark.read. \
    text("/public/airlines_all/airport-codes"). \
    show(truncate=False)

# * Data is tab separated.
# * There is header for the data set.
# * Dataset have 4 fields - **Country, State, City, IATA**
  
    
# Create DataFrame airport_codes applying appropriate Schema.

airport_codes_path = "/public/airlines_all/airport-codes"

airport_codes = spark. \
    read. \
    csv(airport_codes_path,
        sep="\t",
        header=True,
        inferSchema=True
       )

# * Get the count of records
airport_codes.count()

# * Get the count of unique records and see if it is the same as total count.
airport_codes. \
    select("IATA"). \
    distinct(). \
    count()

 * If they are not equal, analyze the data and identify IATA codes which are repeated more than once.
  
#  duplicate_iata_count = airport_codes. \
    groupBy("IATA"). \
    agg(count(lit(1)).alias("iata_count")). \
    filter("iata_count > 1")
  duplicate_iata_count.show()
  
    ## Joining Data Frames : PROBLEM STATEMENTS :

# * Get number of flights departed from each of the US airport.
# * Get number of flights departed from each of the state.
# * Get the list of airports in the US from which flights are not departed.
# * Check if there are any origins in airlines data which do not have record in airport-codes.
# * Get the total number of flights from the airports that do not contain entries in airport-codes.
# * Get the total number of flights per airport that do not contain entries in airport-codes.


## Solutions - Problem 1

Get number of flights departed from each of the US airport.

from pyspark.sql.functions import col, lit, count
airlines_path = "/public/airlines_all/airlines-part/flightmonth=200801"
airport_codes_path = "/public/airlines_all/airport-codes"
airlines = spark. \
    read. \
    parquet(airlines_path)
airport_codes = spark. \
    read. \
    csv(airport_codes_path,
        sep="\t",
        header=True,
        inferSchema=True
       ). \
    filter("!(State = 'Hawaii' AND IATA = 'Big') AND Country='USA'")

joined_flights = airlines. \
    join(airport_codes, airport_codes.IATA == airlines.Origin)

flight_count_per_airport = joined_flights.groupBy("Origin").agg(count(lit(1)).alias("FlightCount")).orderBy(col("FlightCount").desc())
flight_count_per_airport.show()

## Solutions - Problem 2 : Number of flights departed from each of the state.

flight_count_per_state = joined_flights.groupBy("State").agg(count(lit(1)).alias("FlightCount")).orderBy(col("FlightCount").desc())
flight_count_per_state.show()

# Solutions - Problem 3 : Geting the list of airports in the US from which flights are not departed.
airports_not_used = joined_flights.filter(airlines.Origin.isNull()).select('City', 'State', 'Country', 'IATA')
airports_not_used.count()
