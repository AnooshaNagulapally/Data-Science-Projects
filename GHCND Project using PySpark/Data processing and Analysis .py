#   import libraries
import pyspark.sql.functions.udf
import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession, Row, DataFrame, Window, functions as F
from pyspark.sql.types import *


spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
conf = sc.getConf()

N = int(conf.get("spark.executor.instances"))
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M
#   Processing
#   hdfs dfs -ls hdfs:///data/ghcnd/
#   hdfs dfs -du -v hdfs:///data/ghcnd
#   hdfs dfs -tree hdfs:///data/ghcnd

#   Define schemas for metadata tables
Daily = StructType([
    StructField("ID", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("VALUE", DoubleType(), True),
    StructField("MEASUREMENT_FLAG", StringType(), True),
    StructField("QUALITY_FLAG", StringType(), True),
    StructField("SOURCE_FLAG", StringType(), True),
    StructField("OBSERVATION_TIME", StringType(), True),

])


stations = StructType([
    StructField("ID", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("ELEVATION", DoubleType(), True),
    StructField("STATE", StringType(), True),
    StructField("NAME", StringType(), True),
    StructField("GSN_FLAG", StringType(), True),
    StructField("HCN/CRN_FLAG", StringType(), True),
    StructField("WMO_ID", StringType(), True),
])


States = StructType([
    StructField("CODE", StringType(), True),
    StructField("NAME", StringType(), True),
])

Countries = StructType([
    StructField("CODE", StringType(), True),
    StructField("NAME", StringType(), True),
])

Inventory = StructType([
    StructField("ID", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("ELEMENT", DoubleType(), True),
    StructField("FIRSTYEAR", IntegerType(), True),
    StructField("LASTYEAR", IntegerType(), True),

])

#   take a subset of daily data with 1000 rows limit.
daily_2022 = spark.read.load(
    "hdfs:/data///ghcnd/daily/2022.csv.gz", format="csv", header="false", limit=1000)
daily_2022.show()

DailyNew = daily_2022.select(
    F.col("_c0").alias("ID"),
    F.col("_c1").alias("DATE"),
    F.col("_c2").alias("ELEMENT"),
    F.col("_c3").alias("VALUE"),
    F.col("_c4").alias("MEASUREMENT_FLAG"),
    F.col("_c5").alias("QUALITY_FLAG"),
    F.col("_c6").alias("SOURCE_FLAG"),
    F.col("_c7").alias("OBSERVATION_TIME"))

DailyNew.cache()
DailyNew.show()
DailyNew.count()

#   Parse the metadata tables
stations_text = spark.read.text("hdfs:/data///ghcnd/ghcnd-stations.txt")
stations_parsed = stations_text.select(
    stations_text.value.substr(1, 11).alias("ID"),
    stations_text.value.substr(13, 8).alias("LATITUDE"),
    stations_text.value.substr(22, 9).alias("LONGITUDE"),
    stations_text.value.substr(32, 6).alias("ELEVATION"),
    stations_text.value.substr(39, 2).alias("STATE"),
    stations_text.value.substr(42, 30).alias("NAME"),
    stations_text.value.substr(73, 3).alias("GSN_FLAG"),
    stations_text.value.substr(77, 3).alias("HCN/CRN_FLAG"),
    stations_text.value.substr(81, 5).alias("WMO_ID"),
)

stations_parsed.cache()
stations_parsed.show()
stations_parsed.count()

states_text = spark.read.text("hdfs:/data///ghcnd/ghcnd-states.txt")
states_parsed = states_text.select(
    states_text.value.substr(1, 2).alias("CODE"),
    states_text.value.substr(4, 47).alias("NAME"),
)

states_parsed.cache()
states_parsed.show()
states_parsed.count()

countries_text = spark.read.text("hdfs:/data///ghcnd/ghcnd-countries.txt")
countries_parsed = countries_text.select(
    countries_text.value.substr(1, 2).alias("CODE"),
    countries_text.value.substr(4, 47).alias("NAME"),
)

countries_parsed.cache()
countries_parsed.show()
countries_parsed.count()

inventory_text = spark.read.text("hdfs:/data///ghcnd/ghcnd-inventory.txt")
inventory_parsed = inventory_text.select(
    inventory_text.value.substr(1, 11).alias("ID"),
    inventory_text.value.substr(13, 8).alias("LATITUDE"),
    inventory_text.value.substr(22, 9).alias("LONGITUDE"),
    inventory_text.value.substr(32, 4).alias("ELEMENT"),
    inventory_text.value.substr(37, 4).alias("FIRSTYEAR"),
    inventory_text.value.substr(42, 4).alias("LASTYEAR"),
)

inventory_parsed.cache()
inventory_parsed.show()
inventory_parsed.count()


stations_parsed.filter(stations_parsed.WMO_ID == '     ').count()
#   110400

#   Extract Country code
stations_parsed = stations_parsed.withColumn(
    "COUNTRYCODE", stations_parsed.ID.substr(1, 2))
stations_parsed.show()

#   Rename columns in states and countries tables and left join them to stations
countries_New = countries_parsed.select(
    F.col("NAME").alias("CNAME"),
    F.col("CODE").alias("CCODE")
)

states_New = states_parsed.select(
    F.col("NAME").alias("SNAME"),
    F.col("CODE").alias("SCODE")
)

stations_countries_join = stations_parsed.join(
    countries_New, countries_New.CCODE == stations_parsed.COUNTRYCODE, how="left")
stations_countries_join.show()

#   left join states that are present in US
cond = [((states_New.SCODE == stations_countries_join.STATE)
         & (stations_countries_join.CCODE == "US"))]
stations_states_join = stations_countries_join.join(
    states_New, on=cond, how="left")
stations_states_join.show()


stations_states_join.select(stations_states_join.SCODE).distinct().show(100)
stations_states_join.select(stations_states_join.SCODE).distinct().count()
#   53
stations_states_join.filter(stations_states_join.CCODE == "US").show()


#   Find the first year and last year that each station was active
inv_min_year = inventory_parsed.groupBy("ID").agg(F.min("FIRSTYEAR"))

inv_min_firstyear = inv_min_year.withColumnRenamed(
    "min(FIRSTYEAR)", "MINFIRSTYEAR")
    
inv_min_firstyear.show()

inv_min_firstyear = inv_min_firstyear.withColumn(
    "MINFIRSTYEAR", inv_min_firstyear["MINFIRSTYEAR"].cast(IntegerType()))
    
inventory_stations_join = stations_states_join.join(inv_min_firstyear.selectExpr(
    "ID", "MINFIRSTYEAR"), stations_states_join.ID == inv_min_firstyear.ID, how="left").drop(inv_min_firstyear.ID)
    
inventory_stations_join.show()

inv_max_Year = inventory_parsed.groupBy("ID").agg(F.max("LASTYEAR"))

inv_max_Year = inv_max_Year.withColumnRenamed(
    "max(LASTYEAR)", "MAXLASTYEAR")

inv_max_Year = inv_max_Year.withColumn(
    "MAXLASTYEAR", inv_max_Year["MAXLASTYEAR"].cast(IntegerType()))

inventory_stations_join = inventory_stations_join.join(inv_max_Year.selectExpr(
    "ID", "MAXLASTYEAR"), inventory_stations_join.ID == inv_max_Year.ID, how="left").drop(inv_max_Year.ID)
    
inventory_stations_join.show()

#   Find total elements collected by each station.
inv_distinct = inventory_parsed.groupBy(
    inventory_parsed.ID).agg(F.countDistinct('ELEMENT'))
    
inv_distinct = inv_distinct.withColumnRenamed(
    "count(ELEMENT)", "DISTINCTELEMENT")
    
inv_distinct = inv_distinct.withColumn(
    "DISTINCTELEMENT", inv_distinct["DISTINCTELEMENT"].cast(IntegerType()))
    
inventory_stations_join = inventory_stations_join.join(inv_distinct.selectExpr(
    "ID", "DISTINCTELEMENT"), inventory_stations_join.ID == inv_distinct.ID, how="left").drop(inv_distinct.ID)

inventory_stations_join.show()

#   count the number of core elements collected by each station
inv_core = inventory_parsed.filter((inventory_parsed.ELEMENT == "TMAX") | (inventory_parsed.ELEMENT == "TMIN") | (
    inventory_parsed.ELEMENT == "PRCP") | (inventory_parsed.ELEMENT == "SNOW") | (inventory_parsed.ELEMENT == "SNWD"))

inv_core.show()

inv_core = inv_core.groupBy(inv_core.ID).agg(F.count("ELEMENT"))
inv_core = inv_core.withColumnRenamed("count(ELEMENT)", "COUNTCORE")

inv_core = inv_core.withColumn(
    "COUNTCORE", inv_core["COUNTCORE"].cast(IntegerType()))

inventory_stations_join = inventory_stations_join.join(inv_core.selectExpr(
    "ID", "COUNTCORE"), inventory_stations_join.ID == inv_core.ID, how="left").drop(inv_core.ID)

inventory_stations_join.show()

#   count the number of non core elements collected by each station
inv_non_core = inventory_parsed.filter((inventory_parsed.ELEMENT != "TMAX") & (inventory_parsed.ELEMENT != "TMIN") & (
    inventory_parsed.ELEMENT != "PRCP") & (inventory_parsed.ELEMENT != "SNOW") & (inventory_parsed.ELEMENT != "SNWD"))
    
inv_non_core = inv_non_core.groupBy(inv_non_core.ID).agg(F.count("ELEMENT"))
inv_non_core = inv_non_core.withColumnRenamed("count(ELEMENT)", "COUNTNONCORE")

inv_non_core = inv_non_core.withColumn(
    "COUNTNONCORE", inv_non_core["COUNTNONCORE"].cast(IntegerType()))
    
inventory_stations_join = inventory_stations_join.join(inv_non_core.selectExpr(
    "ID", "COUNTNONCORE"), inventory_stations_join.ID == inv_non_core.ID, how="left").drop(inv_non_core.ID)

inventory_stations_join.show()

count_stations = inv_core.groupBy(inv_core.COUNTCORE).agg(F.count("ID"))
count_stations.show()

prcp_count = inventory_parsed.filter(inventory_parsed.ELEMENT == "PRCP")
prcp_count.show()

prcp_count = prcp_count.groupby(prcp_count.ELEMENT).agg(F.count("ID"))
prcp_count.show()

#   Save the stations data final to output directory
inventory_stations_join.write.format("csv").mode("overwrite").option(
    "header", "true").save("hdfs:///user/ana146/ghcnd/stations.csv.gz")

stationsNew = spark.read.load(
    "hdfs:///user/ana146/ghcnd/stations.csv.gz", format="csv", header="true", limit=1000)

stationsNew.show()

#   Take the subset of daily and left join stations
Daily_1000 = DailyNew.sample(False, 0.1, seed=0).limit(1000)
Daily_1000.show()

inventory_stations_join = inventory_stations_join.withColumnRenamed(
    "ID", "STATIONID")
daily_stations = Daily_1000.join(
    inventory_stations_join, Daily_1000.ID == inventory_stations_join.STATIONID, how="left")
daily_stations.show()

daily_stations.filter(daily_stations.STATIONID.isNull()).count()
#   No stations in daily that are not present in stations.

# --------------------------------------------------------------------------------------------------------------------------------------

#   Analysis

#   Increase the resources
start_pyspark_shell - e 4 - c 2 - w 4 - m 4


spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
conf = sc.getConf()

stationsNew = spark.read.load(
    "hdfs:///user/ana146/ghcnd/stations.csv.gz", format="csv", header="true", limit=1000)
stationsNew.show()

#   Count the stations in GSN, HCN & CRN networks
stationsNew.count()
# 118493
stationsNew.filter(stationsNew.MAXLASTYEAR == 2021).count()
# 38284

stationsNew.filter(stationsNew.GSN_FLAG == "GSN").count()
# 991
stationsNew = stationsNew.withColumnRenamed("HCN/CRN_FLAG", "HCN_CRN_FLAG")
stationsNew.filter(stationsNew.HCN_CRN_FLAG == "HCN").count()
# 1218
stationsNew.filter(stationsNew.HCN_CRN_FLAG == "CRN").count()
# 0
stationsNew.select(stationsNew.HCN_CRN_FLAG).distinct().show()
#HCN, null
stationsNew.filter((stationsNew.GSN_FLAG == "GSN") & (
    stationsNew.HCN_CRN_FLAG == "HCN")).count()
# 14

#   join the tables stations by country and save to the output directory
stations_byCountry = stationsNew.groupBy(stationsNew.CCODE).agg(F.count("ID"))
stations_byCountry.show()

stations_byCountry = stations_byCountry.withColumnRenamed(
    "count(ID)", "TOTALSTATIONS")
stations_byCountry.show()

countries_parsed = countries_parsed.join(stations_byCountry.selectExpr(
    "CCODE", "TOTALSTATIONS"), stations_byCountry.CCODE == countries_parsed.CODE, how="left").drop(stations_byCountry.CCODE)
countries_parsed.show()

countries_parsed.write.format("csv").mode("overwrite").option(
    "header", "true").save("hdfs:///user/ana146/ghcnd/countries.csv.gz")

countriesNew = spark.read.load(
    "hdfs:///user/ana146/ghcnd/countries.csv.gz", format="csv", header="true", limit=1000)
countriesNew.show()

#   join the tables stations by state and save to the output directory
stations_byState = stationsNew.groupBy(stationsNew.SCODE).agg(F.count("ID"))
stations_byState.show()

stations_byState = stations_byState.withColumnRenamed(
    "count(ID)", "TOTALSTATIONS_ST")
stations_byState.show()

states_parsed = states_parsed.join(stations_byState.selectExpr(
    "SCODE", "TOTALSTATIONS_ST"), stations_byState.SCODE == states_parsed.CODE, how="left").drop(stations_byState.SCODE)
states_parsed.show()

states_parsed.write.format("csv").mode("overwrite").option(
    "header", "true").save("hdfs:///user/ana146/ghcnd/states.csv.gz")

statesNew = spark.read.load(
    "hdfs:///user/ana146/ghcnd/states.csv.gz", format="csv", header="true", limit=1000)
statesNew.show()

#   find  the distinct stations in Northern Hemisphere
stationsNew.select(stationsNew.CNAME).distinct().show(1000, truncate=False)

#   Palmyra Atoll [United States], Johnston Atoll [United States], Wake Island [United States], Virgin Islands [United States], Puerto Rico [United States], American Samoa [United States], Midway Islands [United States}, Northern Mariana Islands [United States]

stationsNH = stationsNew.filter((stationsNew.CNAME == "Palmyra Atoll [United States]") | (stationsNew.CNAME == "Johnston Atoll [United States]") | (stationsNew.CNAME == "Wake Island [United States]") | (stationsNew.CNAME == "Virgin Islands [United States]") | (
    stationsNew.CNAME == "Puerto Rico [United States]") | (stationsNew.CNAME == "American Samoa [United States]") | (stationsNew.CNAME == "Midway Islands [United States}") | (stationsNew.CNAME == "Northern Mariana Islands [United States]"))
stationsNH.count()
# 318


#    Take the subset of stationsNew table  and cross join stations
stations_5 = stationsNew.sample(False, 0.1, seed=0).limit(5)
stations_5.show()

stations_5copy = stations_5.select("*")
stations_5copy.show()
stations_5copy = stations_5copy.withColumnRenamed("LATITUDE", "LAT")
stations_5copy = stations_5copy.withColumnRenamed("LONGITUDE", "LON")
stations_5copy = stations_5copy.withColumnRenamed("CNAME", "CTRYNAME")
stations_5copy.show()

merged = stations_5.crossJoin(stations_5copy)
merged.show()


#   Define Function to compute distances using longitude and latitude
def hav_dist(LATITUDE_A, LONGITUDE_A, LATITUDE_B, LONGITUDE_B):
    a = (
        F.pow(F.sin(F.radians(LATITUDE_B - LATITUDE_A) / 2), 2) +
        F.cos(F.radians(LATITUDE_A)) * F.cos(F.radians(LATITUDE_B)) *
        F.pow(F.sin(F.radians(LONGITUDE_B - LONGITUDE_A) / 2), 2))
    distance = F.atan2(F.sqrt(a), F.sqrt(-a + 1)) * 12742
    return distance


Dist = merged.withColumn("DISTANCE", hav_dist(
    merged.LONGITUDE, merged.LATITUDE, merged.LON, merged.LAT).cast(DoubleType()))
Dist.select(Dist.CNAME, Dist.CTRYNAME, Dist.DISTANCE).show()

#   Compute pairwise distances for NZ stations

NZSTATIONS = stationsNew.filter(stationsNew.CCODE == "NZ")
NZSTATIONS.show()
NZSTATIONS_copy = NZSTATIONS.select("*")
NZSTATIONS_copy = NZSTATIONS_copy.withColumnRenamed("LATITUDE", "LAT")
NZSTATIONS_copy = NZSTATIONS_copy.withColumnRenamed("LONGITUDE", "LON")
NZSTATIONS_copy = NZSTATIONS_copy.withColumnRenamed("NAME", "STATIONNAME")
NZSTATIONS_copy.show()

NZSTATIONS_merged = NZSTATIONS.crossJoin(NZSTATIONS_copy)
NZSTATIONS_merged.show()

NZ_Dist = NZSTATIONS_merged.withColumn("DISTANCE", hav_dist(
    NZSTATIONS_merged.LONGITUDE, NZSTATIONS_merged.LATITUDE, NZSTATIONS_merged.LON, NZSTATIONS_merged.LAT).cast(DoubleType()))
NZ_Dist = NZ_Dist.select(NZ_Dist.NAME, NZ_Dist.LATITUDE, NZ_Dist.LONGITUDE,
                         NZ_Dist.STATIONNAME, NZ_Dist.LAT, NZ_Dist.LON, NZ_Dist.DISTANCE)

NZ_Dist_sort = NZ_Dist.sort(['DISTANCE'], ascending=True)
NZ_Dist_sort.show()

NZ_Dist_sort.write.format("csv").mode("overwrite").option(
    "header", "true").save("hdfs:///user/ana146/ghcnd/NZStationPairs.csv.gz")

#   the closeset stations in NZ are Paraparaumu and Wellington AERO

#   explore data in hdfs  and count the files & blocks for 2021&2022 years

hdfs dfs - ls / data/ghcnd/daily/
hdfs dfs - ls - h / data/ghcnd/daily/
hdfs dfs - du / data/ghcnd/daily/
hdfs dfs - cat / data/ghcnd/daily/2022.csv.gz | gunzip | wc - l
# 5971307
hdfs dfs - cat / data/ghcnd/daily/2021.csv.gz | gunzip | wc - l
# 34657282
hdfs fsck / data/ghcnd/daily/2022.csv.gz - files - blocks
# Total blocks (validated):      1 (avg. block size 25985757 B) - 24.78MB
hdfs fsck / data/ghcnd/daily/2021.csv.gz - files - blocks
#  Total blocks (validated):      2 (avg. block size 73468012 B)       - 140.12MB
# /data/ghcnd/daily/2021.csv.gz 146936025 bytes, replicated: replication=8, 2 block(s):  OK
# 0. BP-700027894-132.181.129.68-1626517177804:blk_1073769757_28937 len=134217728 Live_repl=8    - 128MB
# 1. BP-700027894-132.181.129.68-1626517177804:blk_1073769758_28938 len=12718297 Live_repl=8     - 12.12 MB

hdfs dfs - copyToLocal / data/ghcnd/daily/2022.csv.gz ./
# year_csv_gz directory

#   load multiple years data in spark 
daily2022 = spark.read.load(
    "hdfs:/data///ghcnd/daily/2022.csv.gz", format="csv", header="false")
daily2022.show()
daily2022.count()
# 5971307
# Total of 4 stages. 1 task is executed by each stage.

daily2021 = spark.read.load(
    "hdfs:/data///ghcnd/daily/2021.csv.gz", format="csv", header="false")
daily2021.show()
daily2021.count()
# 34657282
# Total of 4 stages. 1 task is executed by each stage.

daily2014To22 = spark.read.load(
    "hdfs:/data///ghcnd/daily/20{1[4-9].csv.gz,2[0-2].csv.gz}*", format="csv", header="false")
daily2014To22.show()
daily2014To22.count()
# 284918108

#   total of 4 stages and 12 tasks are executed.

#   load and count daily data for all years in spark

Daily_All = spark.read.load(
    "hdfs:/data///ghcnd/daily/*.csv.gz", format="csv", header="false")
Daily_All.show()
# Daily_All.count()
# 3000243596

Daily_subset = Daily_All.sample(False, 0.1, seed=0).limit(1000)
Daily_subset.show()
Daily_subset.count()

#   filter and count the five core elements  

daily_core = Daily_All.filter((Daily_All._c2 == "TMAX") | (Daily_All._c2 == "TMIN") | (
    Daily_All._c2 == "PRCP") | (Daily_All._c2 == "SNOW") | (Daily_All._c2 == "SNWD"))
daily_core.show()


five_core = daily_core.groupBy(daily_core._c2).agg(F.count("_c0"))
five_core.show()

#   filter and count for TMIN only 

daily_TMin = Daily_All.filter(
    (Daily_All._c2 == "TMIN") | (Daily_All._c2 == "TMAX"))
daily_TMin.show()

daily_T = daily_TMin.groupBy(
    daily_TMin._c0, daily_TMin._c1).agg(F.collect_set("_c2"))
daily_T.show()

daily_T = daily_T.withColumnRenamed("collect_set(_c2)", "SET")
daily_T = daily_T.withColumn("SET", daily_T["SET"].cast(StringType()))

daily_T1 = daily_T.filter(daily_T.SET == "[TMIN]")
daily_T1.count()
# 8808805
daily_T1.select(daily_T1._c0).distinct().count()
# 27650

#   join daily and stations data to filter nz stations 

stations_Data = stationsNew.select(
    "STATIONID", "CCODE", "MINFIRSTYEAR", "MAXLASTYEAR")
stations_Data.show()
daily_Stations_merged = Daily_All.join(
    stations_Data, stations_Data.STATIONID == Daily_All._c0, how="left")
daily_Stations_merged.show()
daily_Stations_merged = daily_Stations_merged.filter(
    daily_Stations_merged.CCODE == "NZ")
daily_Stations_merged.show()
daily_Stations_merged = daily_Stations_merged.filter(
    (daily_Stations_merged._c2 == "TMIN") | (daily_Stations_merged._c2 == "TMAX"))
daily_Stations_merged.show()

daily_Stations_merged.write.format("csv").mode("overwrite").option(
    "header", "true").save("hdfs:///user/ana146/ghcnd/NzTMinTMax.csv.gz")
daily_Stations_merged.count()
# 472271

daily_Stations_merged.agg({"MINFIRSTYEAR": "max"}).show()
daily_Stations_merged.agg({"MINFIRSTYEAR": "min"}).show()
# 1997
# 1940
daily_Stations_merged.agg({"MAXLASTYEAR": "max"}).show()
# 2021

hdfs dfs - copyToLocal / user/ana146/ghcnd/NzTMinTMax.csv.gz ./
hdfs dfs - cat / user/ana146/ghcnd/NzTMinTMax.csv.gz | gunzip | wc - l

#   filter the core element for PRCP and save to the output directory
stations_Data = stationsNew.select(
    "STATIONID", "CCODE", "MINFIRSTYEAR", "MAXLASTYEAR")
stations_Data.show()

daily_Stations_merged_prcp = Daily_All.join(
    stations_Data, stations_Data.STATIONID == Daily_All._c0, how="left")
daily_Stations_merged_prcp.show()

daily_Stations_merged_prcp = daily_Stations_merged_prcp.filter(
    daily_Stations_merged_prcp._c2 == "PRCP")
daily_Stations_merged_prcp.show()

daily_Stations_merged_prcp1 = daily_Stations_merged_prcp.groupBy(
    daily_Stations_merged_prcp._c1, daily_Stations_merged_prcp.CCODE).agg(F.avg("_c3"))
daily_Stations_merged_prcp1.show()

daily_Stations_merged_prcp1.write.format("csv").mode("overwrite").option(
    "header", "true").save("hdfs:///user/ana146/ghcnd/NzPrcp.csv.gz")

#daily_Stations_merged_prcp= daily_Stations_merged_prcp.withColumn("DATE", F.to_date("_c1","yyyy")).show()
