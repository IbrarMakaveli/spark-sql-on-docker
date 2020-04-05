# Spark SQL NY Taxi Trip on Docker

## Installation

Build and run Spark on `docker` and create containers with scale out :

```bash
docker-compose up --scale worker=$NUMBBER_OF_WORKER
```

## Usage general

Run Spark SQL Transformation :
```bash
TaxiTripSpark.py <file_csv> <output_dir>
```

Run Spark SQL Test :
```bash
python -m pytest TaxiTripSpark_test.py
```

## Usage with docker installation

Run Spark SQL Transformation with `pySpark` in the container :
```bash
docker exec -it spark-sql-on-docker_master_1 /bin/bash bin/spark-submit --deploy-mode client /tmp/src/TaxiTripSpark.py /tmp/data/taxi_ny_data.csv /tmp/output
```

Run Spark SQL with `Test` in the container :

```bash
docker exec -it spark-sql-on-docker_master_1 python -m pytest /tmp/src/TaxiTripSpark_test.py
```

## Documentation

We have a dataset `data/taxi_ny_data.csv` with data from the different New York taxi trips, the goal is to calculate different indicators from these data.

The program is sequenced as follows: We have a main function which is for reading and writing data who calls other functions that will transform the data and return a dataframe with new metrics.

### Reading and writing data function

#### Reading

We initialize a Session Spark function with a name and the default configuration, then we use the function `spark.read.csv` to read the csv with `header=true` because have a header in our data
```python
spark = SparkSession \
        .builder \
        .appName("Python Spark Taxi Trip Metrics") \
        .getOrCreate()
spark.read.csv(sys.argv[1],header=True)
```
#### Writing

`save_as_csv` takes in input a dataframe and a folder name, it allows to write the indicators in CSV format with a repartition to 1 to have one and only one file at the end.
```python
df.repartition(1).write \
      .format("com.databricks.spark.csv") \
      .mode("overwrite") \
      .option("header", "true") \
      .csv(sys.argv[2]+"/"+name_folder)
```

### Transformation function

#### add_columns

`add_columns` return a datframe and allows you to add 3 columns which are useful metrics throughout the program, hence the interest of adding them just after reading the data and whole data : 
- distance in km with haversine formula that determines the distance in a great circle between two points on a sphere (the earth in our case), taking into account their longitudes and latitudes, rounded to 2 decimal places after the decimal point
```python
F.round(
    (F.atan2(F.sqrt(
        (
            F.pow(F.sin(F.radians(F.col("dropoff_latitude") - F.col("pickup_latitude")) / 2), 2) +
            F.cos(F.radians(F.col("pickup_latitude"))) * F.cos(F.radians(F.col("dropoff_latitude"))) *
            F.pow(F.sin(F.radians(F.col("dropoff_longitude") - F.col("pickup_longitude")) / 2), 2)
        )
    ), F.sqrt(-1*
        (
        F.pow(F.sin(F.radians(F.col("dropoff_latitude") - F.col("pickup_latitude")) / 2), 2) +
        F.cos(F.radians(F.col("pickup_latitude"))) * F.cos(F.radians(F.col("dropoff_latitude"))) *
        F.pow(F.sin(F.radians(F.col("dropoff_longitude") - F.col("pickup_longitude")) / 2), 2)
        ) 
    + 1)) * 2 * 6371), 
    2
)
```
- average speed in km/h with the formula of the speed divided by 3600 to get the result in km/hr, rounded to 2 decimal places after the decimal point
```python
F.round(
    F.col("distance_km") / ( F.col("trip_duration").cast(T.IntegerType()) / 3600 ) ,
    2
)
```
- weekday we use `date_format` function which allows to extract the day of the week from a datatime/date
```python
F.date_format(F.col("pickup_datetime"),'EEEE') 
```
#### groupby_pickup_day

`groupby_pickup_day` returns 2 dataframes, each containing an aggregation on the day of the week. We make a repartition of the data on the original dataframe from the day of the week, this will avoid too much shuffling of the data afterwards in computation:
- the number of trips made per day of the week
```python
df.select(F.col("pickup_day")) \
    .groupby('pickup_day') \
    .count()
```
- the number of km travelled per day of the week
```python
df.select(F.col("pickup_day"), "distance_km") \
    .groupby('pickup_day') \
    .agg(F.round(F.sum('distance_km'),2).alias('total_distance'))
```
#### average_speed_by_trip

`average_speed_by_trip` return a dataframe, which makes a filter only on the columns id and average speed for the writing of the data :
```python
df.select("id","average_speed_kmh")
```

#### trip_by_intervaldate

`trip_by_intervaldate` return a dataframe, the number of trip made according to the day's schedule in 4-hour increments :
- we made an aggregation on the pickup datetime, and created a 4 hours window with the `window` function of Spark SQL.
```python
df.groupBy(F.window(F.col("pickup_datetime"), "4 hours").alias("window")) \
    .count() \
    .select(F.col('window')['start'],F.col('window')['end'],'count') \
```

## Credit

Original docker spark : https://github.com/suraj95/Spark-on-Docker

My modifications : 
- add hive jar
- add pytest
