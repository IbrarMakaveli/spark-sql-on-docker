from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import sys

## Generique fonction

def save_as_csv(df,name_folder):
    df.repartition(1).write \
        .format("com.databricks.spark.csv") \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(sys.argv[2]+"/"+name_folder)
    
def add_columns(df):
    return df.withColumn(
        "distance_km", 
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
    ) \
    .withColumn(
        "average_speed_kmh", 
        F.round(
            F.col("distance_km") / ( F.col("trip_duration").cast(T.IntegerType()) / 3600 ) ,
            2
        )
    ) \
    .withColumn(
        "pickup_day",
        F.date_format(F.col("pickup_datetime"),'EEEE')
    )

def groupby_pickup_day(df):
    df = df.repartition("pickup_day")

    df_nb_trip_by_day = df.select(F.col("pickup_day")) \
                                    .groupby('pickup_day') \
                                    .count()
    
    df_nb_km_by_day = df.select(F.col("pickup_day"), "distance_km") \
                                    .groupby('pickup_day') \
                                    .agg(F.round(F.sum('distance_km'),2).alias('total_distance'))

    return df_nb_trip_by_day, df_nb_km_by_day

def average_speed_by_trip(df):
    df_average_speed_by_trip = df.select("id","average_speed_kmh")

    return df_average_speed_by_trip

def trip_by_intervaldate(df):
    df_nb_trip_by_intervaldate = df.groupBy(F.window(F.col("pickup_datetime"), "4 hours").alias("window")) \
                                            .count() \
                                            .select(F.col('window')['start'],F.col('window')['end'],'count') \

    return df_nb_trip_by_intervaldate
    
if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: TaxiTripSpark <file_csv> <output_dir>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession \
        .builder \
        .appName("Python Spark Taxi Trip Metrics") \
        .getOrCreate()

    df_taxi_metrics = add_columns(spark.read.csv(sys.argv[1],header=True))
    
    df_average_speed_by_trip = average_speed_by_trip(df_taxi_metrics)
    save_as_csv(df_average_speed_by_trip,"average_speed_by_trip")

    df_nb_trip_by_intervaldate = trip_by_intervaldate(df_taxi_metrics)
    save_as_csv(df_nb_trip_by_intervaldate,"nb_trip_by_intervaldate")

    df_nb_trip_by_day, df_nb_km_by_day = groupby_pickup_day(df_taxi_metrics)
    save_as_csv(df_nb_trip_by_day,"nb_trip_by_day")
    save_as_csv(df_nb_km_by_day,"nb_km_by_day")

    spark.stop()