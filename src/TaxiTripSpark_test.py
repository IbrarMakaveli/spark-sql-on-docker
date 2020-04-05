import pytest
import TaxiTripSpark
import pandas as pd
from haversine import haversine
from pandas.testing import assert_frame_equal
from datetime import datetime

pytestmark = pytest.mark.usefixtures("spark_session")

## INIT Data and function

INPUT_DATA = [("id0801584",1,"2020-01-30 22:01:40","2020-01-30 22:01:03",6,-73.982856750488281,40.742195129394531,-73.992080688476562,40.749183654785156,"N",45),
    ("id1154431",1,"2020-04-14 08:48:26","2020-04-14 09:00:37",1,-73.994255065917969,40.745803833007813,-73.999656677246094,40.723342895507813,"N",731),
    ("id3552682",1,"2020-06-05 09:55:13","2020-06-27 10:17:10",1,-74.003982543945313,40.7130126953125,-73.979194641113281,40.749923706054688,"N",1317),
    ("id3390316",2,"2020-06-05 10:47:23","2020-06-05 10:51:34",1,-73.98388671875,40.738197326660156,-73.991203308105469,40.727870941162109,"N",251),
    ("id2070428",1,"2020-02-28 02:23:02","2020-02-28 02:31:08",1,-73.980369567871094,40.742420196533203,-73.962852478027344,40.760635375976563,"N",486)]

def create_df_with_add_columns(spark_session):

    input_df = spark_session.createDataFrame(INPUT_DATA,
        ["id","vendor_id","pickup_datetime","dropoff_datetime","passenger_count","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","store_and_fwd_flag","trip_duration"],
    )

    return TaxiTripSpark.add_columns(input_df)

#### Start of unittest function Spark

def test_add_columns(spark_session):

    df_result = create_df_with_add_columns(spark_session).toPandas()

    df_expected = pd.DataFrame(INPUT_DATA,
        columns=["id","vendor_id","pickup_datetime","dropoff_datetime","passenger_count","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","store_and_fwd_flag","trip_duration"],
    )

    for index, row in df_expected.iterrows():
        pickup_point = (row["pickup_latitude"], row["pickup_longitude"])
        drop_point = (row["dropoff_latitude"], row["dropoff_longitude"])
        distance_km = round(haversine(pickup_point, drop_point, unit='km'), 2)
        df_expected.loc[index,'distance_km'] = distance_km
        df_expected.loc[index,'average_speed_kmh'] = round((distance_km / ( row['trip_duration'] / 3600 )), 2)
        df_expected.loc[index,'pickup_day'] = datetime.strptime(row['pickup_datetime'], '%Y-%m-%d %H:%M:%S').strftime('%A')

    assert_frame_equal(df_result,df_expected)

def test_groupby_pickup_day(spark_session):

    df_result_trip, df_result_km = TaxiTripSpark.groupby_pickup_day(create_df_with_add_columns(spark_session))
    df_result_trip = df_result_trip.toPandas()
    df_result_km = df_result_km.toPandas()

    df_expected_trip = pd.DataFrame([("Thursday",1),
                                    ("Tuesday",1),
                                    ("Friday",3)],
                                    columns=["pickup_day","count"])

    df_expected_km = pd.DataFrame([("Thursday",1.10),
                                ("Tuesday",2.54),
                                ("Friday",8.42)],
                                columns=["pickup_day","total_distance"])
    
    assert_frame_equal(df_result_trip,df_expected_trip)
    assert_frame_equal(df_result_km,df_expected_km)


def test_average_speed_by_trip(spark_session):

    df_result_average_speed = TaxiTripSpark.average_speed_by_trip(create_df_with_add_columns(spark_session)).toPandas()

    df_expected_average_speed = pd.DataFrame([("id0801584",88.00),
                                    ("id1154431",12.51),
                                    ("id3552682",12.60),
                                    ("id3390316",18.65),
                                    ("id2070428",18.59)],
                                    columns=["id","average_speed_kmh"])

    assert_frame_equal(df_result_average_speed,df_expected_average_speed)

def test_trip_by_intervaldate(spark_session):

    df_result_intervaldate = TaxiTripSpark.trip_by_intervaldate(create_df_with_add_columns(spark_session)).toPandas()

    df_expected_intervaldate = pd.DataFrame([(pd.to_datetime("2020-01-30 20:00:00"),pd.to_datetime("2020-01-31 00:00:00"),1),
                                    (pd.to_datetime("2020-04-14 08:00:00"),pd.to_datetime("2020-04-14 12:00:00"),1),
                                    (pd.to_datetime("2020-06-05 08:00:00"),pd.to_datetime("2020-06-05 12:00:00"),2),
                                    (pd.to_datetime("2020-02-28 00:00:00"),pd.to_datetime("2020-02-28 04:00:00"),1)],
                                    columns=["window.start","window.end","count"])

    assert_frame_equal(df_result_intervaldate,df_expected_intervaldate)
