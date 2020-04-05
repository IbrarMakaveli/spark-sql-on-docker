# Spark SQL NY Taxi Trip on Docker

## Installation

Build and run on `docker` your containers :

```bash
docker-compose up --scale worker=$NUMBBER_OF_WORKER
```

## Usage

Run Spark SQL with `pySpark` in the container :

```bash
docker exec -it spark-sql-on-docker_master_1 /bin/bash bin/spark-submit --deploy-mode client /tmp/src/TaxiTripSpark.py /tmp/data/taxi_ny_data.csv /tmp/output
```

Run Spark SQL `Test` in the container :

```bash
docker exec -it spark-sql-on-docker_master_1 python -m pytest /tmp/src/TaxiTripSpark_test.py
```

## Documentation


## Credit

Original docker spark : https://github.com/suraj95/Spark-on-Docker

My modifications : 
- Add hive jar
- Add pytest