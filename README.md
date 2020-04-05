# Spark SQL NY Taxi Trip on Docker

## Installation

Build and run on `docker` your containers :

```bash
docker-compose up --scale worker=$NUMBBER_OF_WORKER
```

## Usage

Run Spark SQL with `pySpark` in the container :

```bash
docker exec -it spark-on-docker-master_master_1 /bin/bash bin/spark-submit --deploy-mode client /tmp/src/TaxiTripSpark.py /tmp/data/train.csv /tmp/output
```

Run Spark SQL `Test` in the container :

```bash
docker exec -it spark-on-docker-master_master_1 python -m pytest -s /tmp/src/TaxiTripSpark_test.py
```

## Credit

Original docker spark : https://github.com/suraj95/Spark-on-Docker

My modifications : 
- Add hive jar
- Add pytest