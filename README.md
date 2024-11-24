# GPS-Project
Big data class final project

## Prerequisites
- **Python**: You need to have Python installed. You can install Python by following the official [documentation](https://www.python.org/downloads/).
- **Docker**: You need to have Docker installed. If you don't have Docker, follow the official [documentation](https://docs.docker.com/desktop/).
- **PySpark**: You need to have PySpark installed. You can install it using the following command:  
  ```bash
  pip install pyspark

## Spark Cluster
- Create the Spark cluster with the command (you need to be inside the `spark_cluster` folder):  
    `docker compose up --scale spark-worker=3`

- Create the Spark submit container with the command:  
    `docker run -d --name spark_submit_container --network spark_cluster_default --volumes-from spark_cluster-spark-master-1 -p 4041:4040 spark-submit /bin/bash -c "sleep infinity"`

## Kafka Cluster
- Create the Kafka cluster (you need to be inside the `kafka_cluster` directory):  
    `docker compose up -d`

- Create topics for each producer, in this case, we have 3 producers:  
  - Create the first topic:  
    ```bash
    docker exec -it <kafka_cluster> \
    /opt/kafka/bin/kafka-topics.sh \
    --create --zookeeper zookeeper:2181 \
    --replication-factor 1 --partitions 1 \
    --topic gps-topic-location
    ```

  - Create the second topic:  
    ```bash
    docker exec -it <kafka_cluster> \
    /opt/kafka/bin/kafka-topics.sh \
    --create --zookeeper zookeeper:2181 \
    --replication-factor 1 --partitions 1 \
    --topic gps-topic-location2
    ```

  - Create the third topic:  
    ```bash
    docker exec -it <kafka_cluster> \
    /opt/kafka/bin/kafka-topics.sh \
    --create --zookeeper zookeeper:2181 \
    --replication-factor 1 --partitions 1 \
    --topic gps-topic-location3
    ```

- Create the container that's going to have the producer file:
    `docker run -d --name kafka_producer_gps_data --network spark_cluster_default --volumes-from spark_cluster-spark-master-1 kafka-producer-app /bin/bash -c "sleep infinity"`

- Run the producer script whith the command:
    `docker exec -it kafka_producer_gps_data python3 /opt/spark-apps/kafka_producer/kafka_gps_producer.py --kafka-bootstrap <kafka_cluster> --kafka-topic gps-topic-location`

- Run the consumer file:
    ```bash
    docker exec -it spark_submit_container /spark/bin/spark-submit \
    --master spark://<spark_cluster>:7077 \
    --deploy-mode client \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.2 \
    /opt/spark-apps/gps_consumer.py --kafka-bootstrap <kafka_cluster>
    ``` 

## Mongo database container
- Create the mongodb container:
    `docker run -d --name mongo-gps --network spark_cluster_default mongodb/mongodb-community-server:latest`

- Once you have generated the parquet files use this script to upload them to the mongodb:
    ```bash
    docker run --network spark_cluster_default --volumes-from spark_cluster-spark-master-1 spark-submit /spark/bin/spark-submit \
    --packages org.mongodb.spark:mongo-spark-connector_2.13:10.4.0 \
    --master spark://<spark_cluster>:7077 \
    --deploy-mode client \
    /opt/spark-apps/save_to_database.py

    ```