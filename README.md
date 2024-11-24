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
