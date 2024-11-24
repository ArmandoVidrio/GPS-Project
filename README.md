# GPS-Project
Big data class final project

## Prerequisites
- You need to have python installed; You can install python following the oficial python [documentation](https://www.python.org/downloads/)

- You need to have docker installed; If you don't have docker install you can follow the oficial [documentation](https://docs.docker.com/desktop/)

- You need to have pyspark installed; You can install it with the command: 
`pip install pyspark` 

## Spark cluster
- Create the spark cluster with the command (you need to be inside the spark_cluster folder): 
`docker compose up --scale spark-worker=3`

- Create the spark submit container with the command: 
`docker run -d --name spark_submit_container --network spark_cluster_default --volumes-from spark_cluster-spark-master-1 -p 4041:4040 spark-submit /bin/bash -c "sleep infinity"`

## Kafka cluster
- Create the kafka cluster (you need to be inside the kafka_cluster directory): 
`docker compose up -d`

- Create topics for each producer, in this case we have 3 producers:
- ``` docker exec -it <kafka_cluster> \
            /opt/kafka/bin/kafka-topics.sh \
            --create --zookeeper zookeeper:2181 \
            --replication-factor 1 --partitions 1 \
            --topic gps-topic-location
    ```
- ``` docker exec -it <kafka_cluster> \
            /opt/kafka/bin/kafka-topics.sh \
            --create --zookeeper zookeeper:2181 \
            --replication-factor 1 --partitions 1 \
            --topic gps-topic-location
    ```
- ``` docker exec -it <kafka_cluster> \
            /opt/kafka/bin/kafka-topics.sh \
            --create --zookeeper zookeeper:2181 \
            --replication-factor 1 --partitions 1 \
            --topic gps-topic-location3
    ```


