#!/bin/bash

docker exec -it kafka_producer python3 /opt/spark-apps/kafka_producer/kafka_gps_producer.py --kafka-bootstrap 09ed979efef4 --kafka-topic gps-topic-location

