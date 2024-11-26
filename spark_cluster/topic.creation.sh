docker exec -it 09ed979efef4 \
/opt/kafka/bin/kafka-topics.sh \
--create --zookeeper zookeeper:2181 \
--replication-factor 1 --partitions 1 \
--topic gps-topic-location