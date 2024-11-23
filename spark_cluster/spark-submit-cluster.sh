#!/bin/bash

#docker run -d --name spark_submit_container --network spark_cluster_default --volumes-from spark_cluster-spark-master-1 -p 4041:4040 spark-submit /bin/bash -c "sleep infinity"

docker exec -it spark_submit_container /spark/bin/spark-submit \
--master spark://e3aec324799c:7077 \
--deploy-mode client \
--packages org.mongodb.spark:mongo-spark-connector 2.13:10.4.0 \
/opt/spark-apps/save_to_database.py 

