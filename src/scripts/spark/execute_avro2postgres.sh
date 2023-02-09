docker exec -t -i src-spark-master-1 /bin/bash /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
--jars /opt/workspace/spark-apps/packages/postgresql-42.2.22.jar \
--packages org.apache.spark:spark-avro_2.12:3.0.2 \
--driver-memory 1G \
--executor-memory 1G \
/opt/workspace/spark-apps/avro2postgres.py
