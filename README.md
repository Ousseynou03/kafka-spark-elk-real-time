### For python Producer
Before writting script , install package kafka

<code> pip install confluent-kafka </code>


1000 enregistrement par seconde dans un premier temps


Pour spark processor , il faut d'abord commencer par installer pyspark




commande pour spark-submit

<code>docker exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/opt/spark/.ivy2/cache \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/work-dir/jobs/spark_processor.py
</code>