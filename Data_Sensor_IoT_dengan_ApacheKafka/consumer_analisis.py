# Pengumpulan Data Sensor IoT dengan Apache Kafka -Output dan Analisis: Cetak data yang suhu-nya melebihi 80°C
# Sylvia Febrianti - 5027221019
# Khansa Adia Rahma - 5027221071

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os

def create_spark_session():
    os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home'
    
    return SparkSession.builder \
        .appName("SensorTemperatureMonitoring") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .getOrCreate()

def create_sensor_schema():
    return StructType([
        StructField("sensor_id", StringType(), True),
        StructField("suhu", DoubleType(), True),
        StructField("lokasi", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

def process_sensor_data(spark):
    schema = create_sensor_schema()
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sensor-suhu") \
        .load()
    
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    high_temp_df = parsed_df.filter(col("suhu") > 80)
    
    def process_batch(batch_df, batch_id):
        if batch_df.count() > 0:  
            print("\n=== PERINGATAN SUHU TINGGI ===")
            batch_df.show(truncate=False)
            print("============================")
    
    query = high_temp_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()
    
    return query

def main():
    spark = create_spark_session()
    print("PySpark Consumer Started...")
    
    try:
        query = process_sensor_data(spark)
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nPySpark Consumer dihentikan")
        spark.stop()

if __name__ == "__main__":
    main()
