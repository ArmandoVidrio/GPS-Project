import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, explode, split, from_json, col
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType, StructField, IntegerType

estados_mexico = {
        "Aguascalientes": {"lat_min": 21.7, "lat_max": 22.2, "lon_min": -103.6, "lon_max": -102.8},
        "Baja California": {"lat_min": 32.0, "lat_max": 32.8, "lon_min": -116.7, "lon_max": -114.8},
        "Baja California Sur": {"lat_min": 24.0, "lat_max": 26.0, "lon_min": -114.7, "lon_max": -112.5},
        "Campeche": {"lat_min": 18.5, "lat_max": 19.9, "lon_min": -90.5, "lon_max": -89.0},
        "Chiapas": {"lat_min": 14.5, "lat_max": 17.5, "lon_min": -93.0, "lon_max": -88.5},
        "Chihuahua": {"lat_min": 27.5, "lat_max": 30.0, "lon_min": -107.5, "lon_max": -104.5},
        "Coahuila": {"lat_min": 25.0, "lat_max": 27.5, "lon_min": -104.0, "lon_max": -101.0},
        "Colima": {"lat_min": 18.5, "lat_max": 19.5, "lon_min": -103.9, "lon_max": -102.8},
        "Durango": {"lat_min": 23.0, "lat_max": 26.5, "lon_min": -106.5, "lon_max": -103.5},
        "Guanajuato": {"lat_min": 20.5, "lat_max": 21.5, "lon_min": -101.5, "lon_max": -100.0},
        "Guerrero": {"lat_min": 16.5, "lat_max": 18.5, "lon_min": -101.0, "lon_max": -98.5},
        "Hidalgo": {"lat_min": 19.5, "lat_max": 21.0, "lon_min": -99.5, "lon_max": -98.0},
        "Jalisco": {"lat_min": 19.0, "lat_max": 21.5, "lon_min": -104.5, "lon_max": -101.5},
        "Mexico": {"lat_min": 19.0, "lat_max": 20.5, "lon_min": -99.5, "lon_max": -98.0},
        "Michoacán": {"lat_min": 18.0, "lat_max": 20.5, "lon_min": -102.0, "lon_max": -100.0},
        "Morelos": {"lat_min": 18.5, "lat_max": 19.5, "lon_min": -99.5, "lon_max": -98.0},
        "Nayarit": {"lat_min": 21.5, "lat_max": 22.5, "lon_min": -105.5, "lon_max": -104.0},
        "Nuevo Leon": {"lat_min": 25.5, "lat_max": 26.5, "lon_min": -100.5, "lon_max": -99.0},
        "Oaxaca": {"lat_min": 16.0, "lat_max": 18.5, "lon_min": -98.5, "lon_max": -94.5},
        "Puebla": {"lat_min": 18.5, "lat_max": 20.0, "lon_min": -98.5, "lon_max": -97.0},
        "Querétaro": {"lat_min": 20.5, "lat_max": 21.5, "lon_min": -100.5, "lon_max": -99.5},
        "Quintana Roo": {"lat_min": 18.0, "lat_max": 21.0, "lon_min": -89.5, "lon_max": -87.5},
        "San Luis Potosí": {"lat_min": 22.5, "lat_max": 24.0, "lon_min": -101.5, "lon_max": -98.5},
        "Sinaloa": {"lat_min": 22.0, "lat_max": 25.0, "lon_min": -109.0, "lon_max": -106.0},
        "Sonora": {"lat_min": 28.5, "lat_max": 31.5, "lon_min": -113.0, "lon_max": -109.0},
        "Tabasco": {"lat_min": 17.0, "lat_max": 18.5, "lon_min": -94.5, "lon_max": -91.5},
        "Tamaulipas": {"lat_min": 24.0, "lat_max": 26.5, "lon_min": -98.5, "lon_max": -95.5},
        "Tlaxcala": {"lat_min": 19.0, "lat_max": 19.5, "lon_min": -98.5, "lon_max": -97.5},
        "Veracruz": {"lat_min": 18.0, "lat_max": 21.5, "lon_min": -97.5, "lon_max": -94.5},
        "Yucatán": {"lat_min": 20.0, "lat_max": 21.5, "lon_min": -89.5, "lon_max": -87.0},
        "Zacatecas": {"lat_min": 22.0, "lat_max": 23.5, "lon_min": -104.5, "lon_max": -101.0},
        "CDMX": {"lat_min": 19.2, "lat_max": 19.5, "lon_min": -99.3, "lon_max": -99.1}
    }
capitales_mexico = {
    "Aguascalientes": {"lat": 21.8818, "lon": -102.2916},
    "Mexicali": {"lat": 32.6258, "lon": -115.4586},
    "La Paz": {"lat": 24.1425, "lon": -110.3128},
    "Campeche": {"lat": 19.8506, "lon": -90.5341},
    "Tuxtla Gutiérrez": {"lat": 16.7569, "lon": -93.1296},
    "Chihuahua": {"lat": 28.6321, "lon": -106.0694},
    "Saltillo": {"lat": 25.4383, "lon": -100.9737},
    "Colima": {"lat": 19.2417, "lon": -103.7250},
    "Durango": {"lat": 24.0277, "lon": -104.6662},
    "Guanajuato": {"lat": 21.0190, "lon": -101.2574},
    "Chilpancingo": {"lat": 17.5802, "lon": -99.5431},
    "Pachuca": {"lat": 20.0109, "lon": -98.7387},
    "Guadalajara": {"lat": 20.6597, "lon": -103.3496},
    "Toluca": {"lat": 19.1738, "lon": -99.2111},
    "Morelia": {"lat": 19.7007, "lon": -101.1910},
    "Cuernavaca": {"lat": 18.9248, "lon": -99.2383},
    "Tepic": {"lat": 21.7588, "lon": -104.9377},
    "Monterrey": {"lat": 25.6712, "lon": -100.3097},
    "Oaxaca de Juárez": {"lat": 17.0731, "lon": -96.7266},
    "Puebla": {"lat": 19.0414, "lon": -98.2063},
    "Querétaro": {"lat": 20.5888, "lon": -100.3899},
    "Chetumal": {"lat": 21.1743, "lon": -86.8475},
    "San Luis Potosí": {"lat": 22.1562, "lon": -100.9711},
    "Culiacán Rosales": {"lat": 24.7992, "lon": -107.3884},
    "Hermosillo": {"lat": 29.072967, "lon": -110.955919},
    "Villahermosa": {"lat": 17.9925, "lon": -92.9399},
    "Ciudad Victoria": {"lat": 23.7415, "lon": -99.1417},
    "Tlaxcala": {"lat": 19.3133, "lon": -98.2404},
    "Xalapa": {"lat": 19.1738, "lon": -96.2110},
    "Mérida": {"lat": 21.0155, "lon": -89.1551},
    "Zacatecas": {"lat": 22.7709, "lon": -102.5839}
}

# Función para obtener el estado basado en latitud y longitud
def get_estado(latitude, longitude):
    for estado, coords in estados_mexico.items():
        if coords["lat_min"] <= latitude <= coords["lat_max"] and coords["lon_min"] <= longitude <= coords["lon_max"]:
            return estado
    return "Desconocido"  # Si no se encuentra dentro de ningún estado conocido

def get_capital(lat, lon):
    for estado, coords in capitales_mexico.items():
        if abs(coords["lat"] - lat) < 0.1 and abs(coords["lon"] - lon) < 0.1:
            return estado
    return "Desconocido" 

def consume_kafka_events(kafka_server):
    # Initialize SparkSession
    spark = SparkSession.builder \
                .appName("Structured-Streaming-Sensor-Example") \
                .config("spark.ui.port","4040") \
                .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    # Create DataFrame representing the stream of input studens from file
    kafka_bootstrap_server = "{0}:9093".format(kafka_server)
    print("Establishing connection with {0}".format(kafka_bootstrap_server))

    kafka_df = spark \
        .readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
        .option("subscribe", "gps-topic-location") \
        .option("startingOffsets", 
                "latest") \
        .load()

    # Define the schema of the incoming JSON data
    gps_schema = StructType([StructField("latitude", DoubleType(), True),
                                StructField("longitude", DoubleType(), True),
                                StructField("timestamp", StringType(), True),
                                StructField("speed", DoubleType(), True)
    ])

        # Crea una UDF para mapear la latitud y longitud al estado
    get_estado_udf = udf(get_estado, StringType())
    get_capital_udf = udf(get_capital, StringType())

    # Convierte los datos de Kafka de binario a string
    gps_data_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")

    # Parsear el JSON con el esquema definido
    gps_data_df = gps_data_df.withColumn("gps_location", from_json(gps_data_df.json_value, gps_schema))

    # Añadir la columna "location" usando la UDF
    gps_data_df = gps_data_df.withColumn("state", get_estado_udf(gps_data_df["gps_location.latitude"], gps_data_df["gps_location.longitude"]))
    gps_data_df = gps_data_df.withColumn("capital", get_capital_udf(gps_data_df["gps_location.latitude"], gps_data_df["gps_location.longitude"]))
    
    # Seleccionamos las columnas de interés
    gps_data_df = gps_data_df.select("gps_location.*", "state", "capital")

    # Imprime el esquema para verificar
    gps_data_df.printSchema()

    # Escribe los resultados en consola
    query = gps_data_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Await termination to keep the streaming query running
    query.awaitTermination(20)

    print("stream closed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Kafka arguments")
    parser.add_argument('--kafka-bootstrap', required=True, help="Kafka bootstrap server")
    
    args = parser.parse_args()

    consume_kafka_events(args.kafka_bootstrap)
