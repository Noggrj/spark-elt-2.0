import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def executar_pipeline_kafka(mode, bootstrap_servers, output_dir='./data/parquet'):
    """
    Executa o pipeline Kafka-Spark para processamento de dados.
    
    Args:
        mode (str): Modo de execução ('produce', 'consume', 'both')
        bootstrap_servers (str): Endereço dos servidores Kafka
        output_dir (str): Diretório para salvar os arquivos Parquet
    """
    if mode in ['consume', 'both']:
        print(f"🔄 Iniciando consumo de dados do Kafka ({bootstrap_servers})")
        consumir_e_persistir_dados(bootstrap_servers, output_dir)
    
    if mode in ['produce', 'both']:
        print(f"📤 Iniciando produção de dados para o Kafka ({bootstrap_servers})")
        from generator.data_generator import gerar_e_publicar_dados
        gerar_e_publicar_dados(bootstrap_servers=bootstrap_servers, max_iteracoes=10)

def consumir_e_persistir_dados(bootstrap_servers, output_dir):
    """
    Consome dados do Kafka e os persiste em formato Parquet.
    
    Args:
        bootstrap_servers (str): Endereço dos servidores Kafka
        output_dir (str): Diretório para salvar os arquivos Parquet
    """
    # Criar sessão Spark
    spark = SparkSession.builder \
        .appName("KafkaParquetPipeline") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()
    
    # Definir esquemas para os diferentes tópicos
    schema_clientes = StructType([
        StructField("id", IntegerType(), True),
        StructField("nome", StringType(), True),
        StructField("email", StringType(), True),
        StructField("telefone", StringType(), True),
        StructField("endereco", StringType(), True),
        StructField("data_cadastro", TimestampType(), True)
    ])
    
    schema_transacoes = StructType([
        StructField("id", IntegerType(), True),
        StructField("cliente_id", IntegerType(), True),
        StructField("valor", DoubleType(), True),
        StructField("data", TimestampType(), True),
        StructField("tipo", StringType(), True),
        StructField("status", StringType(), True)
    ])
    
    # Configurar diretórios de saída específicos
    output_clientes = os.path.join(output_dir, "clientes")
    output_transacoes = os.path.join(output_dir, "transacoes")
    
    # Garantir que os diretórios existam
    os.makedirs(output_clientes, exist_ok=True)
    os.makedirs(output_transacoes, exist_ok=True)
    
    try:
        # Consumir dados do tópico de clientes
        df_clientes = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", "clientes") \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema_clientes).alias("data")) \
            .select("data.*")
        
        # Persistir stream de clientes em Parquet
        query_clientes = df_clientes.writeStream \
            .format("parquet") \
            .option("path", output_clientes) \
            .option("checkpointLocation", os.path.join(output_clientes, "_checkpoint")) \
            .partitionBy("data_cadastro") \
            .trigger(processingTime="10 seconds") \
            .start()
        
        # Consumir dados do tópico de transações
        df_transacoes = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", "transacoes") \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema_transacoes).alias("data")) \
            .select("data.*")
        
        # Persistir stream de transações em Parquet
        query_transacoes = df_transacoes.writeStream \
            .format("parquet") \
            .option("path", output_transacoes) \
            .option("checkpointLocation", os.path.join(output_transacoes, "_checkpoint")) \
            .partitionBy("data") \
            .trigger(processingTime="10 seconds") \
            .start()
        
        print(f"✅ Pipeline de consumo iniciado. Dados serão salvos em {output_dir}")
        print("📊 Pressione Ctrl+C para interromper o processamento")
        
        # Manter as queries ativas
        query_clientes.awaitTermination()
        query_transacoes.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n⛔ Interrompendo o processamento...")
        spark.stop()
        print("✅ Processamento finalizado")
    except Exception as e:
        print(f"❌ Erro durante o processamento: {str(e)}")
        spark.stop()