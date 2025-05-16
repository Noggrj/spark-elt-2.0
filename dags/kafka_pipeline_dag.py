from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Argumentos padrão para o DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Definição do DAG
dag = DAG(
    'kafka_pipeline',
    default_args=default_args,
    description='Pipeline ELT com Kafka e Spark',
    schedule_interval=None,  # Define como None para permitir apenas execuções manuais
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['kafka', 'spark', 'elt'],
    max_active_runs=10,  # Permite até 10 execuções simultâneas
    is_paused_upon_creation=True  # Adicione esta linha para pausar o DAG por padrão
)


# Tarefa para verificar a disponibilidade do Kafka
verificar_kafka = BashOperator(
    task_id='verificar_kafka',
    bash_command='kafka-topics.sh --list --bootstrap-server kafka:29092 || exit 1',
    dag=dag,
)

# Tarefa para criar tópicos Kafka se não existirem
criar_topicos = BashOperator(
    task_id='criar_topicos',
    bash_command='''
    kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:29092 --replication-factor 1 --partitions 1 --topic clientes
    kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:29092 --replication-factor 1 --partitions 1 --topic transacoes
    ''',
    dag=dag,
)

# Tarefa para gerar e publicar dados no Kafka
gerar_dados = BashOperator(
    task_id='gerar_dados',
    bash_command='cd /home/airflow/project && python -m generator.data_generator --bootstrap-servers kafka:29092 --max-iteracoes 1000',
    dag=dag,
)

# Tarefa para processar dados com Spark
processar_dados = BashOperator(
    task_id='processar_dados',
    bash_command='cd /home/airflow/project && python -c "from pipeline.kafka_pipeline import consumir_kafka_spark; consumir_kafka_spark(\'kafka:29092\')"',
    dag=dag,
)

# Definir a ordem de execução das tarefas
verificar_kafka >> criar_topicos >> gerar_dados >> processar_dados