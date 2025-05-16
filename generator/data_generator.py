import json
import random
import time
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer
import pandas as pd
import os

# Constantes para geração de dados
CATEGORIAS = ["Alimentação", "Transporte", "Lazer", "Saúde", "Educação", "Vestuário", "Eletrônicos"]
CIDADES = ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Salvador", "Brasília", "Curitiba", "Recife", "Fortaleza", "Porto Alegre", "Manaus"]
ESTADOS = ["SP", "RJ", "MG", "BA", "DF", "PR", "PE", "CE", "RS", "AM"]

def carregar_clientes():
    """
    Carrega os dados de clientes do CSV ou cria novos se o arquivo não existir.
    """
    arquivo_clientes = os.path.join('data', 'clientes.csv')
    
    if os.path.exists(arquivo_clientes):
        print(f"📂 Carregando clientes do arquivo {arquivo_clientes}")
        df_clientes = pd.read_csv(arquivo_clientes)
        return df_clientes.to_dict('records')
    else:
        print(f"⚠️ Arquivo {arquivo_clientes} não encontrado. Gerando clientes aleatórios.")
        return gerar_clientes(100)  # Gera 100 clientes aleatórios

def gerar_clientes(quantidade):
    """
    Gera uma lista de clientes aleatórios.
    """
    clientes = []
    for i in range(quantidade):
        cliente = {
            "id_cliente": f"CLI{i+1:04d}",
            "nome": f"Cliente {i+1}",
            "idade": random.randint(18, 80),
            "email": f"cliente{i+1}@email.com",
            "estado": random.choice(ESTADOS)
        }
        clientes.append(cliente)
    return clientes

def gerar_transacao(clientes):
    """
    Gera uma transação aleatória para um cliente existente.
    """
    cliente = random.choice(clientes)
    
    # Data aleatória nos últimos 30 dias
    data = datetime.now() - timedelta(days=random.randint(0, 30))
    data_str = data.strftime("%Y-%m-%d")
    
    transacao = {
        "id_transacao": str(uuid.uuid4()),
        "id_cliente": cliente["id_cliente"],
        "valor": round(random.uniform(10.0, 1000.0), 2),
        "data": data_str,
        "categoria": random.choice(CATEGORIAS),
        "cidade": random.choice(CIDADES)
    }
    return transacao

def publicar_no_kafka(producer, topico, dados):
    """
    Publica dados em um tópico Kafka.
    """
    try:
        producer.send(topico, json.dumps(dados).encode('utf-8'))
        producer.flush()
        return True
    except Exception as e:
        print(f"❌ Erro ao publicar no Kafka: {str(e)}")
        return False

def gerar_e_publicar_dados(bootstrap_servers='localhost:9092', intervalo=1.0, max_iteracoes=None):
    """
    Gera e publica dados continuamente no Kafka.
    
    Parâmetros:
    - bootstrap_servers: endereço dos servidores Kafka
    - intervalo: tempo em segundos entre cada publicação
    - max_iteracoes: número máximo de iterações (None para infinito)
    """
    print(f"🔹 Iniciando gerador de dados (Kafka: {bootstrap_servers})")
    
    # Carregar clientes
    clientes = carregar_clientes()
    print(f"✅ {len(clientes)} clientes carregados")
    
    # Configurar produtor Kafka
    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        print(f"✅ Conexão com Kafka estabelecida")
    except Exception as e:
        print(f"❌ Erro ao conectar ao Kafka: {str(e)}")
        return
    
    # Publicar clientes no Kafka (uma vez só)
    print(f"🔹 Publicando {len(clientes)} clientes no Kafka...")
    for cliente in clientes:
        publicar_no_kafka(producer, 'clientes', cliente)
    print(f"✅ Clientes publicados com sucesso")
    
    # Loop de geração e publicação de transações
    iteracao = 0
    try:
        while max_iteracoes is None or iteracao < max_iteracoes:
            # Gerar transação aleatória
            transacao = gerar_transacao(clientes)
            
            # Publicar no Kafka
            sucesso = publicar_no_kafka(producer, 'transacoes', transacao)
            
            if sucesso:
                print(f"✅ Transação publicada: {transacao['id_cliente']} - R$ {transacao['valor']} - {transacao['categoria']}")
            
            # Incrementar contador
            iteracao += 1
            
            # Aguardar intervalo
            time.sleep(intervalo)
            
    except KeyboardInterrupt:
        print("⚠️ Geração de dados interrompida pelo usuário")
    finally:
        producer.close()
        print("✅ Conexão com Kafka fechada")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Gerador de dados para Kafka')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                      help='Endereço dos servidores Kafka (padrão: localhost:9092)')
    parser.add_argument('--intervalo', type=float, default=1.0,
                      help='Intervalo em segundos entre cada publicação (padrão: 1.0)')
    parser.add_argument('--max-iteracoes', type=int, default=None,
                      help='Número máximo de iterações (padrão: infinito)')
    
    args = parser.parse_args()
    
    gerar_e_publicar_dados(
        bootstrap_servers=args.bootstrap_servers,
        intervalo=args.intervalo,
        max_iteracoes=args.max_iteracoes
    )