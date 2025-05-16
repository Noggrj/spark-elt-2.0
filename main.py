import argparse
import os
import threading
from pipeline.kafka_pipeline import executar_pipeline_kafka
from generator.data_generator import gerar_e_publicar_dados

def main():
    """
    Função principal que serve como ponto de entrada para o pipeline ELT.
    Suporta modos de execução para produção, consumo ou ambos.
    """
    parser = argparse.ArgumentParser(description='Pipeline ELT com Kafka e Spark')
    parser.add_argument('--mode', choices=['produce', 'consume', 'both', 'continuous'], default='both',
                      help='Modo de execução: produce (apenas produzir), consume (apenas consumir), both (ambos), continuous (geração contínua)')
    parser.add_argument('--bootstrap-servers', default=os.environ.get('BOOTSTRAP_SERVERS', 'kafka:29092'),
                      help='Endereço dos servidores Kafka (padrão: kafka:29092)')
    parser.add_argument('--intervalo', type=float, default=1.0,
                      help='Intervalo em segundos entre cada publicação no modo continuous (padrão: 1.0)')
    parser.add_argument('--output-dir', default='./data/parquet',
                      help='Diretório para salvar os arquivos Parquet (padrão: ./data/parquet)')
    
    args = parser.parse_args()
    
    # Garantir que o diretório de saída exista
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Modo de geração contínua
    if args.mode == 'continuous':
        print("🔄 Iniciando modo de geração contínua de dados")
        
        # Iniciar thread para geração de dados
        thread_gerador = threading.Thread(
            target=gerar_e_publicar_dados,
            kwargs={
                'bootstrap_servers': args.bootstrap_servers,
                'intervalo': args.intervalo,
                'max_iteracoes': None  # Infinito
            }
        )
        thread_gerador.daemon = True
        thread_gerador.start()
        
        # Iniciar consumo dos dados em paralelo
        executar_pipeline_kafka('consume', args.bootstrap_servers, output_dir=args.output_dir)
        
    else:
        # Modos tradicionais
        executar_pipeline_kafka(args.mode, args.bootstrap_servers, output_dir=args.output_dir)

if __name__ == "__main__":
    main()