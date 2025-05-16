import unittest
import json
import time
from kafka import KafkaProducer, KafkaConsumer
from generator.data_generator import gerar_cliente, gerar_transacao

class TestKafkaIntegration(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Configuração inicial para os testes de integração com Kafka"""
        # Configuração do produtor Kafka
        cls.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Tópicos de teste
        cls.topico_clientes = 'test_clientes'
        cls.topico_transacoes = 'test_transacoes'
    
    @classmethod
    def tearDownClass(cls):
        """Limpeza após os testes"""
        if hasattr(cls, 'producer'):
            cls.producer.close()
    
    def test_producao_consumo_kafka(self):
        """Testa a produção e consumo de mensagens no Kafka"""
        # Pula o teste se o Kafka não estiver disponível
        try:
            # Verifica se o Kafka está acessível
            self.producer.send('test_topic', {'test': 'message'}).get(timeout=5)
        except Exception as e:
            self.skipTest(f"Kafka não está disponível: {str(e)}")
        
        # Gera dados de teste
        cliente = gerar_cliente(1)
        transacao = gerar_transacao(1, 1)
        
        # Publica no Kafka
        self.producer.send(self.topico_clientes, cliente)
        self.producer.send(self.topico_transacoes, transacao)
        self.producer.flush()
        
        # Configura consumidor para o tópico de clientes
        consumer_clientes = KafkaConsumer(
            self.topico_clientes,
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=5000
        )
        
        # Configura consumidor para o tópico de transações
        consumer_transacoes = KafkaConsumer(
            self.topico_transacoes,
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=5000
        )
        
        # Consome mensagens
        mensagens_clientes = list(consumer_clientes)
        mensagens_transacoes = list(consumer_transacoes)
        
        # Fecha os consumidores
        consumer_clientes.close()
        consumer_transacoes.close()
        
        # Verifica se as mensagens foram recebidas
        self.assertGreaterEqual(len(mensagens_clientes), 1)
        self.assertGreaterEqual(len(mensagens_transacoes), 1)
        
        # Verifica o conteúdo da última mensagem
        ultimo_cliente = mensagens_clientes[-1].value
        ultima_transacao = mensagens_transacoes[-1].value
        
        self.assertEqual(ultimo_cliente['id'], cliente['id'])
        self.assertEqual(ultima_transacao['id'], transacao['id'])

if __name__ == '__main__':
    unittest.main()