import unittest
from unittest.mock import patch, MagicMock
import json
from generator.data_generator import gerar_cliente, gerar_transacao

class TestDataGenerator(unittest.TestCase):
    
    def test_gerar_cliente(self):
        """Testa se a função gerar_cliente retorna um dicionário com as chaves esperadas"""
        cliente = gerar_cliente(1)
        
        # Verifica se o resultado é um dicionário
        self.assertIsInstance(cliente, dict)
        
        # Verifica se todas as chaves esperadas estão presentes
        chaves_esperadas = ['id', 'nome', 'email', 'telefone', 'endereco', 'data_cadastro']
        for chave in chaves_esperadas:
            self.assertIn(chave, cliente)
        
        # Verifica se o ID está correto
        self.assertEqual(cliente['id'], 1)
    
    def test_gerar_transacao(self):
        """Testa se a função gerar_transacao retorna um dicionário com as chaves esperadas"""
        transacao = gerar_transacao(1, 100)
        
        # Verifica se o resultado é um dicionário
        self.assertIsInstance(transacao, dict)
        
        # Verifica se todas as chaves esperadas estão presentes
        chaves_esperadas = ['id', 'cliente_id', 'valor', 'data', 'tipo', 'status']
        for chave in chaves_esperadas:
            self.assertIn(chave, transacao)
        
        # Verifica se o ID está correto
        self.assertEqual(transacao['id'], 1)
        
        # Verifica se o cliente_id está dentro do intervalo esperado
        self.assertLessEqual(transacao['cliente_id'], 100)
        self.assertGreaterEqual(transacao['cliente_id'], 1)
    
    @patch('generator.data_generator.KafkaProducer')
    def test_gerar_e_publicar_dados(self, mock_producer):
        """Testa a função de geração e publicação de dados no Kafka"""
        from generator.data_generator import gerar_e_publicar_dados
        
        # Configura o mock do produtor Kafka
        mock_instance = MagicMock()
        mock_producer.return_value = mock_instance
        
        # Executa a função com parâmetros de teste
        gerar_e_publicar_dados(bootstrap_servers='localhost:9092', max_iteracoes=2)
        
        # Verifica se o produtor foi criado com os parâmetros corretos
        mock_producer.assert_called_once_with(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Verifica se o método send foi chamado pelo menos 4 vezes (2 clientes + 2 transações)
        self.assertGreaterEqual(mock_instance.send.call_count, 4)
        
        # Verifica se o método flush foi chamado
        mock_instance.flush.assert_called_once()

if __name__ == '__main__':
    unittest.main()