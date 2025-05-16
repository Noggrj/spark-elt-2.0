import unittest
from unittest.mock import patch, MagicMock
import os
from pipeline.kafka_pipeline import executar_pipeline_kafka

class TestKafkaPipeline(unittest.TestCase):
    
    @patch('pipeline.kafka_pipeline.consumir_e_persistir_dados')
    @patch('generator.data_generator.gerar_e_publicar_dados')
    def test_executar_pipeline_kafka_modo_both(self, mock_gerar, mock_consumir):
        """Testa se o pipeline executa corretamente no modo 'both'"""
        # Executa a função no modo 'both'
        executar_pipeline_kafka('both', 'localhost:9092', './test_output')
        
        # Verifica se ambas as funções foram chamadas
        mock_consumir.assert_called_once_with('localhost:9092', './test_output')
        mock_gerar.assert_called_once_with(bootstrap_servers='localhost:9092', max_iteracoes=10)
    
    @patch('pipeline.kafka_pipeline.consumir_e_persistir_dados')
    @patch('generator.data_generator.gerar_e_publicar_dados')
    def test_executar_pipeline_kafka_modo_produce(self, mock_gerar, mock_consumir):
        """Testa se o pipeline executa corretamente no modo 'produce'"""
        # Executa a função no modo 'produce'
        executar_pipeline_kafka('produce', 'localhost:9092', './test_output')
        
        # Verifica se apenas a função de geração foi chamada
        mock_gerar.assert_called_once_with(bootstrap_servers='localhost:9092', max_iteracoes=10)
        mock_consumir.assert_not_called()
    
    @patch('pipeline.kafka_pipeline.consumir_e_persistir_dados')
    @patch('generator.data_generator.gerar_e_publicar_dados')
    def test_executar_pipeline_kafka_modo_consume(self, mock_gerar, mock_consumir):
        """Testa se o pipeline executa corretamente no modo 'consume'"""
        # Executa a função no modo 'consume'
        executar_pipeline_kafka('consume', 'localhost:9092', './test_output')
        
        # Verifica se apenas a função de consumo foi chamada
        mock_consumir.assert_called_once_with('localhost:9092', './test_output')
        mock_gerar.assert_not_called()
    
    @patch('pipeline.kafka_pipeline.SparkSession')
    def test_consumir_e_persistir_dados(self, mock_spark):
        """Testa a função de consumo e persistência de dados"""
        from pipeline.kafka_pipeline import consumir_e_persistir_dados
        
        # Configura os mocks para o Spark
        mock_builder = MagicMock()
        mock_spark.builder.return_value = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_session = MagicMock()
        mock_builder.getOrCreate.return_value = mock_session
        
        # Configura o mock para o readStream
        mock_read_stream = MagicMock()
        mock_session.readStream.return_value = mock_read_stream
        mock_read_stream.format.return_value = mock_read_stream
        mock_read_stream.option.return_value = mock_read_stream
        mock_read_stream.load.return_value = mock_read_stream
        mock_read_stream.selectExpr.return_value = mock_read_stream
        mock_read_stream.select.return_value = mock_read_stream
        
        # Configura o mock para o writeStream
        mock_write_stream = MagicMock()
        mock_read_stream.writeStream.return_value = mock_write_stream
        mock_write_stream.format.return_value = mock_write_stream
        mock_write_stream.option.return_value = mock_write_stream
        mock_write_stream.partitionBy.return_value = mock_write_stream
        mock_write_stream.trigger.return_value = mock_write_stream
        
        # Simula uma exceção de teclado para interromper o loop
        mock_write_stream.start.side_effect = KeyboardInterrupt()
        
        # Executa a função
        output_dir = './test_output'
        consumir_e_persistir_dados('localhost:9092', output_dir)
        
        # Verifica se os diretórios foram criados
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'clientes')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'transacoes')))
        
        # Limpa os diretórios de teste
        os.rmdir(os.path.join(output_dir, 'clientes'))
        os.rmdir(os.path.join(output_dir, 'transacoes'))
        os.rmdir(output_dir)

if __name__ == '__main__':
    unittest.main()