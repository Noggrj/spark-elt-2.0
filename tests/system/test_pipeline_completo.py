import unittest
import os
import time
import shutil
import subprocess
from unittest.mock import patch

class TestPipelineCompleto(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Configuração inicial para os testes de sistema"""
        # Diretório de saída para os testes
        cls.output_dir = './test_system_output'
        os.makedirs(cls.output_dir, exist_ok=True)
    
    @classmethod
    def tearDownClass(cls):
        """Limpeza após os testes"""
        # Remove o diretório de saída
        if os.path.exists(cls.output_dir):
            shutil.rmtree(cls.output_dir)
    
    @unittest.skipIf(os.environ.get('SKIP_DOCKER_TESTS') == 'true', 
                    "Pulando testes que dependem do Docker")
    def test_pipeline_docker(self):
        """Testa a execução do pipeline completo via Docker"""
        # Verifica se o Docker está disponível
        try:
            subprocess.run(['docker', '--version'], 
                          check=True, 
                          stdout=subprocess.PIPE, 
                          stderr=subprocess.PIPE)
        except (subprocess.SubprocessError, FileNotFoundError):
            self.skipTest("Docker não está disponível")
        
        # Executa o comando para iniciar os serviços
        try:
            # Inicia apenas a infraestrutura
            subprocess.run(['make', 'infra-start'], 
                          check=True, 
                          stdout=subprocess.PIPE, 
                          stderr=subprocess.PIPE)
            
            # Aguarda os serviços iniciarem
            time.sleep(30)
            
            # Executa o pipeline em modo de teste
            result = subprocess.run(['python', 'main.py', 
                                    '--mode', 'both', 
                                    '--bootstrap-servers', 'localhost:9092',
                                    '--output-dir', self.output_dir],
                                   check=True,
                                   timeout=60,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
            
            # Verifica se o comando foi executado com sucesso
            self.assertEqual(result.returncode, 0)
            
            # Verifica se os diretórios de saída foram criados
            self.assertTrue(os.path.exists(os.path.join(self.output_dir, 'clientes')))
            self.assertTrue(os.path.exists(os.path.join(self.output_dir, 'transacoes')))
            
        finally:
            # Para os serviços
            subprocess.run(['make', 'infra-stop'], 
                          stdout=subprocess.PIPE, 
                          stderr=subprocess.PIPE)
    
    @patch('pipeline.kafka_pipeline.executar_pipeline_kafka')
    def test_main_script(self, mock_executar):
        """Testa o script principal"""
        import main
        
        # Executa o script principal com argumentos de teste
        with patch('sys.argv', ['main.py', '--mode', 'both', 
                               '--bootstrap-servers', 'test:9092',
                               '--output-dir', self.output_dir]):
            main.main()
        
        # Verifica se a função do pipeline foi chamada com os argumentos corretos
        mock_executar.assert_called_once_with('both', 'test:9092', output_dir=self.output_dir)

if __name__ == '__main__':
    unittest.main()