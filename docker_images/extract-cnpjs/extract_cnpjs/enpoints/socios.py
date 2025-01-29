from .utils import Utils
import os
import singer

# Inicializa o logger para registrar informações durante a execução
LOGGER = singer.get_logger()

class GetSoc(Utils):
    """
    Classe responsável por extrair e enviar arquivos de empresas para o S3.
    
    A classe faz requisições a um endpoint baseado em uma data de início (`start_date`), baixa os arquivos compactados, descompacta, e os envia para o bucket S3 configurado.

    Herda a classe `Utils` para reutilizar métodos comuns de requisição e upload.

    Attributes:
        config (dict): Dicionário contendo as configurações para a execução do processo.
        start_date (str): Data de início para formar o endpoint de consulta.
        bucket_name (str): Nome do bucket S3 para onde os arquivos serão enviados.
        s3_directory (str): Diretório dentro do bucket S3 onde os arquivos serão armazenados.
        tap_stream_id (str): ID do stream utilizado no processo de ingestão.
    """
    
    def __init__(self, config) -> None:
        """
        Inicializa a classe com as configurações fornecidas.
        
        Args:
            config (dict): Configurações necessárias para o processo de extração e upload.
        """
        super().__init__(config)  # Chama o construtor da classe base 'Utils'
        
        # Atribui os parâmetros recebidos no 'config'
        self.config = config
        self.start_date = config.get("start_date")
        LOGGER.info(f"START >> {self.start_date}")
        self.bucket_name = config.get("bucket_name")
        self.s3_directory = config.get("s3_directory")
        self.tap_stream_id = config.get("tap_stream_id")
        
    def sync(self):
        """
        Sincroniza os arquivos de empresas: 
        - Faz requisição ao endpoint para buscar o arquivo compactado.
        - Descompacta o arquivo.
        - Faz o upload para o S3.
        
        O processo é repetido para cada página de dados encontrada.
        """
        page = 1
        
        # Formata o endpoint com a data de início e número da página
        endpoint = f"{self.start_date}/Socios{page}.zip"
                
        # Passo 1: Fazer a requisição para o endpoint
        response = self.do_request(endpoint)
        
        # Passo 2: Descompactar o arquivo recebido
        extract_dir = './extracted_files'
        os.makedirs(extract_dir, exist_ok=True)  # Cria o diretório de extração se não existir
        self.extract_zip(response, extract_to=extract_dir)  # Descompacta o arquivo
        
        # Passo 3: Enviar os arquivos extraídos para o S3
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                # Caminho local do arquivo descompactado
                local_file_path = os.path.join(root, file)
                LOGGER.info(f"Arquivo extraído: {local_file_path}")
                
                # Nome do blob e chave no S3
                blob_name = f"{self.s3_directory}"
                s3_key = os.path.join(blob_name, f"{self.tap_stream_id}{page}.csv")
                
                # Envia o arquivo para o S3
                self.upload_to_s3(local_file_path, self.bucket_name, s3_key)
        
                # Passo 4: Excluir arquivo temporário local após upload
                if local_file_path and os.path.exists(local_file_path):
                    os.remove(local_file_path)
                    LOGGER.info(f"Arquivo temporário {local_file_path} excluído com sucesso.")
