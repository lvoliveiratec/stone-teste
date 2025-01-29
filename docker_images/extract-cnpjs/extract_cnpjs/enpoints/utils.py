import os
import requests
import json
import zipfile
import boto3
import csv
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from io import BytesIO
import singer
from abc import ABC

# Define o tempo limite padrão de conexão
DEFAULT_CONNECTION_TIMEOUT = 240

# Inicializa o logger para registrar informações durante a execução
LOGGER = singer.get_logger()


class Utils(ABC):
    def __init__(self, config) -> None:
        """
        Inicializa a classe com a URL base e a configuração do processo.
        """
        self.base_url = (
            "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
        )
        self.config = config
        
    def raise_if_not_200(self, response):
        """
        Verifica se o status code da resposta é 200 (OK).
        Se não for, lança uma exceção.
        """
        if response.status_code != 200:
            message = f"Erro {response.status_code}: {response.text}"
            raise Exception(message)
        else:
            return BytesIO(response.content)

    def do_request(self, endpoint, session=requests.Session()) -> requests.Response:
        """
        Realiza uma requisição GET para o endpoint especificado.
        Utiliza um mecanismo de repetição em caso de falhas.
        """
        url = self.base_url + endpoint
        LOGGER.info(f"Realizando requisição para: {url}")
        
        # Faz a requisição com retries configurados
        raw_resp = self.requests_retry_session(session=session).get(
            url,
            timeout=DEFAULT_CONNECTION_TIMEOUT,
        )
        
        # Verifica se a resposta foi bem-sucedida
        response = self.raise_if_not_200(raw_resp)
        
        return response

    def requests_retry_session(self, retries=3, backoff_factor=2, status_forcelist=(500, 502, 504), session=None):
        """
        Configura o mecanismo de repetição para requisições HTTP em caso de falhas temporárias.
        """
        session = session or requests.Session()
        
        # Configura as tentativas e o fator de backoff para re-tentativas
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def extract_zip(self, zip_file, extract_to='./'):
        """
        Descompacta um arquivo ZIP para o diretório especificado.
        """
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            LOGGER.info(f"Arquivos descompactados para: {extract_to}")
        except Exception as e:
            LOGGER.error(f"Erro ao descompactar o arquivo {zip_file}: {str(e)}")
            raise

    def convert_csv_to_json(self, csv_file_path, json_file_path, fieldnames):
        """
        Converte um arquivo CSV para JSON, usando uma lista de fieldnames.
        Remove o arquivo CSV após a conversão e gera o arquivo JSON.
        """
        try:
            # Lê o CSV e converte para uma lista de dicionários
            with open(csv_file_path, mode='r', encoding='ISO-8859-1') as csv_file:
                reader = csv.DictReader(csv_file, fieldnames=fieldnames, delimiter=';')  # Usando ";" como delimitador
                data = [row for row in reader]
            
            # Remove o arquivo CSV após a conversão
            os.remove(csv_file_path)
            LOGGER.info(f"Arquivo temporário CSV {csv_file_path} excluído com sucesso.")
        
            # Salva os dados como arquivo JSON
            with open(json_file_path, 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)
            LOGGER.info(f"Arquivo JSON gerado em: {json_file_path}")
        
        except Exception as e:
            LOGGER.error(f"Erro ao converter CSV para JSON: {str(e)}")
            raise

    def upload_to_s3(self, local_file, bucket_name, s3_key):
        """
        Envia um arquivo local para um bucket S3.
        """
        try:
            s3_client = boto3.client('s3')
            s3_client.upload_file(local_file, bucket_name, s3_key)
            LOGGER.info(f"Arquivo {local_file} enviado para o S3 com sucesso!")
        
        except Exception as e:
            LOGGER.error(f"Erro ao enviar arquivo {local_file} para o S3: {str(e)}")
            raise
