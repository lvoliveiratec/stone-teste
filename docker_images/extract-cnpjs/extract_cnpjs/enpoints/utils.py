import os
from typing import List
import requests
import json
import zipfile
import boto3
import csv
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import datetime, timedelta
from io import BytesIO
import singer
LOGGER = singer.get_logger()


from abc import ABC

DEFAULT_CONNECTION_TIMEOUT = 240



class Utils(ABC):
    def __init__(self, config) -> None:
        self.base_url = (
            "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
        )
        self.config = config
        
    # Sobe exceção  
    def raise_if_not_200(self, response):
        if response.status_code != 200:
            message = str(response.status_code) + " " + str(response.text)
            raise Exception(message)
        else:
            return BytesIO(response.content)

    # Faz a requisição 
    def do_request(
        self, endpoint, session=requests.Session()
    ) -> requests.Response:

        url = self.base_url + endpoint
        LOGGER.info("URL= " + url)
        
        raw_resp = self.requests_retry_session(session=session).get(
            url,
            timeout=DEFAULT_CONNECTION_TIMEOUT,
        )
        
        response = self.raise_if_not_200(raw_resp)
        
        return response

    def requests_retry_session(
        self,
        retries=3,
        backoff_factor=2,
        status_forcelist=(500, 502, 504),
        session=None,
    ):
        session = session or requests.Session()
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


    def extract_zip(self,zip_file, extract_to='./'):
        # Descompactar o arquivo ZIP
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        LOGGER.info(f"Arquivos descompactados para: {extract_to}")
        
    def convert_csv_to_json(self,csv_file_path, json_file_path,fieldnames):
        
        # Ler o CSV e converter para um formato de lista de dicionários
        with open(csv_file_path, mode='r', encoding='ISO-8859-1') as csv_file:
            reader = csv.DictReader(csv_file,fieldnames=fieldnames, delimiter=';')  # Usando ";" como delimitador
            data = [row for row in reader]
            os.remove(csv_file_path)
            LOGGER.info(f"Arquivo temporário CSV {csv_file_path} excluído com sucesso.")
        
        # Salvar como arquivo JSON
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
        print(f"Arquivo JSON gerado em: {json_file_path}")
    
    # def extract_zip(self, zip_file, extract_to='./'):
    #     if not os.path.exists(zip_file):
    #         raise FileNotFoundError(f"Arquivo ZIP {zip_file} não encontrado.")
        
    #     try:
    #         LOGGER.info("TOAQUII")
    #         with zipfile.ZipFile(zip_file, 'r') as zip_ref:
    #             zip_ref.extractall(extract_to)
    #             extracted_files = zip_ref.namelist()  # Lista dos arquivos extraídos
            
    #         LOGGER.info(f"Arquivos descompactados para: {extract_to}")

    #         # Reprocessar arquivos extraídos para UTF-8, se necessário
    #         for file_name in extracted_files:
    #             LOGGER.info("CHEGUEI AQUI")
    #             file_path = os.path.join(extract_to, file_name)
    #             if file_name.endswith('.csv') or file_name.endswith('.txt'):
    #                 self._convert_to_utf8(file_path)
            
    #         return extracted_files
    #     except zipfile.BadZipFile:
    #         LOGGER.error(f"O arquivo {zip_file} está corrompido.")
    #         raise
    #     except Exception as e:
    #         LOGGER.error(f"Erro ao descompactar o arquivo {zip_file}: {str(e)}")
    #         raise

    # def _convert_to_utf8(self, file_path):
    #     try:
    #         LOGGER.info("ENTREI AQUI")
    #         # Ler o arquivo no encoding original e regravar em UTF-8
    #         with open(file_path, 'r', encoding='latin1') as file:  # Ajuste 'latin1' para o encoding original, se necessário
    #             content = file.read()
    #         with open(file_path, 'w', encoding='utf-8') as file:
    #             file.write(content)
    #         LOGGER.info(f"Arquivo {file_path} convertido para UTF-8.")
    #     except Exception as e:
    #         LOGGER.error(f"Erro ao converter o arquivo {file_path} para UTF-8: {str(e)}")
    #         raise

        
    def upload_to_s3(self,local_file, bucket_name, s3_key):
        # Subir arquivos para o S3
        s3_client = boto3.client('s3')
        try:
            s3_client.upload_file(local_file, bucket_name, s3_key)
            LOGGER.info(f"Arquivo {local_file} enviado para o S3 com sucesso!")
        except Exception as e:
            LOGGER.info(f"Erro ao enviar para o S3: {str(e)}")
            raise Exception(f"Erro ao enviar para o S3: {str(e)}")

            