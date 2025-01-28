import os
from typing import List
import requests
import json
import zipfile
import boto3
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import datetime, timedelta
from io import BytesIO
import singer
LOGGER = singer.get_logger()


from abc import ABC

DEFAULT_CONNECTION_TIMEOUT = 60



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
        
    def upload_to_s3(self,local_file, bucket_name, s3_key):
        # Subir arquivos para o S3
        s3_client = boto3.client('s3')
        try:
            s3_client.upload_file(local_file, bucket_name, s3_key)
            LOGGER.info(f"Arquivo {local_file} enviado para o S3 com sucesso!")
        except Exception as e:
            LOGGER.info(f"Erro ao enviar para o S3: {str(e)}")
            raise Exception(f"Erro ao enviar para o S3: {str(e)}")

            