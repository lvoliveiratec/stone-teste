from .utils import Utils
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile
import json
import os
from datetime import datetime
import pytz
import singer
LOGGER = singer.get_logger()


class GetEmp(Utils):
    def __init__(self, config) -> None:
        super().__init__(config)
        
        self.config=config
        
        self.start_date= config.get("start_date")
        LOGGER.info(f"START >> {self.start_date}")
        self.bucket_name = config.get("bucket_name")
        self.s3_directory = config.get("s3_directory" )
        self.tap_stream_id=config.get("tap_stream_id")
        self.fieldnames= config.get("fieldnames")
        
        
    def sync(self):
        
        page=1
        
        endpoint= f"{self.start_date}/Empresas{page}.zip"
                
        response= self.do_request(endpoint)
        # Passo 2: Descompactar o arquivo
        extract_dir = './extracted_files'
        os.makedirs(extract_dir, exist_ok=True)
        self.extract_zip(response, extract_to=extract_dir)
        
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                local_file_path = os.path.join(root, f"{file}")
                LOGGER.info(local_file_path)
                # fieldnames=self.fieldnames.split(',')
                # self.convert_csv_to_json(local_file_path,local_file_path,fieldnames)
                blob_name=f"{self.s3_directory}"
                s3_key = os.path.join(blob_name, f"{self.tap_stream_id}{page}.csv")
                self.upload_to_s3(local_file_path, self.bucket_name, s3_key)
        
                if local_file_path and os.path.exists(local_file_path):
                    os.remove(local_file_path)
                    LOGGER.info(f"Arquivo temporário {local_file_path} excluído com sucesso.")

        # page=9
        # stop=True
        # while stop:
        #     try:
        #         endpoint= f"{self.start_date}/Empresas{page}.zip"
                
        #         response= self.do_request(endpoint)
        #         # Passo 2: Descompactar o arquivo
        #         extract_dir = './extracted_files'
        #         os.makedirs(extract_dir, exist_ok=True)
        #         self.extract_zip(response, extract_to=extract_dir)
                
        #         for root, dirs, files in os.walk(extract_dir):
        #             for file in files:
        #                 local_file_path = os.path.join(root, f"{file}")
        #                 LOGGER.info(local_file_path)
        #                 blob_name=f"{self.s3_directory}"
        #                 s3_key = os.path.join(blob_name, f"{self.tap_stream_id}{page}.csv")
        #                 self.upload_to_s3(local_file_path, self.bucket_name, s3_key)
                
        #                 if local_file_path and os.path.exists(local_file_path):
        #                     os.remove(local_file_path)
        #                     LOGGER.info(f"Arquivo temporário {local_file_path} excluído com sucesso.")
        #         page+=1
        #     except:
        #         stop=False
                
        # LOGGER.info("Finsh data extract")    
        