from typing import Dict, List
from singer.logger import get_logger
from extract_cnpjs.enpoints.utils import Utils
from extract_cnpjs.enpoints.empresas import GetEmp

LOGGER = get_logger()

class Cnpj(Utils):
    def __init__(self, config: Dict) -> None:
        self.config = config
    
    def sync(self,stream) -> List:
        
        if stream == "empresas":
            GetEmp(self.config).sync()
        elif stream == "socios":
            GetEmp(self.config).sync()
        else:
            raise Exception(f"Stream > {stream} não localizado.")