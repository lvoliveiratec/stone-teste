import singer
import json
import os
from .cnpj import Cnpj

LOGGER = singer.get_logger()


def main():
    
    config= os.getenv('CONFIG')
    config = config.replace("'", '"')
    config=json.loads(config)
    stream=config.get("tap_stream_id")
    tap_data = Cnpj(config).sync(stream)
    

if __name__ == "__main__":
    main()
