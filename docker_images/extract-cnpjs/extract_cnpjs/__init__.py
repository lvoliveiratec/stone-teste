import singer
from singer import utils
import json
import os
from .cnpj import Cnpj


REQUIRED_CONFIG_KEYS = []
LOGGER = singer.get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    # args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    
    # config=args.config
    config= os.getenv('CONFIG')
    config = config.replace("'", '"')
    config=json.loads(config)
    stream=config.get("tap_stream_id")
    tap_data = Cnpj(config).sync(stream)

if __name__ == "__main__":
    main()
