from core import compositeexport as ce
from dotenv import load_dotenv
import logging
log = logging.getLogger(__name__)
import sys
import os


if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s-%(levelname)s] %(name)s: %(message)s')
    log.info(f'Starting export')
    log.info(sys.argv[1])
    log.info(len(sys.argv))

    try:
        ce.mkdir("out")

        es = ce.connect_elasticsearch()

        req = ce.load_request(sys.argv[1])
        log.info('dumping response')

        output_file = ce.search_and_export(es, req, "out")
        # make a copy to root directory for further pipeline manipulation
        shutil.copy(output_file, 'tmp_dump.json')

    except Exception as e:
        log.exception("compositeexport.py")
