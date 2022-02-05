from core import compositeexport as ce
from dotenv import load_dotenv
import logging
log = logging.getLogger(__name__)
import sys
import os, shutil
import traceback


if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s-%(levelname)s] %(name)s: %(message)s')
    log.info(f'Starting export')
    log.info(sys.argv[1])
    log.info(sys.argv[2])

    try:
        ce.mkdir("out")
        #TODO: improve call to local elasticsearch from sh file to python to module
        es = ce.connect_elasticsearch()

        req = ce.load_request(sys.argv[1])
        log.info('dumping response')
        output_file = ce.search_and_export(es, req, "out", sys.argv[2])
        # make a copy to root directory for further pipeline manipulation
        shutil.copy(output_file, 'tmp_dump.json')

    except Exception as e:
        log.exception("compositeexport.py")
