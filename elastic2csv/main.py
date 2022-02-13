#!/usr/bin/env python

from core import compositeexport as ce
from core import elastic2csv
from dotenv import load_dotenv
import logging
log = logging.getLogger(__name__)
import sys
import os, shutil
import traceback
import argparse


if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(filename='runs.log', level=logging.INFO, format='[%(asctime)s-%(levelname)s] %(name)s: %(message)s')


    args = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    required_named = args.add_argument_group('required named arguments')
    required_named.add_argument('-r', '--request-file', type=str, required=True, help='Json file containing query.')
    required_named.add_argument('-i', '--index', type=str, required=True, help='Index pattern name example "logstash". "-*" will be added.')

    args.add_argument('-u', '--url', type=str, default='http://localhost:9200',help='Host server url with default: %(default)s.')
    args.add_argument('-su', '--server-username', type=str, help='if url is from remote ssh enter username. You need ssh access credentials for this to work')
    args.add_argument('-sh', '--server-host', type=str, help='if url is from remote ssh enter host. You need ssh access creddential for this to work')
    args.add_argument('-o', '--out-dir', type=str, default='out', help='Directory where to save the json file dump')


    #
    # try:
    #     ce.mkdir("out")
    #     #TODO: improve call to local elasticsearch from sh file to python to module
    #     es = ce.connect_elasticsearch()
    #
    #     req = ce.load_request(sys.argv[1])
    #     log.info('dumping response')
    #     output_file = ce.search_and_export(es, req, "out", sys.argv[2])
    #     # make a copy to root directory for further pipeline manipulation
    #     shutil.copy(output_file, 'tmp_dump.json')
    #
    # except Exception as e:
    #     log.exception("compositeexport.py")
    es = elastic2csv.Elastic2csv(args.parse_args())
    try:
        log.info(f'Starting export')

        es.connect()
        es.search()

    except Exception as e:
        log.exception("main.py")
