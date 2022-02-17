#!/usr/bin/env python

from core import compositeexport as ce
from core import elastic2csv
from dotenv import load_dotenv
import logging
import sys
import os, shutil
import traceback
import argparse
log = logging.getLogger(__name__)


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
    args.add_argument('-c', '--convert-only', type=str, help='Directory to dump file to convert to csv')
    args.add_argument('-d', '--dump-only', action='store_true', help='flag to only export data from elasticserach (no conversion)')

    arguments = args.parse_args()
    es = elastic2csv.Elastic2csv(arguments)
    try:

        if arguments.convert_only and arguments.dump_only:
            raise Exception("Cannot assign both -c and -d flags at the same time")

        if not arguments.convert_only:
            es.connect()
            es.search()
            if not arguments.dump_only:
                es.to_csv()
        else:
            es.to_csv(file=arguments.convert_only)

    except Exception as e:
        log.exception("main.py")
