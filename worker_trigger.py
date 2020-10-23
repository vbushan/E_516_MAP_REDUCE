import configparser
import cmp_eng
from pprint import pprint
import traceback
import os
import logging

logging.basicConfig(level=logging.DEBUG,filename='worker-trigger.log',filemode='w')


config=configparser.ConfigParser()
config.read('config.ini')

PROJECT = config['MAP_REDUCE']['PROJECT_ID']
ZONE = config['MAP_REDUCE']['ZONE']
startup_script = open(
    os.path.join(
        os.path.dirname(__file__), 'worker_startup_script.sh'), 'r').read()


def start_worker_instance(name):
    try:

        create_op = cmp_eng.create_instance(name, startup_script)

        logging.info('Creating Worker instance....')

        status = cmp_eng.wait_for_operation(create_op['name'])
        pprint(status)

        logging.info('[Checkpoint] Worker Instance Created')

        int_ip,ext_ip=cmp_eng.get_ip(name)

        return int_ip,ext_ip

    except Exception as e:
        traceback.print_exc()
        
#print(start_worker_instance('worker-1'))
