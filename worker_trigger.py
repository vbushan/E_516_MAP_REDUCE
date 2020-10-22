import configparser
import cmp_eng
from pprint import pprint
import traceback
import os

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

        print('Creating Worker instance....')

        status = cmp_eng.wait_for_operation(create_op['name'])
        pprint(status)

        print('[Checkpoint] Worker Instance Created')

        INT_IP,EXT_IP=cmp_eng.get_ip(name)
        return INT_IP,EXT_IP

    except Exception as e:
        traceback.print_exc()
        
#print(start_worker_instance('worker-1'))
