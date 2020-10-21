import configparser
import traceback
from E_516_MAP_REDUCE import cmp_eng
import os
import pprint

config=configparser.ConfigParser()
config.read('config.ini')


PROJECT = config['MAP_REDUCE']['PROJECT_ID']
ZONE = config['MAP_REDUCE']['ZONE']
MASTER_NAME = config['MASTER']['NAME']
KV_SERVER_NAME=config['KV_SERVER']['NAME']
master_startup_script= open(
    os.path.join(
        os.path.dirname(__file__), 'master-startup-script.sh'), 'r').read()

kv_server_startup_script=open(
    os.path.join(
        os.path.dirname(__file__), 'kv_server_startup_script.sh'), 'r').read()
try:
    print('Creating Master instance....')

    master_create_op= cmp_eng.create_instance(
                                        MASTER_NAME, master_startup_script)
    status = cmp_eng.wait_for_operation(PROJECT, ZONE, master_create_op['name'])
    pprint(status)

    print('[Checkpoint] Master Instance Created')

    MASTER_IP = cmp_eng.get_ip(PROJECT, ZONE, MASTER_NAME)
    MASTER_PORT = int(config['MAP_REDUCE']['PORT'])

    print('MASTER NODE ADDRESS', (MASTER_IP, MASTER_PORT))

    print('Creating KV Server instance....')
    kv_server_create_op= cmp_eng.create_instance(
                                        KV_SERVER_NAME, kv_server_startup_script)
    status = cmp_eng.wait_for_operation(PROJECT, ZONE, kv_server_create_op['name'])
    pprint(status)

    print('[Checkpoint] KV Server Instance Created')

    KV_SERVER_IP = cmp_eng.get_ip(PROJECT, ZONE, MASTER_NAME)
    KV_PORT = int(config['MAP_REDUCE']['PORT'])

    print('KV SERVER NODE ADDRESS', (KV_SERVER_IP, KV_PORT))

except Exception as e:
    traceback.print_exc(e)