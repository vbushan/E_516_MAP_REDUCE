import rpyc
import traceback
import cmp_eng
import configparser

config=configparser.ConfigParser()
config.read('config.ini')


MASTER_IP=config['MASTER']['NAME']
PORT=int(config['MAP_REDUCE']['PORT'])


try:
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    master_conn=rpyc.connect(MASTER_IP,PORT,config=rpyc.core.protocol.DEFAULT_CONFIG)
    master=master_conn.root
    result1=master.init_cluster()
    print(result1)
    result2=master.destroy_cluster()
    print(result2)

except Exception as e:
    traceback.print_exc()
