import rpyc
import traceback
import cmp_eng
import configparser
import time

config=configparser.ConfigParser()
config.read('config.ini')


_,MASTER_IP=cmp_eng.get_ip(config['MASTER']['NAME'])
PORT=int(config['MAP_REDUCE']['PORT'])


try:
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    master_conn=rpyc.connect(MASTER_IP,PORT,config=rpyc.core.protocol.DEFAULT_CONFIG)
    master=master_conn.root

    s_time=time.perf_counter()
    result1=master.init_cluster()
    print('Workers',result1)
    e_time=time.perf_counter()
    print("Execution time",round(e_time-s_time,2))

    time.sleep(20)

    result2=master.run_map_reduce('./books/','map_word_count','red_word_count','./Output/')
    if not result2:
        raise Exception('Map reduce task incomplete')

    result3=master.destroy_cluster()
    if not result3:
        raise Exception('Destroy cluster task incomplete')


except Exception as e:
    traceback.print_exc()
