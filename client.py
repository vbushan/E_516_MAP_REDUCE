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
    # Step1: Connect to Master
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    master_conn=rpyc.connect(MASTER_IP,PORT,config=rpyc.core.protocol.DEFAULT_CONFIG)
    master=master_conn.root

    # Step2: Initialize Cluster
    s_time=time.perf_counter()
    result1=master.init_cluster(20)

    print('Workers',result1)

    e_time=time.perf_counter()
    print("Creation time",round(e_time-s_time,2))

    # Step2: Run Map reduce
    result2=master.run_map_reduce(config['MAP_REDUCE']['INPUT_LOCATION'],config['MAPPER']['INVERTED_INDEX_MAP_FUNC'],
                                  config['REDUCER']['INVERTED_INDEX_REDUCER_FUNC'],config['MAP_REDUCE']['OUTPUT_LOCATION'])

    if not result2:
        raise Exception('Map reduce task incomplete')

    # Step 4: Destroy Cluster
    result3=master.destroy_cluster()
    if not result3:
        raise Exception('Destroy cluster task incomplete')


except Exception as e:
    traceback.print_exc()
