import rpyc
import traceback
import configparser
import concurrent.futures
import cmp_eng
import time
config=configparser.ConfigParser()
config.read('config.ini')

"""
arr=([('mp-mapper-1', '34.75.45.27'), ('mp-mapper-2', '35.231.241.231'), ('mp-mapper-3', '35.231.140.220')],
     [('mp-reducer-1', '104.196.181.57'), ('mp-reducer-2', '35.227.100.231'), ('mp-reducer-3', '35.196.210.127')])

"""

arr=(['mp-mapper-'+str(i) for i in range(1,int(config['MASTER']['NUM_MAPPERS'])+1)],
     ['mp-reducer-'+str(i) for i in range(1,int(config['MASTER']['NUM_REDUCERS'])+1)])

#mapper_ips=[ip for _,ip in arr[0]]
#reducer_ips=[ip for _,ip in arr[1]]
mapper_ips=[cmp_eng.get_ip(name)[1] for name in arr[0] ]
reducer_ips=[cmp_eng.get_ip(name)[1] for name in arr[1] ]

print('Mapper IPs- ',mapper_ips)
print('Reducers IPs- ',reducer_ips)

def mapper(ip):
    try:
        rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
        conn = rpyc.connect(ip, config['MAP_REDUCE']['PORT'], config=rpyc.core.protocol.DEFAULT_CONFIG)
        worker = conn.root

        result=worker.add(2, 3)
        print(f'Mapper {ip} result',result)
        if result != 5:
            raise Exception('Incorrect Result')

        return 1

    except Exception as e:
        traceback.print_exc()
        return 0

def reducer(ip):
    try:
        rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
        conn = rpyc.connect(ip, config['MAP_REDUCE']['PORT'], config=rpyc.core.protocol.DEFAULT_CONFIG)
        worker = conn.root

        result = worker.add(2, 3)
        print(f'Reducer {ip} result', result)
        if result != 5:
            raise Exception('Incorrect Result')

        return 1

    except Exception as e:
        traceback.print_exc()
        return 0

s_time=time.perf_counter()
with concurrent.futures.ProcessPoolExecutor() as executor:
    mapper_responses = executor.map(mapper, mapper_ips)

for response in mapper_responses:
    if not response:
        raise Exception("Mapper task incomplete")

with concurrent.futures.ProcessPoolExecutor() as executor:
    reducer_responses = executor.map(reducer, reducer_ips)

for response in reducer_responses:
    if not response:
        raise Exception("Reducer task incomplete")

e_time=time.perf_counter()
print('Execution time',e_time-s_time)



s_time=time.perf_counter()

for ip in mapper_ips:
    if not mapper(ip):
        raise Exception('Incomplete Mapper task')

for ip in reducer_ips:
    if not reducer(ip):
        raise Exception('Incomplete Reducer task')

e_time=time.perf_counter()
print('Execution time',e_time-s_time)