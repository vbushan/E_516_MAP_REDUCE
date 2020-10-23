import cmp_eng
import concurrent.futures
import traceback
import configparser
import os
import time
config=configparser.ConfigParser()
config.read('config.ini')


startup_script = open(
    os.path.join(
        os.path.dirname(__file__), 'worker_startup_script.sh'), 'r').read()


def exposed_create_cluster(mappers,reducers):
    try:
        mapper_names = [name for name in mappers]
        reducer_names = [name for name in reducers]

        with concurrent.futures.ProcessPoolExecutor() as executor:
            for mapper in mapper_names:
                executor.submit(cmp_eng.create_instance, mapper,startup_script)
            for reducer in reducer_names:
                executor.submit(cmp_eng.create_instance, reducer,startup_script)

        for mapper in mapper_names:
            print('Mapper',cmp_eng.get_ip(mapper))

        for reducer in reducer_names:
            print('Reducer',cmp_eng.get_ip(reducer))


        return 1

    except Exception as e:
        traceback.print_exc()
        return 0

mappers=['mp-mapper-'+str(i) for i in range(1,int(config['MASTER']['NUM_MAPPERS'])+1)]
reducers=['mp-reducer-'+str(i) for i in range(1,int(config['MASTER']['NUM_REDUCERS'])+1)]

s_time=time.perf_counter()
print(exposed_create_cluster(mappers,reducers))
e_time=time.perf_counter()
print('Execution time',round(e_time-s_time,2))