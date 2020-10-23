import rpyc
from rpyc.utils.server import ThreadedServer
import datetime
import configparser
import concurrent.futures
import traceback
from worker_trigger import start_worker_instance
import cmp_eng
import logging
import os
import multiprocessing


logging.basicConfig(level=logging.DEBUG,filename='app.log',filemode='w')

config=configparser.ConfigParser()
config.read('config.ini')

class Master(rpyc.Service):
    def __init__(self):
        super().__init__()
        self.cluster = dict()
        self.num_mappers=int(config['MASTER']['NUM_MAPPERS'])
        self.num_reducers=int(config['MASTER']['NUM_REDUCERS'])

    def on_connect(self, conn):
        time=datetime.datetime.now()
        print('Client connected on',time)

    def on_disconnect(self, conn):
        time=datetime.datetime.now()
        print('Client disconnected on',time)

    def exposed_init_cluster(self):
        #mappers=[]
        #reducers=[]

        """
        with concurrent.futures.ProcessPoolExecutor() as executor:
            mapper_names = [config['MAPPER']['NAME'] +
                            str(i) for i in range(1, self.num_mappers+1)]

            for name,IP in zip(mapper_names,executor.map(self.spawn_worker, mapper_names)):
                mappers.append((name,IP))

            reducer_names = [config['REDUCER']['NAME'] +
                             str(i) for i in range(1, self.num_reducers+1)]

            for name,IP in zip(reducer_names,executor.map(self.spawn_worker,reducer_names)):
                reducers.append((name,IP))
        
        """
        try:
            mapper_names = [config['MAPPER']['NAME'] +
                            str(i) for i in range(1, self.num_mappers + 1)]
            reducer_names = [config['REDUCER']['NAME'] + str(i) for i in range(1, self.num_reducers + 1)]

            with concurrent.futures.ProcessPoolExecutor() as executor:
                mapper_processes=[executor.submit(self.spawn_worker,mapper) for mapper in mapper_names]
                reducer_processes=[executor.submit(self.spawn_worker,reducer) for reducer in reducer_names]
                mappers=[process.result() for process in concurrent.futures.as_completed(mapper_processes)]
                reducers=[process.result() for process in concurrent.futures.as_completed(reducer_processes)]



            """
            async_spawn=rpyc.async_(self.spawn_worker)
            mapper_processes=[async_spawn(mapper) for mapper in mapper_names]
            reducer_processes=[async_spawn(reducer) for reducer in reducer_names]

            for mapper in mapper_processes:
                while not mapper.ready:
                    continue

            for reducer in reducer_processes:
                while not reducer.ready:
                    continue

            mappers=[process.value for process in mapper_processes]
            reducers=[process.value for process in reducer_processes]
            """

            self.mappers=mappers
            self.reducers=reducers

            return (self.mappers,self.reducers)

        except Exception as e:
            logging.error(e)
            raise Exception(e)

    def exposed_destroy_cluster(self):
        try:
            mapper_names=[name for name,_ in self.mappers]
            reducer_names=[name for name,_ in self.reducers]

            with concurrent.futures.ProcessPoolExecutor() as executor:
                for mapper in mapper_names:
                    executor.submit(cmp_eng.delete_instance, mapper)
                for reducer in reducer_names:
                    executor.submit(cmp_eng.delete_instance, reducer)

            return 1

        except Exception as e:
            traceback.print_exc()
            return 0

    def exposed_run_map_reduce(self,in_loc,map_func,red_func,out_loc):
        try:
            self.map_func=map_func
            self.red_func=red_func
            self.out_loc=out_loc

            book_names=list(os.listdir(in_loc))
            map_in_files=[[] for i in range(self.num_mappers)]
            for i in range(len(book_names)):
                index=i%self.num_mappers
                map_in_files[index].append((i+1,book_names[i]))

            mapper_input=[[] for i in range(self.num_mappers)]
            
            for i in range(len(map_in_files)):
                for file_index,file in map_in_files[i]:
                    mapper_input[i].append((file_index,open(in_loc+file,'r',encoding='utf-8').read()))

            mapper_ips=[ip for _,ip in self.mappers ]
            reducer_ips=[ip for _,ip in self.reducers ]

            logging.info(f'Worker IPs- {(mapper_ips,reducer_ips)}')

            mappers=self.run_mapper(mapper_ips)
            reducers=self.run_reducer(reducer_ips)

            logging.info('Starting mapper tasks')

            """
            with concurrent.futures.ProcessPoolExecutor() as executor:
                mapper_responses=executor.map(self.run_mapper,zip(mapper_ips,mapper_input,range(1,self.num_mappers+1)))
            """

            map_add=[rpyc.async_(mapper.add)(2,3) for mapper in mappers]

            for process in map_add:
                while not process.ready:
                    continue

            mapper_responses=[]
            for process in map_add:
                mapper_responses.append(process.value)

            for response in mapper_responses:
                if response!=5:
                    raise Exception("Mapper task incomplete")

            logging.info('Completed mapper tasks')

            logging.info('Starting reducer tasks')

            """
            with concurrent.futures.ProcessPoolExecutor() as executor:
                reducer_responses=executor.map(self.run_reducer,zip(reducer_ips,range(1,self.num_reducers+1)))
            
            """
            red_add = [rpyc.async_(reducer.add)(2, 3) for reducer in reducers]

            for process in red_add:
                while not process.ready:
                    continue

            reducer_responses = []

            for process in red_add:
                reducer_responses.append(process.value)

            for response in reducer_responses:
                if response!=5:
                    raise Exception("Reducer task incomplete")

            logging.info('Completed reducer tasks')

            return 1
        except Exception as e:
            traceback.print_exc()
            logging.error('Error in map reduce task- \n'+str(e))
            return 0

    def run_mapper(self,mapper_ips):
        try:
            """
            mapper_ip=mapper_inp[0]
            inp_data=mapper_inp[1]
            mapper_index=mapper_inp[2]

            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            conn=rpyc.connect(mapper_ip[0],int(config['MAP_REDUCE']['PORT']),config=rpyc.core.protocol.DEFAULT_CONFIG)
            worker=conn.root

            logging.debug(f'Mapper {mapper_ip} performing task 1')
            result = worker.add(2, 3)

            logging.debug(f"{mapper_ip} task 1 result- {result}")

            if result!=5:
                raise Exception('Incorrect Result')

            """
            """
            logging.info(f'Mapper {mapper_ip} performing task 2')


            result,_=worker.execute('MAPPER',self.map_func,inp_data,mapper_index)

            logging.debug(f"{mapper_ip} task 2 result- {result}")

            if result!=1:
                raise Exception(f'Something went wrong in mapper- {mapper_ip}')
            """
            """

            conn.close()
            """

            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            mappers=[]
            for ip in mapper_ips:
                conn = rpyc.connect(ip[0], int(config['MAP_REDUCE']['PORT']), config=rpyc.core.protocol.DEFAULT_CONFIG)

                mappers.append(conn.root)

            return mappers

        except Exception as e:
            traceback.print_exc()
            logging.error('Error in map task- \n' + str(e))
            raise Exception(str(e))

    def run_reducer(self,reducer_ips):
        try:
            """
            reducer_ip=reducer_inp[0]
            reducer_index=reducer_inp[1]

            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            conn = rpyc.connect(reducer_ip[0], int(config['MAP_REDUCE']['PORT']), config=rpyc.core.protocol.DEFAULT_CONFIG)
            worker = conn.root

            logging.debug(f"Reducer {reducer_ip} performing task 1")
            result = worker.add(2, 3)
            logging.debug(f"{reducer_ip} task 1 result- {result}")

            if result != 5:
                raise Exception('Incorrect Result')

            
            logging.debug(f"Reducer {reducer_ip} performing task 2")
            status,result=worker.execute('REDUCER',self.red_func,None,reducer_index)
            logging.debug(f"{reducer_ip} task 1 result- {status}")
            
            if status==1:
                with open(self.out_loc+f'-{reducer_index}.txt','w') as file:
                    file.write()
            
            else:
                raise Exception('Something went wrong in the reducer')
            
            
            conn.close()
            """

            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            reducers = []
            for ip in reducer_ips:
                conn = rpyc.connect(ip[0], int(config['MAP_REDUCE']['PORT']), config=rpyc.core.protocol.DEFAULT_CONFIG)

                reducers.append(conn.root)

            return reducers

        except Exception as e:
            traceback.print_exc()
            logging.error('Error in reduce task- \n' + str(e))
            raise Exception(str(e))

    def spawn_worker(self,worker_name):
        try:

            return start_worker_instance(worker_name)
        except Exception as e:
            traceback.print_exc()


if __name__=="__main__":
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    t=ThreadedServer(Master,
    hostname='0.0.0.0',port=8080,protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    t.start()