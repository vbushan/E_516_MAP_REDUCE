import rpyc
from rpyc.utils.server import ThreadedServer
import datetime
import time
import configparser
import concurrent.futures
import traceback
from worker_trigger import start_worker_instance
import cmp_eng
import logging
import os


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
        c_time=datetime.datetime.now()
        logging.info(f'Client connected on {c_time}')

    def on_disconnect(self, conn):
        d_time=datetime.datetime.now()
        logging.info(f'Client disconnected on {d_time}')

    def exposed_init_cluster(self,t):
        try:
            mappers=[]
            reducers=[]

            # Create Workers in parallel.
            # Note: Parallel processing is limited by the number of cores in the Master Node instance

            with concurrent.futures.ProcessPoolExecutor() as executor:
                mapper_names = [config['MAPPER']['NAME'] +
                                str(i) for i in range(1, self.num_mappers + 1)]

                for name, IP in zip(mapper_names, executor.map(self.spawn_worker, mapper_names)):
                    mappers.append((name, IP))

                reducer_names = [config['REDUCER']['NAME'] +
                                 str(i) for i in range(1, self.num_reducers + 1)]

                for name, IP in zip(reducer_names, executor.map(self.spawn_worker, reducer_names)):
                    reducers.append((name, IP))

            # Wait till the workers are ready to connect.
            time.sleep(t)
            self.mappers=mappers
            self.reducers=reducers

            return (self.mappers,self.reducers)

        except Exception as e:
            logging.error(str(e),exc_info=True)
            raise Exception(e)

    def exposed_destroy_cluster(self):
        try:
            # Destroy Worker Instance in parallel
            mapper_names=[name for name,_ in self.mappers]
            reducer_names=[name for name,_ in self.reducers]

            with concurrent.futures.ProcessPoolExecutor() as executor:
                for mapper in mapper_names:
                    executor.submit(cmp_eng.delete_instance, mapper)
                for reducer in reducer_names:
                    executor.submit(cmp_eng.delete_instance, reducer)

            return 1

        except Exception as e:
            
            logging.error(str(e),exc_info=True)
            raise Exception(e)
            return 0

    def exposed_run_map_reduce(self,in_loc,map_func,red_func,out_loc):
        try:
            self.map_func=map_func
            self.red_func=red_func
            self.out_loc=out_loc

            # Step 1: Read the input and divide the data into chunks.
            book_names=list(os.listdir(in_loc))
            map_in_files=[[] for i in range(self.num_mappers)]
            for i in range(len(book_names)):
                index=i%self.num_mappers
                map_in_files[index].append((i+1,book_names[i]))

            logging.info(map_in_files)
            mapper_input=[[] for i in range(self.num_mappers)]
            
            for i in range(len(map_in_files)):
                for file_index,file in map_in_files[i]:
                    mapper_input[i].append((file_index,open(in_loc+file,'r',encoding='utf-8', errors="surrogateescape").read()))

            logging.info(f'Length of mapper input {len(mapper_input)}')

            # Step 2: Connect to workers and divide the map task among workers
            mapper_ips=[ip for _,ip in self.mappers ]
            reducer_ips=[ip for _,ip in self.reducers ]

            logging.info(f'Worker IPs- {(mapper_ips,reducer_ips)}')

            mappers=self.run_mapper(mapper_ips)
            reducers=self.run_reducer(reducer_ips)

            logging.info('Starting mapper tasks')

            # Step 3: Run Map tasks asynchronously to achieve parallel processing.
            map_add=[rpyc.async_(mappers[i].execute)('MAPPER',self.map_func,mapper_input[i],i) for i in range(len(mappers))]

            for process in map_add:
                while not process.ready:
                    continue

            mapper_responses=[]
            for process in map_add:
                mapper_responses.append(process.value)

            for response in mapper_responses:
                if response[0]!=1:
                    raise Exception("Mapper task incomplete")

            logging.info('Completed mapper tasks')

            logging.info('Starting reducer tasks')

            # Step 4: Run reduce task asynchronously
            red_add=[rpyc.async_(reducers[i].execute)('REDUCER',self.red_func,None,i) for i in range(len(reducers))]

            for process in red_add:
                while not process.ready:
                    continue

            reducer_responses = []

            for process in red_add:
                reducer_responses.append(process.value)


            for response in reducer_responses:
                if response[0]!=1:
                    raise Exception("Reducer task incomplete")



            logging.info('Completed reducer tasks')

            return 1
        except Exception as e:
            traceback.print_exc()
            logging.error('Error in map reduce task- \n'+str(e),exc_info=True)
            return 0

    def run_mapper(self,mapper_ips):
        try:
            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            mappers=[]
            for ip in mapper_ips:
                conn = rpyc.connect(ip[0], int(config['MAP_REDUCE']['PORT']), config=rpyc.core.protocol.DEFAULT_CONFIG)

                mappers.append(conn.root)

            return mappers

        except Exception as e:
            traceback.print_exc()
            logging.error('Error in map task- \n' + str(e),exc_info=True)
            raise Exception(str(e))

    def run_reducer(self,reducer_ips):
        try:
            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            reducers = []
            for ip in reducer_ips:
                conn = rpyc.connect(ip[0], int(config['MAP_REDUCE']['PORT']), config=rpyc.core.protocol.DEFAULT_CONFIG)

                reducers.append(conn.root)

            return reducers

        except Exception as e:
            traceback.print_exc()
            logging.error('Error in reduce task- \n' + str(e),exc_info=True)
            raise Exception(str(e))

    def spawn_worker(self,worker_name):
        try:

            return start_worker_instance(worker_name)
        except Exception as e:
            traceback.print_exc()


if __name__=="__main__":
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    t=ThreadedServer(Master,
                     hostname=config['MAP_REDUCE']['IP'], port=int(config['MAP_REDUCE']['PORT']),
                     protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    t.start()