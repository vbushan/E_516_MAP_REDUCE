import rpyc
from rpyc.utils.server import ThreadedServer
import datetime
import configparser
import concurrent.futures
import traceback
from worker_trigger import start_worker_instance
import cmp_eng

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
        mappers=[]
        reducers=[]
        with concurrent.futures.ProcessPoolExecutor() as executor:
            mapper_names = [config['MAPPER']['NAME'] +
                            str(i) for i in range(1, self.num_mappers+1)]

            for name,IP in zip(mapper_names,executor.map(self.spawn_worker, mapper_names)):
                mappers.append((name,IP))

            reducer_names = [config['REDUCER']['NAME'] +
                             str(i) for i in range(1, self.num_reducers+1)]

            for name,IP in zip(reducer_names,executor.map(self.spawn_worker,reducer_names)):
                reducers.append((name,IP))

        self.mappers=mappers
        self.reducers=reducers

        return (self.mappers,self.reducers)


    def exposed_destroy_cluster(self):
        try:
            mapper_names=[name for name,_ in self.mappers]
            reducer_names=[name for name,_ in self.reducers]

            with concurrent.futures.ProcessPoolExecutor() as executor:
                executor.map(cmp_eng.delete_instance,mapper_names)
                executor.map(cmp_eng.delete_instance,reducer_names)

            return 1

        except Exception as e:
            traceback.print_exc()
            return 0

    def exposed_run_map_reduce(self):
        try:
            mapper_ips=[ip for _,ip in self.mappers ]
            reducer_ips=[ip for _,ip in self.reducers ]

            with concurrent.futures.ProcessPoolExecutor() as executor:
                mapper_responses=executor.map(self.run_mapper,mapper_ips)

            for response in mapper_responses:
                if not response:
                    raise Exception("Mapper task incomplete")

            with concurrent.futures.ProcessPoolExecutor() as executor:
                reducer_responses=executor.map(self.run_reducer,reducer_ips)

            for response in reducer_responses:
                if not response:
                    raise Exception("Reducer task incomplete")

            return 1
        except Exception as e:
            traceback.print_exc()
            return 0

    def run_mapper(self,mapper_ip):
        try:
            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            conn=rpyc.connect(mapper_ip,config['MAP_REDUCE']['PORT'],config=rpyc.core.protocol.DEFAULT_CONFIG)
            worker=conn.root
            if worker.add(2,3)!=5:
                raise Exception('Incorrect Result')

            return 1
        
        except Exception as e:
            traceback.print_exc()
            return 0

    def run_reducer(self,reducer_ip):
        try:
            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            conn = rpyc.connect(reducer_ip, config['MAP_REDUCE']['PORT'], config=rpyc.core.protocol.DEFAULT_CONFIG)
            worker = conn.root

            if worker.add(2, 3) != 5:
                raise Exception('Incorrect Result')

            return 1

        except Exception as e:
            traceback.print_exc()
            return 0

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