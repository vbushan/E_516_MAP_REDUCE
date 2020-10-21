import rpyc
from rpyc.utils.server import ThreadedServer
import datetime
#import cmp_eng

class Master(rpyc.Service):
    def __init__(self):
        super().__init__()
        self.cluster = dict()

    def on_connect(self, conn):
        time=datetime.datetime.now()
        print('Client connected on',time)

    def on_disconnect(self, conn):
        time=datetime.datetime.now()
        print('Client disconnected on',time)

    def exposed_init_cluster(self):
        pass

    def exposed_destroy_cluster(self):
        pass

    def exposed_run_map_reduce(self):
        pass

    def spawn_worker(self):
        pass


if __name__=="__main__":
    t=ThreadedServer(Master,
    hostname='0.0.0.0',port=8080)
    t.start()