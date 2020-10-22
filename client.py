import rpyc
import traceback


try:
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    master_conn=rpyc.connect('35.231.241.236', 8080,config=rpyc.core.protocol.DEFAULT_CONFIG)
    master=master_conn.root
    result1=master.init_cluster()
    print(result1)
    result2=master.destroy_cluster()
    print(result2)

except Exception as e:
    traceback.print_exc()
