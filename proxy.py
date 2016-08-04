import pdb
import datetime
import gevent
from mysql import SQL

class proxy(object):

    def __init__(self, queue):
        self.queue = queue

    def get_proxy(self, last_time):
        sql = SQL(host='10.134.43.26', user='wenwen', passwd='wenwen', db='platform')
        sql_text = "select host, update_time from proxy where status=1 order by update_time desc"
        results = sql.execute_query(sql_text)
        
        update_last_time = last_time 
        r = []
        if results:
            update_last_time = (results[0]["update_time"]-datetime.datetime(1970, 1, 1)).total_seconds()
            for res in results[:2000]:
                t = res["update_time"]
                if (t-datetime.datetime(1970,1,1)).total_seconds() > last_time:
                    r.append(res["host"])
                else:
                    break
        return (update_last_time, r) 
    
    def on_run(self):
        last_time = 0
        while True:
            last_time, r = self.get_proxy(last_time)
            for host in r:
                try:
                    self.queue.put_nowait((host,0,0))
                except gevent.queue.Full:
                    pass
            
            gevent.sleep(20)
