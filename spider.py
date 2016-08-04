#coding=utf8

import requests
import re
import time
import hashlib
import json
import random
import pdb

import gevent
from gevent import queue, pool, monkey

from log import log
from rdd import RDD


class HttpHandler(gevent.Greenlet):

    def __init__(self, url, callback,  retries, spider, interval=3):
        gevent.Greenlet.__init__(self)
        self.url = url
        self.callback = callback
        self.retries = retries
        self.interval = interval
        self.spider = spider
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36"}

    def get_one_proxy(self):

        while True:
            host, times, time_use = self.spider._proxy_qs.get()
            if times < 4:
                break
            print "drop one:%s"%()

        return (host, times, time_use)

    def _run(self):
        self.spider._total_count += 1
        before = time.time()
        response = None
        host = None
        try:
            host, times, time_use = self.get_one_proxy()
            proxies = {"http":"http://%s"%(host)}
            
            now = time.time()
            if now-time_use < self.interval:
                gevent.sleep(self.interval-now+time_use)
            before = time.time()
            response = requests.get(self.url, timeout=4, proxies=proxies, headers=self.headers)
        except :
            if host:
                try:
                    self.spider._proxy_qs.put_nowait((host, times+1, time.time()))
                except gevent.queue.Full:
                    pass

            self.spider._wait_urls.put((self.url, self.callback, self.retries+1))
            after = time.time()
            return

        if response:
            try:
                self.spider._proxy_qs.put_nowait((host, 0, time.time()))
            except gevent.queue.Full:
                pass

            if response.status_code == 403:
                print "warning, forbidden"
                self.spider._wait_urls.put((self.url, self.callback, self.retries+1))

            if response.status_code == 200:
                result = None
                try:
                    result = self.callback(response)
                except:
                    pass
                if result:
                    self.spider._count += 1
                    if self.spider._count % 100 == 0:
                        print "count: %s"%(self.spider._count)
                    self.spider._out_file.write(("!@#$").join(result).replace('\n','').encode('utf-8') +'\n')
        else:
            if host:
                try:
                    self.spider._proxy_qs.put_nowait((host, times+1, time.time()))
                except gevent.queue.Full:
                    pass
                self.spider._wait_urls.put((self.url, self.callback, self.retries+1))


        after = time.time()


class base_crawler:
    def __init__(self, out_path, pool_size=500):
        monkey.patch_all(httplib=True)

        self._count = 0
        self._total_count = 0

        self._rep_table = set()
        self._wait_urls = queue.Queue(maxsize=50000)
        self._proxy_qs = queue.Queue(maxsize=20000)
        
        self.pool = pool.Pool(pool_size)

        self._out_path = out_path
        self._out_file = None

    def add_rep(self, url):
        self._rep_table.add(url)

    def crawl(self, url, callback=None, retries=0):
        if url not in self._rep_table:
            #self._rep_table.add(url)
            if self._wait_urls.qsize() > 20000:
                gevent.sleep(5)
            self._wait_urls.put((url, callback, retries))

    def on_start(self):
        pass

    def get_one_url(self):
        while True:
            url, callback, retries = self._wait_urls.get_nowait()
            if retries < 3:
                break
            else:
                log.log("%s\tfailed"%(url))
        return (url, callback, retries)

    def on_scheduler(self):

        i = 0
        self._out_file = open(self._out_path, 'w')
        while True:     
            try:
                for let in list(self.pool):
                    if let.dead:
                        self.pool.discard(let)
                        print "discard a let"

                url, callback, retries = self.get_one_url()
            except queue.Empty:
                if self.pool.free_count() != self.pool.size:
                    print "sleep for get url:%s"%(3)
                    gevent.sleep(3)
                    i += 1
                    if i < 10:
                        continue
                    else:
                        break
                else:
                    break

            let = HttpHandler(url, callback, retries, self)
            self.pool.start(let)
            i = 0


        self._out_file.close()
        print "game over"

    def proxy_run(self):
        from proxy import proxy
        p = proxy(self._proxy_qs)
        p.on_run()

    def run(self):
        gevent.joinall([gevent.spawn(self.on_start), gevent.spawn(self.on_scheduler), gevent.spawn(self.proxy_run)])
       


class spider(base_crawler):

    def __init__(self, out_path):
        base_crawler.__init__(self,  out_path)


    def on_start(self):

        def parse_line(line):
            business = json.loads(line.strip())
            business_id = business.get('business_id',None) 
            requestUrl = 'http://www.dianping.com/shop/%s'%(business_id)
            self.crawl(requestUrl, callback=self.index_page)

        def parse_add_rep(line):
            url = line.strip().split("!@#$")[0]
            self.add_rep(url)

        day = '2015_7_12'

        RDD.txtfile('/search/data/dianping/result'+day).map(parse_line)

    def index_page(self, response):
        return (response.url,response.text)


if __name__ == '__main__':
    day = '2015_7_12'
    log_name = '/search/data/dianping/html_%s.log'%(day)
    out = '/search/data/dianping/dianping_table/dianping_html_'+day

    log.basicConfig(filename=log_name)
    
    sp = spider(out)
    sp.run()
