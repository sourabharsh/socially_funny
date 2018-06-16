import sys
from queue import Queue 
from threading import Thread
import time
from datetime import timedelta

from twitter_scraper import Twitter_Scraper


class Work_Assigner(Thread):
    def __init__(self, duration, final_time, queue):
        Thread.__init__(self)
        self.duration = duration
        self.final_time  = final_time
        self.queue = queue

    # query: 'near:"India" since:2018-06-10 until:2018-06-11',
    def run(self):
        self.until_time = self.final_time 
        
        while True :
            self.since_time = self.until_time - self.duration
            
            query = 'near:"India" since:%s until:%s'%(self.since_time, self.until_time)
            print("query:   ", query)
            self.queue.put(query)
            self.until_time = self.since_time
        
            # make sure there are not too many tasks 
            while   self.queue.unfinished_tasks > 10: 
                time.sleep(5)

class Worker(Thread):
    
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
    
    def run(self):
        ts = Twitter_Scraper() 
        while True:
            if self.queue.empty():
                time.sleep(5)
            
            else:
                query = self.queue.get()
                print("Picking:     ", query)
                ts.main(query)
                self.queue.task_done()            
            

if __name__ == "__main__":
    
    # Take the values from the terminal  until_timestamp , interval , last_timestamp file 
    try:
        duration    =   sys.argv[1]*1000
    except: 
        duration  =   60*60          # 1 hour in secconds 
    
    try:
        until_timestamp   =   int(timedelta(sys.argv[2]))
    except:
        until_timestamp   =   int(time.time())
    
    try:
        last_timestamp_file = str(sys.argv[3])
    except:
        last_timestamp_file = "last_timestamp"
    
    queue = Queue()
    # set up for distributing work 
    assigner_thread = Work_Assigner(duration, until_timestamp, queue )
    assigner_thread.daemon = True
    assigner_thread.start()

    # set up for executing work 
    worker_thread_list = {}
    for i in range(6):
        worker_thread_list[i] = Worker(queue)
        worker_thread_list[i].daemon = True
        worker_thread_list[i].start()

    # from a given timestamp, keep going back in time
    # fix a interval of 1 hour
    # save the last timestamp that was executed completely 
    queue.join()
    


