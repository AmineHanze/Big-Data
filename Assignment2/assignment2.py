from Bio import Entrez
import multiprocessing as mp
from multiprocessing.managers import BaseManager
import argparse as ap
import time, queue
import pickle
from pathlib import Path
import os

Entrez.email = "amine.jalali@gmail.com"
POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
AUTHKEY = b'Amine'
Entrez.api_key = "d4cea981fb2c06210b961c26c207cac00c08"

def make_server_manager(port, authkey, ip):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = queue.Queue()
    result_q = queue.Queue()

    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    class QueueManager(BaseManager):
        pass

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    manager = QueueManager(address=(ip, port), authkey=authkey)
    manager.start()
    print('Server started at port %s' % port)
    return manager

def runserver(fn, data, ip, port):
    # Start a shared manager server and access its queues
    manager = make_server_manager(port, AUTHKEY, ip)
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()
    
    if not data:
        print("Gimme something to do here!")

        return
    
    print("Sending data!")
    for d in data:
        shared_job_q.put({'fn' : fn, 'arg' : d})
    print(shared_job_q)
    time.sleep(3)  
    
    results = []
    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            print("Got result!", result)
            if len(results) == len(data):
                print("Got all results!")
                break
        except queue.Empty:
            time.sleep(3)
            continue
    # Tell the client process no more data will be forthcoming
    print("Time to kill some peons!")
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    print("Aaaaaand we're done for the server!")
    manager.shutdown()

def make_client_manager(ip, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """
    class ServerQueueManager(BaseManager):
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    manager.connect()

    print('Client connected to %s:%s' % (ip, port))
    return manager

def runclient(num_processes, ip, port):
    manager = make_client_manager(ip, port, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)
    
def run_workers(job_q, result_q, num_processes):
    processes = []
    for p in range(num_processes):
        temP = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(temP)
        temP.start()
    print("Started %s workers!" % len(processes))
    for temP in processes:
        temP.join()

def peon(job_q, result_q):
    my_name = mp.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                print(my_name, "is killed")
                return
            else:
                try:
                    result = job['fn'](job['arg'])
                    print("Peon %s Workwork on %s!" % (my_name, job['arg']))
                    result_q.put({'job': job, 'result' : result})
                except NameError:
                    print("Can't find yer fun Bob!")
                    result_q.put({'job': job, 'result' : ERROR})

        except queue.Empty:
            print("sleepytime for", my_name)
            time.sleep(1)

def find_references(pmid,n):
   
   results = Entrez.read(Entrez.elink(dbfrom="pubmed", db="pmc", LinkName="pubmed_pmc_refs",id=pmid))
   references = [f'{link["Id"]}' for link in results[0]["LinkSetDb"][0]["Link"]]
   return references[:n]

def find_author(ref):

    handle = Entrez.esummary(db="pmc", id=ref, rettype="XML", retmode="text")
    record = Entrez.read(handle)
    data=tuple(record[0]["AuthorList"])
    my_path = Path('output')
    with open(str(my_path)+ "/" + f'{ref}.authors.pickle', 'wb') as f:
        pickle.dump(data, f)
    return "Done"

if __name__ == "__main__":

    argparser = ap.ArgumentParser(description="Script that analyze the references to extract the outhor of the given PubMed ID")
    argparser.add_argument("-n", action="store",dest="n", required=True, type=int,help="Number of peons per client")
    group = argparser.add_mutually_exclusive_group()
    group.add_argument("-c", action='store_true',dest="client")
    group.add_argument("-s" , action='store_true',dest="server")
    argparser.add_argument("--port", action="store",dest="port", required=True, type=int,help="port of client")
    argparser.add_argument("--host", action="store",dest='host', type=str, help="hostname")
    argparser.add_argument("-a", action="store",dest="a", required=True, type=int,help="Number of references")
    argparser.add_argument("pubmed_id", action="store", type=str, nargs=1, help="Pubmed ID of the article")
    args = argparser.parse_args()
    ip = args.host
    port = args.port
    data = find_references(args.pubmed_id,args.a)
    if not os.path.isdir("output"):
        os.makedirs("output")
    if args.client:
        client = mp.Process(target=runclient, args=(args.n, ip, port))
        client.start()
        client.join()
    elif args.server:
        server = mp.Process(target=runserver, args=(find_author,data, ip, port))
        server.start()
        server.join()
