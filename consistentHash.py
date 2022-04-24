#  hashring takes input of "package_id" and return "worker_id"
from uhashring import HashRing

# Assumptions : workers and worker is just worker_id or worker_name not worker object


class ConsistentHash:
    def __init__(self, workers):
        self.workers = workers
        # initialised hashring with nodes as workers
        self.hr = HashRing(nodes=workers)
        print("ConsistentHash initialised with workers: ", workers)

    def addWorker(self, worker):
        if worker in self.workers:
            print("Worker already in the cluster")
            return
        self.hr.add_node(worker)
        self.workers.append(worker)
        print("Workers added to ConsitentHash: ", worker)

    def removeWorker(self, worker):
        if worker not in self.workers:
            print("Worker is not in the cluster")
            return
        self.hr.remove_node(worker)
        self.workers.remove(worker)
        print("Workers removed from ConsitentHash: ", worker)

    def getWorker(self, package_id):
        worker = self.hr.get_node(package_id)
        print("getworker : ", worker)
        # if there are no workers then return err
        if(worker == None):
            return {"No worker can be chosen, add worker to execute the lambda", None}
        else:
            return {"", worker}