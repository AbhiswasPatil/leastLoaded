import math
from GLOBAL import *

class LeastLoaded:
    # workers: Worker[], functions : Function[], packages : Package[]
    def __init__(self, workers, functions, packages):
         
        
        self.threshold = threshold

        self.workers = workers
        self.functions = functions
        self.packages = packages
        
        self.totalRequests = 0
        self.cacheHits = 0
        self.cacheMiss = 0
        self.LEASTLOADEDCALLS = 0

    def getCacheHitAndMissDetails(self):
        return {"totalRequests":self.totalRequests,"cacheHits":self.cacheHits,"cacheMiss":self.cacheMiss , "LEASTLOADEDCALLS": self.LEASTLOADEDCALLS}

    def getLoad(self, worker_id, timestamp):
        workerNodes = self.workers

        index = self.getIndexInWorkersArray(worker_id) #here self.workers == workerNodes hence ok

        #here we will need to clear all the functions that have been executed at this worker.runningFunction and update the 
        #worker.currentLoad as well which is just len(worker.runningFunction)
        workerNodes[index].updateRuningFunctionsList(timestamp)
        self.workers = workerNodes
        return workerNodes[index].currentLoad


    def getLeastLoadedWorker(self,timestamp):
            self.LEASTLOADEDCALLS += 1
            min_worker_id =""
            min_worker_id_load = math.inf
            for i in range(0,len(self.workers)) :
                curr_load = self.getLoad(self.workers[i].worker_id,timestamp)
                if(min_worker_id_load > curr_load) :
                    min_worker_id_load = curr_load
                    min_worker_id = self.workers[i].worker_id
            
            return min_worker_id

    def getWorkerDetails(self,timestamp):
        oldWorkerNodes = self.workers
        workerNodes = self.updateStaleWorkerData(oldWorkerNodes,timestamp)
        
        workerDetails = []

        for i in range(0,len(workerNodes)) :
            workerDetails.append({"worker_id:":workerNodes[i].worker_id,
            "threshold":workerNodes[i].threshold,
            "currentLoad":workerNodes[i].currentLoad,
            "runningFunctions":workerNodes[i].runningFunctions,
            "lastExecutedTime":workerNodes[i].lastExecutedTime,
            })
        
        self.workers = workerNodes

        return workerDetails

    def updateStaleWorkerData(self,workerNodes,timestamp):
        
        for i in range(0,len(workerNodes)) :
            #updates current Load
            workerNodes[i].currentLoad = self.getLoad(workerNodes[i].worker_id,timestamp)
            # updates runningFunctions -> they are updated automatically when getLoad is called
            # updates lastExecutedTime {pid,time}
            newLastExecutedTime = {}
            for key in workerNodes[i].lastExecutedTime :
                if(workerNodes[i].lastExecutedTime[key] + cacheCleanTime > timestamp) :
                    #time to remove cached pkg is yet to come hence keep them as they are in the map
                    newLastExecutedTime[key] = workerNodes[i].lastExecutedTime[key]
            
            workerNodes[i].lastExecutedTime.clear()
            workerNodes[i].lastExecutedTime.update(newLastExecutedTime)

        return workerNodes

    def getIndexInWorkersArray(self,worker_id):
        for i in range(0,len(self.workers)) :
            # print(self.workers[i])
            if(self.workers[i].worker_id == worker_id):
                return i

    def getIndexInFunctionsArray(self,function_id):
        for i in range(0,len(self.functions)) :
            if(self.functions[i].function_id == function_id):
                return i

    def getIndexInPackagesArray(self,package_id):
        for i in range(0,len(self.packages)) :
            if(self.packages[i].package_id == package_id):
                return i

    def addWorker(self,worker) :
        workerNodes = self.workers
        workerNodes.append(worker)
        self.workers = workerNodes 
    
    def removeWorker(self,worker_id) :
        workerNodes = self.workers
        newWorkerNodes = []
        for x in workerNodes:
            if(x.worker_id != worker_id) :
                newWorkerNodes.append(x)
        
        self.workers = newWorkerNodes 

    def assignWorker(self,function_id,timestamp):
        
        workerNodes = self.workers
        self.totalRequests = self.totalRequests + 1


        function_object = self.functions[self.getIndexInFunctionsArray(function_id)]

        err, pkg = function_object.getLargestPackage()

        package_object = self.packages[self.getIndexInPackagesArray(pkg)]
        
        if(err!= "") :
            print("No packages in function : ",function_id," to run !!!\n")
        
        chosen_node_to_run = self.getLeastLoadedWorker(timestamp)
        index_of_chosen_node_to_run=self.getIndexInWorkersArray(chosen_node_to_run)
        

        finalTimeOfFunctionExecution = timestamp + function_object.function_size

        if(self.workers[index_of_chosen_node_to_run].lastExecutedTime.get(pkg) == None) :
            # first time caching pkg
            self.cacheMiss = self.cacheMiss + 1
        elif(self.workers[index_of_chosen_node_to_run].lastExecutedTime[pkg] + cacheCleanTime > timestamp) :
            # as cache is hit, we will remove largest packag's time from time of execution
            finalTimeOfFunctionExecution -= package_object.package_size
            self.cacheHits = self.cacheHits + 1
        else :
            self.cacheMiss = self.cacheMiss + 1

        #add the new function to execute in the worker.runningFunction list depending on cache hit or missed
        workerNodes[index_of_chosen_node_to_run].runningFunctions.append({"finish_time":finalTimeOfFunctionExecution,
                                                                        "function_id":function_object.function_id})
        
        #updating load
        workerNodes[index_of_chosen_node_to_run].currentLoad = len(workerNodes[index_of_chosen_node_to_run].runningFunctions)

        # updating the cache executed time for all imported packages including the biggest package
        # packages_imported contains array of package_id
        packages_imported = self.functions[self.getIndexInFunctionsArray(function_id)].function_imports 
        for i in range(0,len(packages_imported)):
            workerNodes[index_of_chosen_node_to_run].lastExecutedTime[packages_imported[i]] = timestamp

       
        #updating the changes in object
        self.workers = workerNodes
        # print({"",workerNodes[index_of_chosen_node_to_run].worker_id})
        return {"",workerNodes[index_of_chosen_node_to_run].worker_id}







