# -*- coding: utf-8 -*-
"""Computational Spot implementation
This module contains the implementation of a set of VMs residing at a node. Each VM is abstracted as a FIFO queue. 
"""
from collections import deque
import random
import abc
import copy

import numpy as np

from icarus.util import inheritdoc

__all__ = [
        'ComputationalSpot'
           ]


class ComputationalSpot(object):
    """ 
    A set of computational resources, where the basic unit of computational resource 
    is a VM. Each VM is bound to run a specific service instance and abstracted as a 
    Queue. The service time of the Queue is extracted from the service properties. 
    """

    def __init__(self, numOfVMs, n_services, services, dist=None):
        """Constructor

        Parameters
        ----------
        numOfVMs: total number of VMs available at the computational spot
        n_services : size of service population
        services : list of all the services with their attributes
        """

        self.numOfVMs = numOfVMs
        self.n_services = n_services
        # number of VMs per service
        self.service_counts = {x : 0 for x in range(0, n_services)}
        # Finish time of the last request (i.e., tail of the queue)
        self.tailFinishTime = {x : 0 for x in range(0, n_services)}
        # Hypothetical finish time of the last request (i.e., tail of the queue)
        self.virtualTailFinishTime = {x : 0 for x in range(0, n_services)}
        
        # Measurement statistics
        # number of requests since last measurement
        self.n_requests = 0 
        # ranking metric of the queue
        self.metric = {x : 0 for x in range(0, n_services)}
        # ranking metric of the virtual queue
        self.virtual_metric = {x: 0 for x in range(0, n_services)}

        # PIT table for bookkeeping: arrival times, hypothetical service times, etc. 
        self.arrival_time = {} # arrival time of a request
        self.virtual_finish = {} # hypothetical finish time
        self.deadline = {} # remaining deadline of a flow
        self.upstream_service_time = {} # observed service time from upstream

        if dist is None:
            # setup a random set of services to run initially
            for range(0, numOfVMs):
                service_index = random.choice(range(0, n_services))
                self.service_counts[service_index] += 1

    def getFinishTime(self, service, time):
        """ get finish time of the request
        """

        if self.service_counts[service] is 0:
            return None
        else:
            finishTime = self.tailFinishTime[service]
            if finishTime < time:
                self.tailFinishTime[service] = time
                return time
            else:
                return finishTime

    def getVirtualTailFinishTime(self, service, time):
        """ get finish time of a request in a virtual queue
        """

        finishTime = self.virtualTailFinishTime[service]
        if finishTime < time:
            self.virtualTailFinishTime[service] = time
            return time
        else:
            return finishTime

    def runVirtualService(service, time, flow_id, deadline):
        """ compute hypothetical finish time of a request sent upstream
        """
        serviceTime = self.services[service].service_time
        tailFinish = self.getvirtualTailFinishTime(service, time)
        completion = tailFinish + serviceTime

        if completion > time + deadline:
            return
        else:
            self.virtual_finish[flow_id] = completion
            self.virtualTailFinishTime[service] += serviceTime

    def run_service(self, service, time, deadline, flow_id):
        """Attempt to run the service at this spot

        Parameter
        ---------
        service : service id (integer)
        time : the arriival time of the request
        deadline : remaining deadline of the request
        flod_id : flow identifier (integer)
        
        Return
        ------
        completion time of the request if successful; otherwise 0

        """

        self.arrival[flow_id] = time
        self.deadline[flow_id] = deadline
        tailFinish = self.getFinishTime(service, time)
        serviceTime = self.services[service].service_time/self.service_counts[service]
        completionTime = tailFinish + serviceTime
        if completionTime > time + deadline:
            self.runVirtualService(service, time, flow_id)
            return 0 #Failed to run
        else:
            self.tailFinishTime[service] += serviceTime
            # TODO compute contribution (Problem: what if we don't know upstream_service_time)
            return self.tailFinishTime

    def process_response(self, service, time, flow_id):
        """Process an arriving response packet

        NOTE: The request was not serviced at this Spot (for whatever reason) and 
        handled upstream (e.g., cloud)

        Parameters
        ----------
        service : service id (integer) 
        time : the arrival time of the service response
        flow_id : the flow identifier (integer) 
        """

        if flow_id in self.virtual_finish.keys():
            arr_time = self.arrival_time[flow_id]
            elapsed = time - arr_time
            contribution = (elapsed - self.virtual_finish[flow_id])/self.deadline[flow_id]
            self.virtual_metric += contribution
            self.virtual_finish.pop(flow_id, None)
        
        self.upstream_service_time[service] = time - self.arrival_time[flow_id]
        self.arrival_time.pop(flow_id, None)
        self.deadline.pop(flow_id, None)

