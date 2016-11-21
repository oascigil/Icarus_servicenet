# -*- coding: utf-8 -*-
"""Computational Spot implementation
This module contains the implementation of a set of VMs residing at a node. Each VM is abstracted as a FIFO queue. 
"""
from __future__ import division
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

    def __init__(self, numOfVMs, n_services, services, dist=None, measurement_interval = 5, ranking_interval = 20):
        """Constructor

        Parameters
        ----------
        numOfVMs: total number of VMs available at the computational spot
        n_services : size of service population
        services : list of all the services with their attributes
        measurement_interval : perform upstream (i.e., probe) measurement and decide which services to run.
        """


        if numOfVMs is -1:
            numOfVMs = 10000
            self.is_cloud = True
        else:
            self.numOfVMs = numOfVMs
            self.is_cloud = False
        print "Number of VMS: " + repr(numOfVMs)
        self.n_services = n_services
        # number of VMs per service
        self.service_counts = {x : 0 for x in range(1, n_services)}
        # Finish time of the last request (i.e., tail of the queue)
        self.tailFinishTime = {x : 0 for x in range(1, n_services)}
        # Hypothetical finish time of the last request (i.e., tail of the queue)
        self.virtualTailFinishTime = {x : 0 for x in range(1, n_services)}
        
        
        # Ranking metrics/statistics
        # number of requests forwarded upstream within the measurement interval
        self.forwarded_requests = {x: 0 for x in range(1, n_services)}
        # number of requests satisfied locally within the measurement interval
        self.returned_requests = {x: 0 for x in range(1, n_services)}
        # ranking metric of the queue
        self.metric = {x : 0 for x in range(1, n_services)}
        # ranking metric of the virtual queue
        self.virtual_metric = {x: 0 for x in range(1, n_services)}

        # PIT table for bookkeeping: arrival times, hypothetical service times, etc. 
        self.arrival_time = {} # arrival time of a request
        self.virtual_finish = {} # hypothetical finish time
        self.deadline = {} # remaining deadline of a flow
        self.upstream_service_time = {} # observed service time from upstream
        self.measurement_interval = measurement_interval
        self.last_measurement_time = {x : -1*measurement_interval for x in range(1, n_services)}
        self.services = services
        self.view = None
        self.node = None

        if dist is None:
            # setup a random set of services to run initially
            for x in range(0, numOfVMs):
                service_index = random.choice(range(1, n_services))
                self.service_counts[service_index] += 1

    def replace_services(self, k, interval):
        """
        replace the k worst service instances (VMs) with the best k alternative 
        (i.e., virtual) instances according to the service ranking metrics
        """
        if self.is_cloud:
            return

        # Compute service utilization
        utilization = {x:(self.returned_requests[x]*self.services[x].service_time)/(interval*self.service_counts[x]) for x in range(1, self.n_services) if self.service_counts[x] > 0}
        v_utilization = {x:(self.forwarded_requests[x]*self.services[x].service_time)/interval for x in range(1, self.n_services)}
        util = sorted(utilization.keys(), key=utilization.get)
        v_util = sorted(v_utilization.keys(), key = v_utilization.get)
        
        # Take the average of the metrics (per request)
        av_metric = {x:self.metric[x]/(1+self.returned_requests[x]) for x in self.metric.keys()}
        av_virtual_metric = {x:self.virtual_metric[x]/(1+self.forwarded_requests[x]) for x in self.virtual_metric.keys()}
        v_candidates = [x for x in range(1, self.n_services)]
        v_candidates = sorted(v_candidates, key = av_virtual_metric.get, reverse=True)
        print "Virtual candidates: " + repr(v_candidates)
        candidates = [x for x in range(1, self.n_services) if self.service_counts[x] > 0]
        candidates = sorted(candidates, key = av_metric.get)
        print "candidates to remove: " + repr(candidates)
        print "service utilization" + repr(utilization)
        print "virtual service utilization " + repr(v_utilization)

        # Replace any idle service instances with non-idle
        index = 0
        """
        for service in util:
            if util >= 0.98:
                break
            self.service_counts[service] -= 1
            self.service_counts[v_candidates[index]] += 1
            print "Idle service " + repr(service) + " is replaced with " + repr(v_candidates[index])
            index+=1
            k -= 1
            if not k:
                break
        """
        r_index = 0
        while k > 0:
            if av_metric[candidates[r_index]] < av_virtual_metric[v_candidates[index]]:
                self.service_counts[candidates[r_index]] -= 1
                self.service_counts[v_candidates[index]] += 1
                print "Service " + repr(candidates[r_index]) + " is replaced with " + repr(v_candidates[index])
                k -= 1
                index+=1
                r_index+=1
            else:
                break
            
        # Reinitialise the statistics
        self.forwarded_requests = {x: 0 for x in range(1, self.n_services)}
        self.returned_requests = {x: 0 for x in range(1, self.n_services)}
        self.metric = {x : 0 for x in range(1, self.n_services)}
        self.virtual_metric = {x: 0 for x in range(1, self.n_services)}
            
    def getFinishTime(self, service, time):
        """
        get finish time of the request
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

    def runVirtualService(self, service, time, flow_id, deadline):
        """ compute hypothetical finish time of a request sent upstream
        """
        serviceTime = self.services[service].service_time
        tailFinish = self.getVirtualTailFinishTime(service, time)
        completion = tailFinish + serviceTime

        if completion > time + deadline:
            return
        else:
            self.virtual_finish[flow_id] = completion
            self.virtualTailFinishTime[service] += serviceTime

    def perform_measurement(self, service, time, deadline):
        """ perform measurement (i.e., equivalent to sending a probe upstream)
        """
        curr_node = self.node
        path = self.view.shortest_path(curr_node, self.view.content_source(service))
        print "Path is: " + repr(path)
        latency = 0
        remaining_deadline = deadline
        for node in path[1:len(path)-1]:
            cs = self.view.compSpot(node)
            latency += self.view.link_delay(curr_node, node)
            remaining_deadline -= self.view.link_delay(curr_node, node)
            if cs.service_counts[service] > 0:
                service_time = self.services[service].service_time/cs.service_counts[service]
                if cs.tailFinishTime[service] + service_time < time + remaining_deadline:
                    latency += cs.tailFinishTime[service] + service_time
                    break
            curr_node = node
        
        self.last_measurement_time[service] = time 
        self.upstream_service_time[service] = latency

    def process_request(self, service, time, deadline, flow_id):
        """
        perform bookkeeping
        """
        self.arrival_time[flow_id] = time
        self.deadline[flow_id] = deadline
        if not self.service_counts[service]:
            self.runVirtualService(service, time, flow_id, deadline)
            self.forwarded_requests[service] += 1

    def run_service(self, service, time, deadline, flow_id):
        """run the service at this spot.

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

        if self.is_cloud:
            print "Runnning in cloud!"
            tailFinish = self.getFinishTime(service, time)
            serviceTime = self.services[service].service_time/self.service_counts[service]
            self.tailFinishTime[service] += serviceTime
            return self.tailFinishTime[service]
        else:
            if time - self.last_measurement_time[service] > self.measurement_interval:
                self.perform_measurement(service, time, deadline)
            
        tailFinish = self.getFinishTime(service, time)
        serviceTime = self.services[service].service_time/self.service_counts[service]
        completionTime = tailFinish + serviceTime
        if completionTime > time + deadline:
            self.runVirtualService(service, time, flow_id, deadline)
            self.forwarded_requests[service] += 1
            return 0 #Failed to run
        else:
            self.returned_requests[service] += 1
            self.tailFinishTime[service] += serviceTime
            # TODO compute contribution (Problem: what if we don't know upstream_service_time)
            contribution = (self.upstream_service_time[service] - self.tailFinishTime[service])/self.deadline[flow_id]
            self.metric[service] += contribution
            return self.tailFinishTime[service]

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
            self.virtual_metric[service] += contribution
            self.virtual_finish.pop(flow_id, None)
        

        self.upstream_service_time[service] = time - self.arrival_time[flow_id]
        self.last_measurement_time[service] = time
        self.arrival_time.pop(flow_id, None)
        self.deadline.pop(flow_id, None)

