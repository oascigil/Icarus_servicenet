# -*- coding: utf-8 -*-
"""Implementations of all service-based strategies"""
from __future__ import division

import networkx as nx

from icarus.registry import register_strategy
from icarus.util import inheritdoc, path_links
from .base import Strategy

__all__ = [
       'ServiceRouting'
           ]

@register_strategy('SBR')
class ServiceRouting(Strategy):
    """ A distributed approach for service-centric routing
    """
   
    def __init__(self, view, controller, measurement_interval, **kwargs):
        super(ServiceRouting, self).__init__(view, controller)
        self.measurement_interval = measurement_interval
        self.receivers = view.topology().receivers()

    @inheritdoc(Strategy)
    def process_event(self, time, receiver, content, log, node, flow_id, deadline, response):
        service = content
        compSpot = None
        if self.view.has_computationalSpot(node):
            compSpot = self.model.compSpot[node]
            
            
        if response is True:
            # response is on its way back to the receiver
            if node is receiver:
                self.controller.end_session() #TODO add flow_time
                return
            else:
                if compSpot is not None:
                   compSpot.process_service_response(node, service, time, flow_id)

                path = self.view.shortest_path(node, receiver)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, True)

        else:
            # Processing a request
            source = self.view.content_source(service)
            if self.view.has_service(node, service):
                compTime = cs.run_service(service, time, deadline, flow_id)
                if compTime is 0:
                    # Pass the request upstream
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.link_delay(node, next_node)
                    self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline-delay, False)
                else:
                    # Success in running the service
                    path = self.view.shortest_path(node, receiver)
                    next_node = path[1]
                    self.controller.add_event(time+compTime, receiver, service, next_node, flow_id, deadline-delay, True)


            else:
            source = self.view.content_source(service)
            finishTime = cs.getFinishTime()
             
            





            



        
