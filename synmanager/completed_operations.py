#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import logging
import uuid

# Libs


# Custom
from .base import ProducerOperator, ConsumerOperator
from .config import (
    COMPLETED_ROUTING_KEY,  
    COMPLETED_EXCHANGE_NAME,
    COMPLETED_EXCHANGE_TYPE,
    COMPLETED_QUEUE
)

##################
# Configurations #
##################

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)

#################################################################
# Completed producer operator Class - CompletedProducerOperator #
#################################################################

class CompletedProducerOperator(ProducerOperator):
    """
    Contains management functionality for handling completed job submissions to
    the "Completed_*" queue(s)
    """
    def __init__(self, host: str, port: int):
        super().__init__(host=host, port=port)

        # General attributes


        # Network attributes
        self.exchange_name = COMPLETED_EXCHANGE_NAME
        self.exchange_type = COMPLETED_EXCHANGE_TYPE
        self.routing_key = COMPLETED_ROUTING_KEY


        # Data attributes
    
        
        # Model attributes


        # Optimisation attributes


        # Export Attributes


    ############
    # Checkers #
    ############


    ###########    
    # Helpers #
    ###########
  

    ##################
    # Core Functions #
    ##################



#################################################################
# Completed consumer operator Class - CompletedConsumerOperator #
#################################################################

class CompletedConsumerOperator(ConsumerOperator):
    """ 
    Contains management functionality for handling completed job consumptions 
    from the "Completed" queue. Completed queues are exclusive to consumers 
    and are within a fanout exchange.
    """
    def __init__(self, host: str, port: int):
        super().__init__(host=host, port=port)

        # General attributes


        # Network attributes
        self.routing_key = COMPLETED_ROUTING_KEY
        self.exchange_name = COMPLETED_EXCHANGE_NAME
        self.exchange_type = COMPLETED_EXCHANGE_TYPE
        self.queue = COMPLETED_QUEUE

        # Data attributes
    
        
        # Model attributes


        # Optimisation attributes


        # Export Attributes


    ############
    # Checkers #
    ############


    ###########    
    # Helpers #
    ###########
  

    ##################
    # Core Functions #
    ##################