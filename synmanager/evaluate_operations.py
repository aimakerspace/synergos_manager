#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import logging

# Libs


# Custom
from .base import ProducerOperator, ConsumerOperator
from .config import EVALUATE_ROUTING_KEY, EVALUATE_QUEUE

##################
# Configurations #
##################

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)

#################################################################
# Evaluation producer operator Class - EvaluateProducerOperator #
#################################################################

class EvaluateProducerOperator(ProducerOperator):
    """ 
    Contains management functionality for handling preprocessing job 
    submissions to the "Evaluate" queue.
    """
    def __init__(self, host=None):
        super().__init__(host)

        # General attributes


        # Network attributes
        self.routing_key = EVALUATE_ROUTING_KEY

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
# Evaluation consumer operator Class - EvaluateConsumerOperator #
#################################################################

class EvaluateConsumerOperator(ConsumerOperator):
    """ 
    Contains management functionality for handling evaluation job 
    consumptions from the "Evaluate" queue.
    """
    def __init__(self, host=None):
        super().__init__(host)

        # General attributes
        

        # Network attributes
        self.routing_key = EVALUATE_ROUTING_KEY
        self.queue = EVALUATE_QUEUE

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