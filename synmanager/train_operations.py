#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import logging

# Libs


# Custom
from .base import ProducerOperator, ConsumerOperator
from .config import TRAIN_ROUTING_KEY, TRAIN_QUEUE

##################
# Configurations #
##################

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)

#########################################################
# Train producer operator Class - TrainProducerOperator #
#########################################################

class TrainProducerOperator(ProducerOperator):
    """ 
    Contains management functionality for handling training job 
    submissions to the "Train" queue.
    """

    def __init__(self, host: str, port: int):
        super().__init__(host=host, port=port)

        # General attributes


        # Network attributes
        self.routing_key = TRAIN_ROUTING_KEY

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



#########################################################
# Train consumer operator Class - TrainConsumerOperator #
#########################################################

class TrainConsumerOperator(ConsumerOperator):
    """ 
    Contains management functionality for handling training job 
    consumptions from the "Train" queue
    """

    def __init__(self, host: str, port: int):
        super().__init__(host=host, port=port)

        # General attributes


        # Network attributes
        self.routing_key = TRAIN_ROUTING_KEY
        self.queue = TRAIN_QUEUE

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