#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import logging
from typing import Dict

# Libs


# Custom
from .base import ProducerOperator, ConsumerOperator
from .config import PREPROCESS_ROUTING_KEY, PREPROCESS_QUEUE

##################
# Configurations #
##################

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)

###################################################################
# Preprocess producer operator Class - PreprocessProducerOperator #
###################################################################

class PreprocessProducerOperator(ProducerOperator):
    """ 
    Contains management functionality for handling preprocessing job 
    submissions to the "Preprocess" queue.
    """

    def __init__(self, host: str, port: int):
        super().__init__(host=host, port=port)

        # General attributes
        

        # Network attributes
        self.routing_key = PREPROCESS_ROUTING_KEY

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



###################################################################
# Preprocess consumer operator Class - PreprocessConsumerOperator #
###################################################################

class PreprocessConsumerOperator(ConsumerOperator):
    """ 
    Contains management functionality for handling preprocessing job 
    consumptions from the "Preprocess" queue
    """

    def __init__(self, host: str, port: int):
        super().__init__(host=host, port=port)

        # General attributes


        # Network attributes
        self.routing_key = PREPROCESS_ROUTING_KEY
        self.queue = PREPROCESS_QUEUE

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