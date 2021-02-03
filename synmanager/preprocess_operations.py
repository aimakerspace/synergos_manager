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
    def __init__(self, host: str = None):
        super().__init__(host=host)

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

    def process(self, project_id: str) -> Dict[str, str]:
        """ Sends message for project_id to poll involved participants

        Args:
            project_id (str): Project ID of the target project to operate on
        Returns:
            Message payload (dict)
        """
        preprocess_kwarg = {'project_id' : project_id}
        message = self.create_message(preprocess_kwarg)
        self.publish_message(message)

        return preprocess_kwarg



###################################################################
# Preprocess consumer operator Class - PreprocessConsumerOperator #
###################################################################

class PreprocessConsumerOperator(ConsumerOperator):
    """ 
    Contains management functionality for handling preprocessing job 
    consumptions from the "Preprocess" queue
    """
    def __init__(self, host: str = None):
        super().__init__(host=host)

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