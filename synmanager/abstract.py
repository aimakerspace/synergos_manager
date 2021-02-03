#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import abc
import logging
from typing import Dict, List, Tuple, Union

# Libs


# Custom


##################
# Configurations #
##################


##############################################
# Operator Abstract Class - AbstractOperator #
##############################################

class AbstractOperator(abc.ABC):

    @abc.abstractmethod
    def connect(self):
        """ Creates an operation payload to be sent to a remote queue for 
            linearising jobs for a Synergos cluster
        """
        pass


    @abc.abstractmethod
    def process(self):
        """ Sends an operation payload to a remote queue for linearising jobs 
            for a Synergos cluster
        """
        pass
    

    @abc.abstractmethod
    def delete(self):
        """ Removes an operation payload that had been sent to a remote queue 
            for job linearisation
        """
        pass


    @abc.abstractmethod
    def disconnect(self):
        """ Closes current channel & termiates connection with RabbitMQ 
            exchange where queues exist
        """
        pass