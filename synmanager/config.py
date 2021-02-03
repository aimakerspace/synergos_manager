#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic
import logging

# Libs

##################
# Configurations #
##################

# "Preprocess" Queue Settings
PREPROCESS_ROUTING_KEY = 'SynMQ_topic_preprocess'
PREPROCESS_QUEUE = 'preprocess'

# "Train" Queue Settings
TRAIN_ROUTING_KEY = 'SynMQ_topic_train'
TRAIN_QUEUE = 'train'

# "Evaluate" Queue Settings
EVALUATE_ROUTING_KEY = 'SynMQ_topic_evaluate'
EVALUATE_QUEUE = 'evaluate'

# "Completed" Queue Settings
COMPLETED_ROUTING_KEY = 'SynMQ_fanout_completed'
COMPLETED_EXCHANGE_NAME = 'SynMQ_fanout_logs'
COMPLETED_EXCHANGE_TYPE = 'fanout'
COMPLETED_QUEUE = 'completed'