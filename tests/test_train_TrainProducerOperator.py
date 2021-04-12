#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import time
from multiprocessing import Manager, Process

# Libs


# Custom
from synmanager.config import TRAIN_QUEUE
from conftest import (
    PROJECT_KEY, RUN_RECORD_1, RUN_RECORD_2,
    enumerate_federated_conbinations
)

##################
# Configurations #
##################


#################################
# Tests - TrainProducerOperator #
#################################

def test_TrainProducerOperator_publish_message(
    test_message, 
    train_producer_operator
):
    """ Tests if message publishing to the `Train` queue is valid.

    # C1: Check that a single message was published successfully 
    # C2: Check that the published message is indentical to original
    """
    train_producer_operator.connect()
    train_producer_operator.publish_message(test_message)

    # C1
    declared_queue = train_producer_operator.channel.queue_declare(
        TRAIN_QUEUE, 
        passive=False, 
        durable=True
    )
    queue_message_count = declared_queue.method.message_count
    assert queue_message_count == 1
    # C2
    _, _, body = train_producer_operator.channel.basic_get(
        queue=TRAIN_QUEUE, 
        auto_ack=True
    )
    assert body.decode() == test_message
    
    train_producer_operator.disconnect()


def test_TrainProducerOperator_process(test_kwargs, train_producer_operator):
    """ Tests if message generation is valid. Bulk messages are generated from
        a set of declared arguments and sent to the `Train` queue.

    # C1: Check that declared arguments was decomposed into correct no. of jobs
    # C2: Check that published message is composed of a single job
    # C2: Check that published message is valid
    """
    train_producer_operator.connect()
    job_combinations = enumerate_federated_conbinations(**test_kwargs)
    for job_key, job_kwargs in job_combinations.items():
        train_producer_operator.process(**{
            'process': 'completed',   # operations filter for MQ consumer
            'combination_key': job_key,
            'combination_params': job_kwargs
        })

    # C1
    declared_queue = train_producer_operator.channel.queue_declare(
        TRAIN_QUEUE,
        passive=False, 
        durable=True
    )
    queue_message_count = declared_queue.method.message_count
    assert queue_message_count == 2

    store = Manager().list()
    def message_callback(ch, method, properties, body):
        decoded_msg = body.decode()
        train_kwargs = train_producer_operator.parse_message(decoded_msg)
        store.append(train_kwargs)

    train_producer_operator.channel.basic_consume(
        queue=TRAIN_QUEUE,
        on_message_callback=message_callback,
        auto_ack=True
    )
    p = Process(target=train_producer_operator.channel.start_consuming)
    p.start()
    
    while len(store) != 2:
        time.sleep(1)

    assert len(store) == 2
    for federated_config in store:
        # C2
        registered_run = federated_config['combination_params']['run']
        assert registered_run in [RUN_RECORD_1, RUN_RECORD_2]

    p.terminate()
    p.join()
    p.close()
    train_producer_operator.disconnect()