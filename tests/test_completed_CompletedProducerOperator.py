#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import time
from multiprocessing import Manager, Process

# Libs


# Custom
from synmanager.config import COMPLETED_QUEUE
from conftest import (
    PROJECT_KEY, RUN_RECORD_1, RUN_RECORD_2,
    enumerate_federated_conbinations
)

##################
# Configurations #
##################


#####################################
# Tests - CompletedProducerOperator #
#####################################

def test_CompletedProducerOperator_publish_message(
    test_message, 
    completed_producer_operator
):
    """ Tests if message publishing to the `Completed` queue is valid.

    # C1: Check that a single message was published successfully 
    # C2: Check that the published message is indentical to original
    """
    completed_producer_operator.connect()
    completed_producer_operator.publish_message(test_message)

    # C1
    declared_queue = completed_producer_operator.channel.queue_declare(
        COMPLETED_QUEUE, 
        passive=False, 
        durable=True
    )
    queue_message_count = declared_queue.method.message_count
    assert queue_message_count == 1
    # C2
    _, _, body = completed_producer_operator.channel.basic_get(
        queue=COMPLETED_QUEUE, 
        auto_ack=True
    )
    assert body.decode() == test_message
    
    completed_producer_operator.disconnect()


def test_CompletedProducerOperator_process(
    test_kwargs, 
    completed_producer_operator
):
    """ Tests if message generation is valid. Bulk messages are generated from
        a set of declared arguments and sent to the `Completed` queue.

    # C1: Check that declared arguments was decomposed into correct no. of jobs
    # C2: Check that published message is composed of a single job
    """
    completed_producer_operator.connect()

    job_combinations = enumerate_federated_conbinations(**test_kwargs)
    for job_key, job_kwargs in job_combinations.items():
        completed_producer_operator.process(**{
            'process': 'completed',   # operations filter for MQ consumer
            'combination_key': job_key,
            'combination_params': job_kwargs
        })

    # C1
    declared_queue = completed_producer_operator.channel.queue_declare(
        COMPLETED_QUEUE,
        passive=False, 
        durable=True
    )
    queue_message_count = declared_queue.method.message_count
    assert queue_message_count == 2

    store = Manager().list()
    def message_callback(ch, method, properties, body):
        decoded_msg = body.decode()
        kwargs = completed_producer_operator.parse_message(decoded_msg)
        store.append(kwargs)

    completed_producer_operator.channel.basic_consume(
        queue=COMPLETED_QUEUE,
        on_message_callback=message_callback,
        auto_ack=True
    )
    p = Process(target=completed_producer_operator.channel.start_consuming)
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
    completed_producer_operator.disconnect()