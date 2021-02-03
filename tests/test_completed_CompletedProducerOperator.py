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
from conftest import PROJECT_KEY, RUN_RECORD_1, RUN_RECORD_2

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
    completed_producer_operator.connect()
    completed_producer_operator.publish_message(test_message)

    declared_queue = completed_producer_operator.channel.queue_declare(
        COMPLETED_QUEUE, 
        passive=False, 
        durable=True
    )
    queue_message_count = declared_queue.method.message_count
    assert queue_message_count == 1
    
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
    completed_producer_operator.connect()
    completed_producer_operator.process(**PROJECT_KEY, kwargs=test_kwargs)

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
        registered_runs = federated_config[PROJECT_KEY['project_id']]['runs']
        assert len(registered_runs) == 1
        assert registered_runs.pop() in [RUN_RECORD_1, RUN_RECORD_2]

    p.terminate()
    p.join()
    p.close()
    completed_producer_operator.disconnect()