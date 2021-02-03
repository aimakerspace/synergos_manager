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
from conftest import PROJECT_KEY, RUN_RECORD_1, RUN_RECORD_2

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
    train_producer_operator.connect()
    train_producer_operator.publish_message(test_message)

    declared_queue = train_producer_operator.channel.queue_declare(
        TRAIN_QUEUE, 
        passive=False, 
        durable=True
    )
    queue_message_count = declared_queue.method.message_count
    assert queue_message_count == 1
    
    _, _, body = train_producer_operator.channel.basic_get(
        queue=TRAIN_QUEUE, 
        auto_ack=True
    )
    assert body.decode() == test_message
    
    train_producer_operator.disconnect()


def test_TrainProducerOperator_process(test_kwargs, train_producer_operator):
    train_producer_operator.connect()
    train_producer_operator.process(**PROJECT_KEY, kwargs=test_kwargs)

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
        registered_runs = federated_config[PROJECT_KEY['project_id']]['runs']
        assert len(registered_runs) == 1
        assert registered_runs.pop() in [RUN_RECORD_1, RUN_RECORD_2]

    p.terminate()
    p.join()
    p.close()
    train_producer_operator.disconnect()