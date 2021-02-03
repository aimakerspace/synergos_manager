#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import time
from multiprocessing import Manager, Process

# Libs


# Custom
from conftest import PROJECT_KEY, TEST_QUEUE, RUN_RECORD_1, RUN_RECORD_2

##################
# Configurations #
##################


############################
# Tests - ProducerOperator #
############################

def test_ProducerOperator_publish_message(test_message, producer_operator):
    producer_operator.connect()
    producer_operator.publish_message(test_message)

    while not producer_operator.channel.is_open:
        time.sleep(1)

    declared_queue = producer_operator.channel.queue_declare(
        TEST_QUEUE, 
        passive=False, 
        durable=False
    )
    queue_message_count = declared_queue.method.message_count
    assert queue_message_count == 1
    
    _, _, body = producer_operator.channel.basic_get(
        queue=TEST_QUEUE, 
        auto_ack=True
    )

    assert body.decode() == test_message
    producer_operator.disconnect()


def test_ProducerOperator_process(test_kwargs, producer_operator):
    producer_operator.connect()
    producer_operator.process(**PROJECT_KEY, kwargs=test_kwargs)

    declared_queue = producer_operator.channel.queue_declare(
        TEST_QUEUE,
        passive=False, 
        durable=False
    )
    queue_message_count = declared_queue.method.message_count
    assert queue_message_count == 2

    store = Manager().list()
    def message_callback(ch, method, properties, body):
        decoded_msg = body.decode()
        kwargs = producer_operator.parse_message(decoded_msg)
        store.append(kwargs)

    producer_operator.channel.basic_consume(
        queue=TEST_QUEUE,
        on_message_callback=message_callback,
        auto_ack=True
    )
    p = Process(target=producer_operator.channel.start_consuming)
    p.start()
    
    while len(store) != 2:
        time.sleep(1)

    for federated_config in store:
        registered_runs = federated_config[PROJECT_KEY['project_id']]['runs']
        assert len(registered_runs) == 1
        assert registered_runs.pop() in [RUN_RECORD_1, RUN_RECORD_2]

    p.terminate()
    p.join()
    p.close()
    producer_operator.disconnect()