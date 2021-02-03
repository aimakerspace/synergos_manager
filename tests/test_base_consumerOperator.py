#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import logging
import time
from multiprocessing import Process, Manager

# Libs
import pika

# Custom
from conftest import (TEST_MESSAGE_COUNT, TEST_EXCHANGE, TEST_ROUTING_KEY)

##################
# Configurations #
##################


############################
# Tests - ConsumerOperator #
############################

def test_ConsumerOperator_listen_message(test_message, consumer_operator):
    consumer_operator.connect()

    # Publish X test messages
    for _ in range(TEST_MESSAGE_COUNT):
        consumer_operator.channel.basic_publish(
            exchange=TEST_EXCHANGE,
            routing_key=TEST_ROUTING_KEY,
            body=test_message,
            properties=pika.BasicProperties(delivery_mode=2)  
        )

    store = Manager().list()
    def test_archival_process(kwargs, host, archive=store):
        assert type(kwargs) == dict
        archive.append(kwargs)

    p = Process(
        target=consumer_operator.listen_message,
        args=(test_archival_process,)
    )
    p.start()

    while len(store) < TEST_MESSAGE_COUNT:
        logging.info(f"Current store: {store}")
        time.sleep(1)

    assert len(store) == TEST_MESSAGE_COUNT
    assert list(store) == [
        consumer_operator.parse_message(test_message) 
        for _ in range(TEST_MESSAGE_COUNT)
    ]
    
    consumer_operator.disconnect()
    p.terminate()
    p.join()
    p.close()


def test_ConsumerOperator_poll_message(
    test_message,
    test_kwargs,
    consumer_operator
):
    consumer_operator.connect()

    # Publish 1 message
    consumer_operator.channel.basic_publish(
        exchange=TEST_EXCHANGE,
        routing_key=TEST_ROUTING_KEY,
        body=test_message,
        properties=pika.BasicProperties(delivery_mode=2)  
    )

    store = Manager().list()
    def test_archival_process(kwargs, host, archive=store):
        archive.append(kwargs)

    consumer_operator.poll_message(process_function=test_archival_process)
    assert len(store) == 1
    assert store.pop() == test_kwargs
    
    consumer_operator.disconnect()

    
def test_ConsumerOperator_check_message_count(test_message, consumer_operator):
    consumer_operator.connect()
    assert consumer_operator.check_message_count() == 0

    # Publish X test messages
    for _ in range(TEST_MESSAGE_COUNT):
        consumer_operator.channel.basic_publish(
            exchange=TEST_EXCHANGE,
            routing_key=TEST_ROUTING_KEY,
            body=test_message,
            properties=pika.BasicProperties(delivery_mode=2)  
        )
    
    assert consumer_operator.check_message_count() == TEST_MESSAGE_COUNT
    consumer_operator.channel.queue_purge(consumer_operator.queue)
    consumer_operator.disconnect()