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
from conftest import TEST_MESSAGE_COUNT

##################
# Configurations #
##################


####################################
# Tests - EvaluateConsumerOperator #
####################################

def test_EvaluateConsumerOperator_listen_message(
    test_message,
    evaluate_consumer_operator
):
    evaluate_consumer_operator.connect()

    # Publish X test messages
    for _ in range(TEST_MESSAGE_COUNT):
        evaluate_consumer_operator.channel.basic_publish(
            exchange=evaluate_consumer_operator.exchange_name,
            routing_key=evaluate_consumer_operator.routing_key,
            body=test_message,
            properties=pika.BasicProperties(delivery_mode=2)  
        )

    store = Manager().list()
    def test_archival_process(kwargs, host, archive=store):
        assert type(kwargs) == dict
        archive.append(kwargs)

    p = Process(
        target=evaluate_consumer_operator.listen_message,
        args=(test_archival_process,)
    )
    p.start()

    while len(store) < TEST_MESSAGE_COUNT:
        logging.info(f"Current store: {store}")
        time.sleep(1)

    assert len(store) == TEST_MESSAGE_COUNT
    assert list(store) == [
        evaluate_consumer_operator.parse_message(test_message) 
        for _ in range(TEST_MESSAGE_COUNT)
    ]
    
    p.terminate()
    p.join()
    p.close()
    evaluate_consumer_operator.disconnect()


def test_EvaluateConsumerOperator_poll_message(
    test_message,
    test_kwargs,
    evaluate_consumer_operator
):
    evaluate_consumer_operator.connect()

    # Publish 1 message
    evaluate_consumer_operator.channel.basic_publish(
        exchange=evaluate_consumer_operator.exchange_name,
        routing_key=evaluate_consumer_operator.routing_key,
        body=test_message,
        properties=pika.BasicProperties(delivery_mode=2)  
    )

    store = Manager().list()
    def test_archival_process(kwargs, host, archive=store):
        archive.append(kwargs)

    evaluate_consumer_operator.poll_message(
        process_function=test_archival_process
    )
    assert len(store) == 1
    assert store.pop() == test_kwargs
    
    evaluate_consumer_operator.disconnect()

    
def test_EvaluateConsumerOperator_check_message_count(
    test_message, 
    evaluate_consumer_operator
):
    evaluate_consumer_operator.connect()
    assert evaluate_consumer_operator.check_message_count() == 0

    # Publish X test messages
    for _ in range(TEST_MESSAGE_COUNT):
        evaluate_consumer_operator.channel.basic_publish(
            exchange=evaluate_consumer_operator.exchange_name,
            routing_key=evaluate_consumer_operator.routing_key,
            body=test_message,
            properties=pika.BasicProperties(delivery_mode=2)  
        )
    
    assert evaluate_consumer_operator.check_message_count() == TEST_MESSAGE_COUNT
    evaluate_consumer_operator.channel.queue_purge(evaluate_consumer_operator.queue)
    evaluate_consumer_operator.disconnect()