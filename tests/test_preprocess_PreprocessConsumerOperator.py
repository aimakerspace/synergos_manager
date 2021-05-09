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
from conftest import PROJECT_KEY, TEST_MESSAGE_COUNT

##################
# Configurations #
##################


######################################
# Tests - PreprocessConsumerOperator #
######################################

# def test_PreprocessConsumerOperator_listen_message(
#     preprocess_consumer_operator
# ):
#     """ Tests if long term message retrieval from the `Preprocess` queue is 
#         valid. Bulk messages are manually published to the queue, and checked 
#         if properly retrieved.

#     # C1: Check that runtime parameters retrieved from queue are valid
#     # C2: Check that all messages were successfully retrieved
#     # C3: Check that all decoded messages are identical to their originals
#     """
#     preprocess_consumer_operator.connect()

#     # Publish X test messages
#     test_message = preprocess_consumer_operator.create_message(PROJECT_KEY)
#     for _ in range(TEST_MESSAGE_COUNT):
#         preprocess_consumer_operator.channel.basic_publish(
#             exchange=preprocess_consumer_operator.exchange_name,
#             routing_key=preprocess_consumer_operator.routing_key,
#             body=test_message,
#             properties=pika.BasicProperties(delivery_mode=2)  
#         )

#     store = Manager().list()
#     def test_archival_process(kwargs, host, archive=store):
#         # C1
#         assert type(kwargs) == dict
#         archive.append(kwargs)

#     p = Process(
#         target=preprocess_consumer_operator.listen_message,
#         args=(test_archival_process,)
#     )
#     p.start()

#     while len(store) < TEST_MESSAGE_COUNT:
#         logging.info(f"Current store: {store}")
#         time.sleep(1)

#     # C2
#     assert len(store) == TEST_MESSAGE_COUNT
#     # C3
#     assert list(store) == [
#         preprocess_consumer_operator.parse_message(test_message) 
#         for _ in range(TEST_MESSAGE_COUNT)
#     ]
    
#     p.terminate()
#     p.join()
#     p.close()
#     preprocess_consumer_operator.disconnect()


def test_PreprocessConsumerOperator_poll_message(preprocess_consumer_operator):
    """ Tests if single message retrieval from the `Preprocess` queue is valid. 
        Bulk messages are manually published to the queue, and checked if 
        properly retrieved.

    # C1: Check that a single set of runtime parameters is retrieved from queue
    # C2: Check that the retrieved message is identical to the original
    """
    preprocess_consumer_operator.connect()

    # Publish 1 message
    test_message = preprocess_consumer_operator.create_message(PROJECT_KEY)
    preprocess_consumer_operator.channel.basic_publish(
        exchange=preprocess_consumer_operator.exchange_name,
        routing_key=preprocess_consumer_operator.routing_key,
        body=test_message,
        properties=pika.BasicProperties(delivery_mode=2)  
    )

    store = Manager().list()
    def test_archival_process(archive=store, **kwargs):
        archive.append(kwargs)

    # C1
    preprocess_consumer_operator.poll_message(
        process_function=test_archival_process
    )
    assert len(store) == 1
    # C2
    assert store.pop() == PROJECT_KEY
    
    preprocess_consumer_operator.disconnect()

    
def test_PreprocessConsumerOperator_check_message_count(
    test_message, 
    preprocess_consumer_operator
):
    """ Tests if state checking of the `Preprocess` queue is valid. This queries
        for the number of messages currently left in the queue.

    # C1: Check that before publishing messages, no. of messages in queue is 0
    # C2: Check that after publishing messages, no. of messages in queue is right
    """
    # C1
    preprocess_consumer_operator.connect()
    assert preprocess_consumer_operator.check_message_count() == 0

    # Publish X test messages
    test_message = preprocess_consumer_operator.create_message(PROJECT_KEY)
    for _ in range(TEST_MESSAGE_COUNT):
        preprocess_consumer_operator.channel.basic_publish(
            exchange=preprocess_consumer_operator.exchange_name,
            routing_key=preprocess_consumer_operator.routing_key,
            body=test_message,
            properties=pika.BasicProperties(delivery_mode=2)  
        )
    
    # C2
    assert preprocess_consumer_operator.check_message_count() == TEST_MESSAGE_COUNT
    preprocess_consumer_operator.channel.queue_purge(preprocess_consumer_operator.queue)
    preprocess_consumer_operator.disconnect()