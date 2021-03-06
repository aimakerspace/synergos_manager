#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import time
from multiprocessing import Manager, Process

# Libs


# Custom
from synmanager.config import PREPROCESS_QUEUE
from conftest import PROJECT_KEY, TEST_MESSAGE_COUNT

##################
# Configurations #
##################


######################################
# Tests - PreprocessProducerOperator #
######################################

def test_PreprocessProducerOperator_publish_message(
    test_message, 
    preprocess_producer_operator
):
    """ Tests if message publishing to the `Preprocess` queue is valid.

    # C1: Check that a single message was published successfully 
    # C2: Check that the published message is indentical to original
    """
    preprocess_producer_operator.connect()
    preprocess_producer_operator.publish_message(test_message)

    # C1
    declared_queue = preprocess_producer_operator.channel.queue_declare(
        PREPROCESS_QUEUE, 
        passive=False, 
        durable=True
    )
    queue_message_count = declared_queue.method.message_count
    assert queue_message_count == 1
    # C2
    _, _, body = preprocess_producer_operator.channel.basic_get(
        queue=PREPROCESS_QUEUE, 
        auto_ack=True
    )
    assert body.decode() == test_message
    
    preprocess_producer_operator.disconnect()


def test_PreprocessProducerOperator_process(
    test_alignment,
    preprocess_producer_operator
):
    """ Tests if message generation is valid. Bulk messages are generated from
        a set of declared arguments and sent to the `Preprocess` queue.

    # C1: Check that declared arguments was decomposed into correct no. of jobs
    # C2: Check that N no. of messages wereh published
    # C2: Check that published message is valid
    """
    preprocess_producer_operator.connect()

    for _ in range(TEST_MESSAGE_COUNT):
        preprocess_producer_operator.process(**test_alignment)

    # C1
    declared_queue = preprocess_producer_operator.channel.queue_declare(
        PREPROCESS_QUEUE,
        passive=False, 
        durable=True
    )
    queue_message_count = declared_queue.method.message_count
    assert queue_message_count == TEST_MESSAGE_COUNT

    store = Manager().list()
    def message_callback(ch, method, properties, body):
        decoded_msg = body.decode()
        preprocess_kwargs = preprocess_producer_operator.parse_message(
            decoded_msg
        )
        store.append(preprocess_kwargs)

    preprocess_producer_operator.channel.basic_consume(
        queue=PREPROCESS_QUEUE,
        on_message_callback=message_callback,
        auto_ack=True
    )
    p = Process(target=preprocess_producer_operator.channel.start_consuming)
    p.start()
    
    while len(store) != TEST_MESSAGE_COUNT:
        time.sleep(1)

    # C2
    assert len(store) == TEST_MESSAGE_COUNT
    # C3
    assert list(store) == [test_alignment for _ in range(TEST_MESSAGE_COUNT)]

    p.terminate()
    p.join()
    p.close()
    preprocess_producer_operator.disconnect()