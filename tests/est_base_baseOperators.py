#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import json

# Libs
import pika

# Custom


##################
# Configurations #
##################


########################
# Tests - BaseOperator #
########################

def test_BaseOperator_create_message(test_kwargs, base_operator):
    """ Tests if message creation is valid. This is when runtime arguments are
        converted into a message string to be streamed to the message queue.

    # C1: Check that specified runtime arguments are valid
    # C2: Check that created message string is valid
    # C3: Check that decoded message is equivalent to the original arguments
    """
    # C1
    assert type(test_kwargs) == dict
    # C2
    message_str = base_operator.create_message(run_kwarg=test_kwargs)
    assert type(message_str) == str
    # C3
    assert json.loads(message_str) == test_kwargs


def test_BaseOperator_parse_message(test_message, base_operator):
    """ Tests if message parsing is valid. This is used when a message string 
        is decoded into dictionary arguments used to initialize jobs retrieved
        from the allocated message queue.

    # C1: Check that specified message to be decoded is valid
    # C2: Check that decoded arguments is valid
    # C3: Check that decoded arguments is equivalent to the original arguments
    """
    # C1
    assert type(test_message) == str
    # C2
    parsed_kwargs = base_operator.parse_message(message=test_message)
    assert type(parsed_kwargs) == dict
    # C3
    assert json.dumps(parsed_kwargs) == test_message


def test_BaseOperator_connect(base_operator):
    """ Tests if connection intialization with RabbitMQ exchange is valid.

    # C1: Check that upon connection, connection is now open
    # C2: Check that state changes when connection is closed
    """
    # C1
    base_operator.connect()
    assert base_operator.connection.is_open
    # C2
    base_operator.connection.close()
    assert base_operator.connection.is_closed


def test_BaseOperator_disconnect(base_operator):
    """ Tests if connection intialization with RabbitMQ exchange is valid.

    # C1: Check that connection & channel are manually initialized successfully 
    # C2: Check that upon disconnection, channel object is destroyed & resetted
    # C3: Check that channel is no longer open
    # C4: Check that upon disconnection, connection object is destroyed & resetted
    # C5: Check that connection no longer exists
    """
    parameters = pika.ConnectionParameters(host=base_operator.host)
    test_connection = pika.BlockingConnection(parameters)
    test_channel = test_connection.channel()
    base_operator.connection = test_connection
    base_operator.channel = test_channel

    # C1
    assert base_operator.connection.is_open and base_operator.channel.is_open
    # C2
    base_operator.disconnect()
    assert base_operator.channel is None
    # C3
    assert not test_channel.is_open 
    # C4
    assert base_operator.connection is None
    # C5
    assert not test_connection.is_open


