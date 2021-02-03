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
    assert type(test_kwargs) == dict
    message_str = base_operator.create_message(run_kwarg=test_kwargs)
    assert type(message_str) == str
    assert json.loads(message_str) == test_kwargs


def test_BaseOperator_parse_message(test_message, base_operator):
    assert type(test_message) == str
    parsed_kwargs = base_operator.parse_message(message=test_message)
    assert type(parsed_kwargs) == dict
    assert json.dumps(parsed_kwargs) == test_message


def test_BaseOperator_connect(base_operator):
    base_operator.connect()
    assert base_operator.connection.is_open
    base_operator.connection.close()
    assert base_operator.connection.is_closed


def test_BaseOperator_disconnect(base_operator):
    parameters = pika.ConnectionParameters(host=base_operator.host)
    test_connection = pika.BlockingConnection(parameters)
    test_channel = test_connection.channel()
    base_operator.connection = test_connection
    base_operator.channel = test_channel

    assert base_operator.connection.is_open and base_operator.channel.is_open

    base_operator.disconnect()
    assert base_operator.channel is None
    assert not test_channel.is_open 
    assert base_operator.connection is None
    assert not test_connection.is_open


