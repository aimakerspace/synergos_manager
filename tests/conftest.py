#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import json

# Libs
import pytest

# Custom
import synmanager

##################
# Configurations #
##################

HOST = '0.0.0.0'
PORT = '5672'
IS_SECURED = False
ADDRESS = f"https://{HOST}:{PORT}" if IS_SECURED else f"http://{HOST}:{PORT}"

collab_id = "test_collab"
project_id = "test_project"
expt_id_1 = "test_expt_1"
expt_id_2 = "test_expt_2"
run_id_1 = "test_run_1"
run_id_2 = "test_run_2"
participant_id_1 = "test_participant_1"
participant_id_2 = "test_participant_2"

COLLAB_KEY = {'collab_id': collab_id}
PROJECT_KEY = {**COLLAB_KEY, 'project_id': project_id}
EXPT_KEY_1 = {**PROJECT_KEY, 'expt_id': expt_id_1}
EXPT_KEY_2 = {**PROJECT_KEY, 'expt_id': expt_id_2}
RUN_KEY_1 = {**EXPT_KEY_1, 'run_id': run_id_1}
RUN_KEY_2 = {**EXPT_KEY_1, 'run_id': run_id_2}
RUN_KEY_3 = {**EXPT_KEY_2, 'run_id': run_id_2}
PARTICIPANT_KEY_1 = {'participant_id': participant_id_1}
PARTICIPANT_KEY_2 = {'participant_id': participant_id_2}
REG_KEY_1 = {**PROJECT_KEY, 'participant_id': participant_id_1}
REG_KEY_2 = {**PROJECT_KEY, 'participant_id': participant_id_2}
TAG_KEY_1 = {**PROJECT_KEY, 'participant_id': participant_id_1}
TAG_KEY_2 = {**PROJECT_KEY, 'participant_id': participant_id_2}

ACTION = "classify"

EXPT_RECORD = {
    "created_at": "{TinyDate}:2021-01-28 01:34:40 N",
    "key": EXPT_KEY_1,
    "model": [
        {
            "activation": "relu",
            "is_input": True,
            "l_type": "Conv2d",
            "structure": {
                "in_channels": 1,
                "kernel_size": 3,
                "out_channels": 4,
                "padding": 1,
                "stride": 1
            }
        },
        {
            "activation": None,
            "is_input": False,
            "l_type": "Flatten",
            "structure": {}
        },
        {
            "activation": "softmax",
            "is_input": False,
            "l_type": "Linear",
            "structure": {
                "bias": True,
                "in_features": 3136,
                "out_features": 3
            }
        }
    ]
}

RUN_RECORD_1 = {
    "algorithm": "FedProx",
    "base_lr": 0.0005,
    "created_at": "{TinyDate}:2021-01-28 01:34:40 N",
    "criterion": "NLLLoss",
    "delta": 0.0,
    "epochs": 2,
    "is_snn": False,
    "key": RUN_KEY_1,
    "l1_lambda": 0.0,
    "l2_lambda": 0.0,
    "lr": 0.001,
    "lr_decay": 0.1,
    "lr_scheduler": "CyclicLR",
    "max_lr": 0.005,
    "mu": 0.1,
    "optimizer": "SGD",
    "patience": 10,
    "precision_fractional": 5,
    "rounds": 5,
    "seed": 42,
    "weight_decay": 0.0
}

RUN_RECORD_2 = {
    "algorithm": "FedAvg",
    "base_lr": 0.0005,
    "created_at": "{TinyDate}:2021-01-28 01:34:40 N",
    "criterion": "NLLLoss",
    "delta": 0.0,
    "epochs": 30,
    "is_snn": False,
    "key": RUN_KEY_2,
    "l1_lambda": 0.0,
    "l2_lambda": 0.0,
    "lr": 0.001,
    "lr_decay": 0.1,
    "lr_scheduler": "CyclicLR",
    "max_lr": 0.005,
    "mu": 0.1,
    "optimizer": "SGD",
    "patience": 15,
    "precision_fractional": 5,
    "rounds": 7,
    "seed": 42,
    "weight_decay": 0.0
}

REGISTRATION_RECORDS = [
    {
        "created_at": "{TinyDate}:2021-01-28 01:34:40 N",
        "key": REG_KEY_1,
        "link": {
            "registration_id": "fd7836d4610811ebb8c60242ac110004"
        },
        "role": "guest"
    },
    {
        "created_at": "{TinyDate}:2021-01-28 01:34:40 N",
        "key": REG_KEY_2,
        "link": {
            "registration_id": "fd7f743a610811eba6e40242ac110004"
        },
        "role": "host"
    }
]

ALIGN_CONFIG = {
    'experiments': [EXPT_RECORD],
    'auto_align': True,
    'auto_fix': True
}

FEDERATED_CONFIG = {
    'action': ACTION,
    'experiments': [EXPT_RECORD],
    'runs': [RUN_RECORD_1, RUN_RECORD_2],
    'auto_align': False,
    'dockerised': True, 
    'log_msgs': False, 
    'verbose': False
}

TEST_MESSAGE_COUNT = 50
TEST_EXCHANGE = 'SynMQ_topic_logs'
TEST_ROUTING_KEY = 'SynMQ_topic_unittest'
TEST_QUEUE = 'unittest'

###########
# Helpers #
###########

def enumerate_federated_conbinations(
    action: str,
    experiments: list,
    runs: list,
    auto_align: bool = True,
    dockerised: bool = True,
    log_msgs: bool = True,
    verbose: bool = True,
    **kwargs
) -> dict:
    """ Enumerates all registered combinations of experiment models and run
        configurations for a SINGLE project in preparation for bulk operations.

    Args:
        action (str): Type of machine learning operation to be executed
        experiments (list): All experimental models to be reconstructed
        runs (dict): All hyperparameter sets to be used during grid FL inference
        auto_align (bool): Toggles if multiple feature alignments will be used
        dockerised (bool): Toggles if current FL grid is containerised or not. 
            If true (default), hosts & ports of all participants are locked at
            "0.0.0.0" & 8020 respectively. Otherwise, participant specified
            configurations will be used (grid architecture has to be finalised).
        log_msgs (bool): Toggles if messages are to be logged
        verbose (bool): Toggles verbosity of logs for WSCW objects
        **kwargs: Miscellaneous keyword argmuments to 
    Returns:
        Combinations (dict)
    """
    combinations = {}
    for expt_record in experiments:
        curr_expt_id = expt_record['key']['expt_id']

        for run_record in runs:
            run_key = run_record['key']
            collab_id = run_key['collab_id']
            project_id = run_key['project_id']
            expt_id = run_key['expt_id']
            run_id = run_key['run_id']

            if expt_id == curr_expt_id:

                combination_key = (collab_id, project_id, expt_id, run_id)
                combination_params = {
                    'keys': run_key,
                    'action': action,
                    'experiment': expt_record,
                    'run': run_record,
                    'auto_align': auto_align,
                    'dockerised': dockerised, 
                    'log_msgs': log_msgs, 
                    'verbose': verbose,
                    **kwargs
                }
                combinations[combination_key] = combination_params

    return combinations

######################
# Component Fixtures #
######################

@pytest.fixture
def init_params():
    return {
        'host': HOST, 
        'port': PORT, 
        'is_secured': IS_SECURED, 
        'address': ADDRESS
    }


@pytest.fixture
def test_kwargs():
    return FEDERATED_CONFIG


@pytest.fixture
def test_alignment():
    return ALIGN_CONFIG


@pytest.fixture
def test_message():
    return json.dumps(FEDERATED_CONFIG, default=str, sort_keys=True)


@pytest.fixture
def base_operator():
    return synmanager.base.BaseOperator(host=HOST, port=PORT)


@pytest.fixture
def producer_operator():
    producer = synmanager.base.ProducerOperator(host=HOST, port=PORT)
    producer.routing_key = TEST_ROUTING_KEY
    return producer


@pytest.fixture
def consumer_operator():
    consumer = synmanager.base.ConsumerOperator(host=HOST, port=PORT)
    consumer.routing_key = TEST_ROUTING_KEY
    consumer.queue = TEST_QUEUE
    consumer.auto_ack = False
    return consumer


@pytest.fixture
def preprocess_producer_operator():
    return synmanager.preprocess.PreprocessProducerOperator(
        host=HOST, 
        port=PORT
    )


@pytest.fixture
def preprocess_consumer_operator():
    return synmanager.preprocess.PreprocessConsumerOperator(        
        host=HOST, 
        port=PORT
    )


@pytest.fixture
def train_producer_operator():
    return synmanager.train.TrainProducerOperator(
        host=HOST, 
        port=PORT
    )


@pytest.fixture
def train_consumer_operator():
    return synmanager.train.TrainConsumerOperator(
        host=HOST, 
        port=PORT
    )


@pytest.fixture
def evaluate_producer_operator():
    return synmanager.evaluate.EvaluateProducerOperator(
        host=HOST, 
        port=PORT
    )


@pytest.fixture
def evaluate_consumer_operator():
    return synmanager.evaluate.EvaluateConsumerOperator(
        host=HOST, 
        port=PORT
    )


@pytest.fixture
def completed_producer_operator():
    return synmanager.completed.CompletedProducerOperator(
        host=HOST, 
        port=PORT
    )


@pytest.fixture
def completed_consumer_operator():
    return synmanager.completed.CompletedConsumerOperator(
        host=HOST, 
        port=PORT
    )