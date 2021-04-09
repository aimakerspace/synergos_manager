#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic/Built-in
import json
import logging
from typing import Dict, List, Callable

# Libs
import pika

# Custom
from .abstract import AbstractOperator

##################
# Configurations #
##################

logging.getLogger("pika").setLevel(logging.WARNING) # reduce log level
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)

######################################
# Base Operator Class - BaseOperator #
######################################

class BaseOperator(AbstractOperator):
    """ Contains baseline functionality to all queue related operations. An 
        operator is an entity that facilitates communication with a registered
        message queue, effectively linearising job submissions from multiple 
        sources. These jobs would then be consumed across multiple grid
        networks, allowing for optimised distributed parallellisation.

    Attributes:
        host (str): Address where queue is hosted on
        port (int): Port where queue is hosted on
        channel (pika.channel.Channel): Communication method used
        connection (pika.connection.Connection): Connection on which to 
            communicate with deployed RabbitMQ server
        exchange_name (str): Name of exchange to operate on
        exchange_type (str): Type of exchange (i.e. "direct", "fanout", "topic",
            "headers"). Default: "topic"
        durability (bool): Toggles if persistent messages are to be re-declared 
            when broker restarts after it had been taken down
        routing_key (str): Message attribute of header
    """
    def __init__(self, host: str, port: int):
        # General attributes
        self.host = host
        self.port = port
        
        # Network attributes
        self.channel = None
        self.connection = None
        self.exchange_name = 'SynMQ_topic_logs'
        self.exchange_type = 'topic'
        self.routing_key = 'default'
        self.durability = True
        

        # Data attributes
        # e.g participant_id/run_id in specific format

        # Optimisation attributes
        # e.g multiprocess/asyncio if necessary for optimisation

        # Export Attributes 
        # e.g. any artifacts that are going to be exported eg Records

    
    ############
    # Checkers #
    ############

    def is_connected(self) -> bool:
        """ Checks if an operator is ready for publishing jobs
        
        Returns:
            True    if ready
            False   otherwise
        """
        return self.channel and self.connection

    ###########    
    # Helpers #
    ###########

    def create_message(self, run_kwarg: dict) -> str:
        """ Creates an operation payload to be sent to a remote queue for 
            linearising jobs for a Synergos cluster

        Args:
            run_kwargs
        Returns:
            Message string (str)
        """
        return json.dumps(run_kwarg, default=str, sort_keys=True)
        
    
    def parse_message(self, message: str) -> dict:
        """ Decodes string message to dictionary

        Args:
            message (str)
        Returns:
            Parsed JSON (dict)
        """ 
        return json.loads(message)

        # also need to do the unstr() of our msg
        # string representation of TinyDate() must be converted back 
        # to the same date format that was from database.json with start_proc 

    ##################
    # Core Functions #
    ##################

    def connect(self):
        """ Initiate connection with RabbitMQ exchange where queues exist """
        if not self.is_connected(): 
            parameters = pika.ConnectionParameters(
                host=self.host, 
                port=self.port
            )
            self.connection = pika.BlockingConnection(parameters)

            self.channel = self.connection.channel()
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type=self.exchange_type,
                durable=self.durability
            )
            self.channel.confirm_delivery()


    def process(self):
        """ Sends an operation payload to a remote queue for linearising jobs 
            for a Synergos cluster
        """
        raise NotImplementedError


    def delete(self):
        """ Removes an operation payload that had been sent to a remote queue 
            for job linearisation
        """
        raise NotImplementedError


    def disconnect(self):
        """ Closes current channel & termiates connection with RabbitMQ 
            exchange where queues exist 
        """ 
        if self.is_connected():
            self.channel.close()
            self.connection.close()
            self.connection = None
            self.channel = None

            

##########################################
# Base Operator Class - ProducerOperator #
##########################################

class ProducerOperator(BaseOperator):
    """ Contains baseline functionality for all types of message producers in 
        Synergos. Producers populate the message queue with jobs.

    Attributes:
        host (str): Address where queue is hosted on
        port (int): Port where queue is hosted on
        channel (pika.channel.Channel): Communication method used
        connection (pika.connection.Connection): Connection on which to 
            communicate with deployed RabbitMQ server
        exchange_name (str): Name of exchange to operate on
        exchange_type (str): Type of exchange (i.e. "direct", "fanout", "topic",
            "headers"). Default: "topic"
        durability (bool): Toggles if persistent messages are to be re-declared 
            when broker restarts after it had been taken down
        routing_key (str): Message attribute of header
    """
    def __init__(self, host: str, port: int):
        super().__init__(host=host, port=port)
    

    ###########    
    # Helpers #
    ###########

    def publish_message(self, message: str):
        """ Publish single message specified queue in exchange
        
        Args:
            message (str): Message to be published
        """
        if not self.is_connected():
            raise RuntimeError("Operator is not connected! Run '.connect()' and try again!")

        try:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=self.routing_key,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2) # persist msgs
            )
            logging.info('Message publish was confirmed')

        except pika.exceptions.UnroutableError:
            logging.info('Message could not be confirmed')

    ##################
    # Core Functions #
    ##################

    def process(self, project_id: str, kwargs: dict) -> List[str]:
        """ Splits kwargs into individual messages, one message for each run.
            Returns number of messages published with publish_message()

        Args:
            project_id (str): Project ID of the target project to operate on
            kwargs (dict): Configurations to be decomposed into single runs
        """
        if not self.is_connected():
            raise RuntimeError("Operator is not connected! Run '.connect()' and try again!")

        run_ids = []
        for experiment in kwargs['experiments']:
            
            curr_expt_id = experiment['key']['expt_id']
            
            for run in kwargs['runs']:
                
                if run['key']['expt_id'] == curr_expt_id:
                    run_kwarg = kwargs.copy()
                    run_kwarg['experiments'] = [experiment]
                    run_kwarg['runs'] = [run]

                    message = self.create_message({project_id: run_kwarg})
                    self.publish_message(message)
                    run_ids.append(run['key']['run_id'])

        return run_ids



##########################################
# Base Operator Class - ConsumerOperator #
##########################################

class ConsumerOperator(BaseOperator):
    """ Contains baseline functionality for all types of message consumers in 
        Synergos. Consumers extract jobs from the message queues for processing.

    Attributes:
        host (str): Address where queue is hosted on
        port (int): Port where queue is hosted on
        channel (pika.channel.Channel): Communication method used
        connection (pika.connection.Connection): Connection on which to 
            communicate with deployed RabbitMQ server
        exchange_name (str): Name of exchange to operate on
        exchange_type (str): Type of exchange (i.e. "direct", "fanout", "topic",
            "headers"). Default: "topic"
        durability (bool): Toggles if persistent messages are to be re-declared 
            when broker restarts after it had been taken down
        routing_key (str): Message attribute of header
        queue (str): Name of queue to listen on
        auto_ack (bool): Toggles if a message should be acknowledged before 
            callback process conpletion. If True, RMQ server is receives 
            message ack before process completion, and is not robust against 
            crashes. If False, RMQ server will only receive message ack after
            process is completed & any intermittent failures will result in 
            lost messages being restored after restart
    """
    def __init__(self, host: str, port: int):
        super().__init__(host=host, port=port)
        
        # General attributes


        # Network attributes
        self.queue = None
        self.auto_ack = False

        # Data attributes
        # e.g participant_id/run_id in specific format


        # Optimisation attributes
        # e.g multiprocess/asyncio if necessary for optimisation


        # Export Attributes 
        # e.g. any artifacts that are going to be exported eg Records


    ###########    
    # Helpers #
    ###########

    def __bind_consumer(self):
        """ Bind consumer to queue """
        self.channel.queue_bind(
            exchange=self.exchange_name,
            queue=self.queue,
            routing_key=self.routing_key
        )
    

    def generate_callback(self, process_function: Callable, host: str):
        
        logging.debug(f"Process function specified for callback: {process_function}")

        def message_callback(ch, method, properties, body):
            """ Callback function to execute when message received by consumer 
            
            Args:
                ch: Channel over which the communication is happening
                method: Meta information regarding the message delivery
                properties: User-defined properties on the message
                body: Additional data
            """
            decoded_msg = body.decode()
            logging.info(f" [x] {method.routing_key} - Received: {decoded_msg}")

            kwargs = self.parse_message(decoded_msg)
            completed_trainings = process_function(kwargs, host)
            logging.info(f" [x] {method.routing_key} - Process completed")

            ch.basic_ack(delivery_tag=method.delivery_tag) # manual acknowledgement
            logging.info(f" [x] {method.routing_key} - Delivered: {completed_trainings}")
        
        return message_callback

        
    def listen_message(self, process_function: Callable):
        """ Commence message consumption from queue on current consumer. This
            opens a long running channel that listens to a specific queue, in
            contrast with `.poll_message(...)` which only consumes a single
            message.

        Args:
            process_function (Callable): Callback function to be executed with
                arguments retrieved from queue.
        """
        if not self.is_connected():
            raise RuntimeError("Operator is not connected! Run '.connect()' and try again!")

        self.__bind_consumer()

        self.channel.basic_consume(
            queue=self.queue,
            on_message_callback=self.generate_callback(
                process_function=process_function, 
                host=self.host
            ),
            auto_ack=self.auto_ack
        )

        logging.info(f"Listening from {self.queue} queue: ")
        self.channel.start_consuming()


    def poll_message(self, process_function: Callable):
        """ Synchronous call to the broker for an individual message. This only 
            consumes a single message, in contrast with `.listen_message(...)` 
            which opens a long running channel that listens to a specific queue. 
            
        Args:
            process_function (Callable): Callback function to be executed with
                arguments retrieved from queue.
        """
        if not self.is_connected():
            raise RuntimeError("Operator is not connected! Run '.connect()' and try again!")

        method, properties, body = self.channel.basic_get(
            queue=self.queue, 
            auto_ack=self.auto_ack
        )
        
        if body:
            message_callback = self.generate_callback(
                process_function=process_function, 
                host=self.host
            )
            message_callback(self.channel, method, properties, body)

        else:
            logging.info(f"No message received in {self.queue}")


    def check_message_count(self) -> int:
        """ Check for the no. of remaining messages waiting in queue
        
        Returns:
            Queue message count (int)
        """
        if not self.is_connected():
            raise RuntimeError("Operator is not connected! Run '.connect()' and try again!")

        declared_queue = self.channel.queue_declare(
            self.queue, 
            passive=True, 
            durable=self.durability
        )
        queue_message_count = declared_queue.method.message_count

        return queue_message_count

    ##################
    # Core Functions #
    ##################

    def connect(self):
        """ Initiate connection with RabbitMQ exchange while configuring a
            prefetch threshold to prevent consumers from overloading
        """
        super().connect()
        self.channel.basic_qos(
            prefetch_size=0,   # no message size limit
            prefetch_count=10  # max no. of messages to accumulate
        )