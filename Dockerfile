FROM rabbitmq:3.8.11-management

ADD rabbitmq.config /etc/rabbitmq/
ADD definitions.json /etc/rabbitmq/
