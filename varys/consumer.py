import pika
from pika.exchange_type import ExchangeType
from threading import Thread
from functools import partial
import time
import ssl

from varys.utils import init_logger, varys_message


class consumer(Thread):
    exchange_type = ExchangeType.fanout

    def __init__(
        self,
        message_queue,
        routing_key,
        exchange,
        configuration,
        log_file,
        log_level,
        queue_suffix,
        prefetch_count=5,
        sleep_interval=10,
        reconnect=True,
        use_ssl=True,
    ):
        Thread.__init__(self)

        self._messages = message_queue

        self._log = init_logger(exchange, log_file, log_level)

        self._should_reconnect = reconnect
        self._reconnect_delay = 10
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        self._prefetch_count = prefetch_count

        self._exchange = exchange
        self._queue = exchange + "." + queue_suffix

        self._routing_key = routing_key
        self._sleep_interval = sleep_interval

        if use_ssl:
            # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")

            self._parameters = pika.ConnectionParameters(
                host=configuration.ampq_url,
                port=configuration.port,
                credentials=pika.PlainCredentials(
                    username=configuration.username, password=configuration.password
                ),
                ssl=True,
                ssl_options=pika.SSLOptions(ssl_context),
            )
        else:
            self._parameters = pika.ConnectionParameters(
                host=configuration.ampq_url,
                port=configuration.port,
                credentials=pika.PlainCredentials(
                    username=configuration.username, password=configuration.password
                ),
            )

    def __connect(self):
        return pika.SelectConnection(
            parameters=self._parameters,
            on_open_callback=self.__on_connection_open,
            on_open_error_callback=self.__on_connection_open_error,
            on_close_callback=self.__on_connection_closed,
        )

    def __on_connection_open(self, _unused_connection):
        self._log.info(f"Successfully connected to server")
        self.__open_channel()

    def __on_connection_open_error(self, _unused_connection, err):
        self._log.error(f"Failed to connect to server due to error: {err}")
        self.__reconnect()

    def __on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._log.warning(f"Connection closed, reconnect necessary: {reason}")
            self.__reconnect()

    def __reconnect(self):
        if self._should_reconnect:
            self.stop()
            self._log.warning(f"Reconnecting after {self._reconnect_delay} seconds")
            time.sleep(self._reconnect_delay)
            self.__connect()
        else:
            self._log.info(
                f"Reconnection was not set to re-connect after disconnection so closing"
            )

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            self._log.info("Connection is closing or already closed")
        else:
            self._log.info("Closing connection")
            self._connection.close()

    def __open_channel(self):
        self._log.info("Creating a new channel")
        self._connection.channel(on_open_callback=self.__on_channel_open)

    def __on_channel_open(self, channel):
        self._channel = channel
        self._log.info("Channel opened")
        self.__add_on_channel_close_callback()
        self.__setup_exchange(self._exchange)

    def __add_on_channel_close_callback(self):
        self._log.info("Adding channel on closed callback")
        self._channel.add_on_close_callback(self.__on_channel_closed)

    def __on_channel_closed(self, channel, reason):
        self._log.warning(f"Channel {channel} was closed: {reason}")
        self.close_connection()

    def __setup_exchange(self, exchange_name):
        self._log.info(f"Declaring exchange - {exchange_name}")
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.exchange_type,
            callback=self.__on_exchange_declareok,
            durable=True,
        )

    def __on_exchange_declareok(self, _unused_frame):
        self._log.info("Exchange successfully declared")
        self.__setup_queue(self._queue)

    def __setup_queue(self, queue_name):
        self._log.info(f"Declaring queue: {queue_name}")
        q_callback = partial(self.__on_queue_declareok, queue_name=queue_name)
        self._channel.queue_declare(
            queue=queue_name,
            callback=q_callback,
            durable=True,
        )

    def __on_queue_declareok(self, _unused_frame, queue_name):
        self._log.info(
            f"Binding queue {queue_name} to exchange: {self._exchange} with routing key {self._routing_key}"
        )
        self._channel.queue_bind(
            queue_name,
            self._exchange,
            routing_key=self._routing_key,
            callback=self.__on_bindok,
        )

    def __on_bindok(self, _unused_frame):
        self._log.info("Queue bound successfully")
        self.__set_qos()

    def __set_qos(self):
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.__on_basic_qos_ok
        )

    def __on_basic_qos_ok(self, _unused_frame):
        self._log.info(f"QOS set to: {self._prefetch_count}")
        self.__start_consuming()

    def __start_consuming(self):
        self._log.info("Issuing consumer RPC commands")
        self.__add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self._queue,
            self.__on_message,
        )
        self._consuming = True

    def __add_on_cancel_callback(self):
        self._log.info("Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self.__on_consumer_cancelled)

    def __on_consumer_cancelled(self, method_frame):
        self._log.info(
            f"Consumer cancelled remotely, now shutting down: {method_frame}"
        )
        if self._channel:
            self._channel.close()

    def __on_message(self, _unused_channel, basic_deliver, properties, body):
        message = varys_message(basic_deliver, properties, body)
        self._log.info(
            f"Received Message: # {message.basic_deliver.delivery_tag} from {message.properties.app_id}, {message.body}"
        )
        self._messages.put(message)
        self.__acknowledge_message(message.basic_deliver.delivery_tag)

    def __acknowledge_message(self, delivery_tag):
        self._log.info(f"Acknowledging message: {delivery_tag}")
        self._channel.basic_ack(delivery_tag)

    def __stop_consuming(self):
        if self._channel:
            self._log.info(
                "Sending a Basic.Cancel command to central command (stopping message consumption)"
            )
            stop_consume_callback = partial(
                self.__on_cancelok, consumer_tag=self._consumer_tag
            )
            self._channel.basic_cancel(self._consumer_tag, stop_consume_callback)

    def __on_cancelok(self, _unused_frame, consumer_tag):
        self._consuming = False
        self._log.info(
            f"Broker acknowledged the cancellation of the consumer: {consumer_tag}"
        )
        self.__close_channel()

    def __close_channel(self):
        self._log.info("Closing the channel")
        self._channel.close()

    def run(self):
        self._connection = self.__connect()
        self._connection.ioloop.start()

        return True

    def stop(self):
        if not self._closing:
            self._closing = True
            self._log.info("Stopping as instructed")
            if self._consuming:
                self.__stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            self._log.info("Stopped as instructed")
