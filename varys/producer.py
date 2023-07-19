import pika
from pika.exchange_type import ExchangeType

from threading import Thread
import time
import queue
import json

from varys.utils import init_logger


class producer(Thread):
    def __init__(
        self,
        message_queue,
        exchange,
        configuration,
        log_file,
        log_level,
        queue_suffix,
        routing_key="arbitrary_string",
        sleep_interval=10,
    ):
        # username, password, queue, ampq_url, port, log_file, exchange="", routing_key="default", sleep_interval=5
        Thread.__init__(self)

        self._log = init_logger(exchange, log_file, log_level)

        self._message_queue = message_queue

        self._connection = None
        self._channel = None
        self._sleep_interval = sleep_interval

        self._exchange = exchange
        self._exchange_type = ExchangeType.fanout
        self._queue = exchange + "." + queue_suffix
        self._routing_key = routing_key

        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None

        self._stopping = False

        self._parameters = pika.ConnectionParameters(
            host=configuration.ampq_url,
            port=configuration.port,
            credentials=pika.PlainCredentials(
                username=configuration.username, password=configuration.password
            ),
        )

        self._message_properties = pika.BasicProperties(
            content_type="json", delivery_mode=2
        )

    def __connect(self):
        self._log.info("Connecting to broker")
        return pika.SelectConnection(
            self._parameters,
            on_open_callback=self.__on_connection_open,
            on_open_error_callback=self.__on_connection_open_error,
            on_close_callback=self.__on_connection_closed,
        )

    def __on_connection_open(self, _unused_connection):
        self._log.info("Connection to broker successfully opened")
        self.__open_channel()

    def __on_connection_open_error(self, _unused_connection, error):
        self._log.error(
            f"Connection attempt to broker failed, attempting to re-open in {self._sleep_interval} seconds: {error}"
        )
        self._connection.ioloop.stop()
        time.sleep(self._sleep_interval)
        self._connection = self.__connect()
        self._connection.ioloop.start()

    def __on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            self._log.warning(
                f"Connection to broker closed, will attempt to re-connect in 10 seconds: {reason}"
            )
            self._connection.ioloop.call_later(10, self._connection.ioloop.start)

    def __open_channel(self):
        self._log.info("Creating a new channel")
        self._connection.channel(on_open_callback=self.__on_channel_open)

    def __on_channel_open(self, channel):
        self._log.info("Channel successfully opened")
        self._channel = channel
        self.__add_on_channel_close_callback()
        self.__setup_exchange(self._exchange)

    def __add_on_channel_close_callback(self):
        self._log.info("Adding channel close callback")
        self._channel.add_on_close_callback(self.__on_channel_closed)

    def __on_channel_closed(self, channel, reason):
        self._log.warning(f"Channel {channel} was closed by broker: {reason}")
        self._channel = None
        if not self._stopping:
            self._connection.close()

    def __setup_exchange(self, exchange_name):
        self._log.info(f"Declaring exchange: {exchange_name}")
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self._exchange_type,
            callback=self.__on_declare_exchangeok,
            durable=True,
        )

    def __on_declare_exchangeok(self, _unused_frame):
        self._log.info("Exchange declared")
        self.__setup_queue(self._queue)

    def __setup_queue(self, queue):
        self._log.info(f"Declaring queue {queue}")
        self._channel.queue_declare(
            queue=queue, durable=True, callback=self.__on_queue_declareok
        )

    def __on_queue_declareok(self, _unused_frame):
        self._log.info(
            f"Binding {self._exchange} to {self._queue} with {self._routing_key}"
        )
        self._channel.queue_bind(
            self._queue,
            self._exchange,
            routing_key=self._routing_key,
            callback=self.__on_bindok,
        )

    def __on_bindok(self, _unused_frame):
        self._log.info("Queue successfully bound")
        self.__start_publishing()

    def __start_publishing(self):
        self._log.info(
            "Issuing consumer delivery confirmation commands and sending first message"
        )
        self.__enable_delivery_confirmations()
        self.__send_if_queued()

    def __enable_delivery_confirmations(self):
        self._log.info("Issuing Confirm.Select RPC command")
        self._channel.confirm_delivery(self.__on_delivery_confirmation)

    def __on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split(".")[1].lower()
        self._log.info(
            f"Received {confirmation_type} for delivery tag: {method_frame.method.delivery_tag}"
        )
        if confirmation_type == "ack":
            self._acked += 1
        elif confirmation_type == "nack":
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        self._log.info(
            f"Published {self._message_number} messages, {len(self._deliveries)} have yet to be confirmed, "
            f"{self._acked} were acked, {self._nacked} were nacked"
        )

    def __send_if_queued(self):
        try:
            to_send = self._message_queue.get(block=False)
            self.publish_message(to_send)
        except queue.Empty:
            self._connection.ioloop.call_later(
                self._sleep_interval, self.__send_if_queued
            )

    def __close_channel(self):
        if self._channel is not None:
            self._log.info("Closing the channel")
            self._channel.close()

    def __close_connection(self):
        if self._connection is not None:
            self._log.info("Closing connection")
            self._channel.close()

    def publish_message(self, message):
        if self._channel is None or not self._channel.is_open:
            return False

        try:
            message_str = json.dumps(message, ensure_ascii=False)
        except TypeError:
            self._log.error(f"Unable to serialise message into json: {str(message)}")

        self._log.info(f"Sending message: {json.dumps(message)}")
        self._channel.basic_publish(
            self._exchange,
            self._routing_key,
            message_str,
            self._message_properties,
            mandatory=True,
        )

        self._message_number += 1
        self._deliveries.append(self._message_number)
        self._message_queue.task_done()
        self._log.info(f"Published message # {self._message_number}")

        self.__send_if_queued()

    def run(self):
        while not self._stopping:
            self._connection = None
            self._deliveries = []
            self._acked = 0
            self._nacked = 0
            self._message_number = 0

            try:
                self._connection = self.__connect()
                self._connection.ioloop.start()
            except KeyboardInterrupt:
                self.stop()
                if self._connection is not None and not self._connection.is_closed:
                    # Finish closing
                    self._connection.ioloop.stop()

            return True

    def stop(self):
        self._log.info("Stopping publisher")
        self._stopping = True
        self.__close_channel()
        self.__close_connection()
