import pika
from functools import partial
import time

from varys.utils import init_logger, varys_message
from varys.process import Process

class consumer(Process):
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
    ):
        super().__init__()

        self._messages = message_queue

        self._log = init_logger(exchange, log_file, log_level)

        self._should_reconnect = reconnect
        self._reconnect_delay = 10
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        self._prefetch_count = prefetch_count

        self._exchange = exchange
        self._queue = exchange + "." + queue_suffix

        self._routing_key = routing_key
        self._sleep_interval = sleep_interval

        self._parameters = pika.ConnectionParameters(
            host=configuration.ampq_url,
            port=configuration.port,
            credentials=pika.PlainCredentials(
                username=configuration.username, password=configuration.password
            ),
        )

    def _on_connection_open_error(self, _unused_connection, err):
        self._log.error(f"Failed to connect to server due to error: {err}")
        self._reconnect()

    def _on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._log.warning(f"Connection closed, reconnect necessary: {reason}")
            self._reconnect()

    def _reconnect(self):
        if self._should_reconnect:
            self.stop()
            self._log.warning(f"Reconnecting after {self._reconnect_delay} seconds")
            time.sleep(self._reconnect_delay)
            self._connect()
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

    def _on_channel_closed(self, channel, reason):
        self._log.warning(f"Channel {channel} was closed: {reason}")
        self.close_connection()

    def _setup_queue(self, queue_name):
        self._log.info(f"Declaring queue: {queue_name}")
        q_callback = partial(self._on_queue_declareok, queue_name=queue_name)
        self._channel.queue_declare(
            queue=queue_name,
            callback=q_callback,
            durable=True,
        )

    def _on_queue_declareok(self, _unused_frame, queue_name):
        self._log.info(
            f"Binding queue {queue_name} to exchange: {self._exchange} with routing key {self._routing_key}"
        )
        self._channel.queue_bind(
            queue_name,
            self._exchange,
            routing_key=self._routing_key,
            callback=self._on_bindok,
        )

    def _on_bindok(self, _unused_frame):
        self._log.info("Queue bound successfully")
        self._set_qos()

    def _set_qos(self):
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self._on_basic_qos_ok
        )

    def _on_basic_qos_ok(self, _unused_frame):
        self._log.info(f"QOS set to: {self._prefetch_count}")
        self._start_consuming()

    def _start_consuming(self):
        self._log.info("Issuing consumer RPC commands")
        self._add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self._queue,
            self._on_message,
        )
        self._consuming = True

    def _add_on_cancel_callback(self):
        self._log.info("Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self._on_consumer_cancelled)

    def _on_consumer_cancelled(self, method_frame):
        self._log.info(
            f"Consumer cancelled remotely, now shutting down: {method_frame}"
        )
        if self._channel:
            self._channel.close()

    def _on_message(self, _unused_channel, basic_deliver, properties, body):
        message = varys_message(basic_deliver, properties, body)
        self._log.info(
            f"Received Message: # {message.basic_deliver.delivery_tag} from {message.properties.app_id}, {message.body}"
        )
        self._messages.put(message)
        self._acknowledge_message(message.basic_deliver.delivery_tag)

    def _acknowledge_message(self, delivery_tag):
        self._log.info(f"Acknowledging message: {delivery_tag}")
        self._channel.basic_ack(delivery_tag)

    def _stop_consuming(self):
        if self._channel:
            self._log.info(
                "Sending a Basic.Cancel command to central command (stopping message consumption)"
            )
            stop_consume_callback = partial(
                self._on_cancelok, consumer_tag=self._consumer_tag
            )
            self._channel.basic_cancel(self._consumer_tag, stop_consume_callback)

    def _on_cancelok(self, _unused_frame, consumer_tag):
        self._consuming = False
        self._log.info(
            f"Broker acknowledged the cancellation of the consumer: {consumer_tag}"
        )
        self._close_channel()

    def _close_channel(self):
        self._log.info("Closing the channel")
        self._channel.close()

    def run(self):
        self._connection = self._connect()
        self._connection.ioloop.start()

        return True

    def stop(self):
        if not self._closing:
            self._closing = True
            self._log.info("Stopping as instructed")
            if self._consuming:
                self._stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            self._log.info("Stopped as instructed")
