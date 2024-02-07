import functools
import pika
import time

from pika.exchange_type import ExchangeType

from varys.utils import varys_message
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
        exchange_type,
        prefetch_count=5,
        sleep_interval=10,
        reconnect=True,
    ):
        super().__init__(
            message_queue,
            routing_key,
            exchange,
            configuration,
            log_file,
            log_level,
            queue_suffix,
            exchange_type,
            sleep_interval=sleep_interval,
        )

        self._should_reconnect = reconnect
        self._reconnect_delay = 10
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        self._prefetch_count = prefetch_count
        self._stopping = False

    def _on_message(self, _unused_channel, basic_deliver, properties, body):
        message = varys_message(basic_deliver, properties, body)
        self._log.info(
            f"Received Message: # {message.basic_deliver.delivery_tag} from {message.properties.app_id}, {message.body}"
        )
        self._message_queue.put(message)

    def _acknowledge_message(self, delivery_tag):
        self._log.info(f"Acknowledging message: {delivery_tag}")
        self._connection.add_callback_threadsafe(
            functools.partial(self._channel.basic_ack, delivery_tag)
        )

    def _nack_message(self, delivery_tag, requeue):
        self._log.info(f"Nacking message: {delivery_tag}")
        self._connection.add_callback_threadsafe(
            functools.partial(
                self._channel.basic_nack,
                delivery_tag=delivery_tag,
                multiple=False,
                requeue=requeue,
            )
        )

    def run(self):
        while True:
            try:
                self._connection = pika.BlockingConnection(self._parameters)
                self._channel = self._connection.channel()
                self._channel.exchange_declare(
                    exchange=self._exchange,
                    exchange_type=self._exchange_type,
                    durable=True,
                )
                self._channel.queue_declare(queue=self._queue, durable=True)
                self._channel.queue_bind(queue=self._queue, exchange=self._exchange, routing_key=self._routing_key)
                self._channel.basic_qos(prefetch_count=1)
                self._channel.basic_consume(self._queue, self._on_message, auto_ack=False)
                self._channel.start_consuming()
            except Exception as e:
                if self._stopping:
                    self._log.debug("Consumer caught exception while stopping as expected.")
                    break
                else:
                    self._log.warn("Consumer caught exception but not told to stop!")
                    continue

            break

    def stop(self):
        self._log.info("Stopping consumer as instructed...")
        self._stopping = True

        self._connection.add_callback_threadsafe(
            self._channel.stop_consuming
        )

        self._connection.add_callback_threadsafe(
            self._channel.close
        )

        self._connection.add_callback_threadsafe(
            self._connection.close
        )

        self._log.debug("Stopping consumer logger...")
        self._stop_logger()

        self._log.info("Stopped consumer as instructed.")
