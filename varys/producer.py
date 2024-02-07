import functools
import pika
import time
import queue
import json

from varys.process import Process


class producer(Process):
    def __init__(
        self,
        message_queue,
        exchange,
        configuration,
        log_file,
        log_level,
        queue_suffix,
        exchange_type,
        routing_key="arbitrary_string",
        sleep_interval=10,
    ):
        # username, password, queue, ampq_url, port, log_file, exchange="", routing_key="default", sleep_interval=5
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

        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None

        self._stopping = False

        self._message_properties = pika.BasicProperties(
            content_type="json", delivery_mode=pika.DeliveryMode.Persistent,
        )

    # def publish_message(self, message):
    #     if self._channel is None or not self._channel.is_open:
    #         return False

    #     try:
    #         message_str = json.dumps(message, ensure_ascii=False)
    #     except TypeError:
    #         self._log.error(f"Unable to serialise message into json: {str(message)}")

    #     self._log.info(f"Sending message: {json.dumps(message)}")
    #     self._channel.basic_publish(
    #         self._exchange,
    #         self._routing_key,
    #         message_str,
    #         self._message_properties,
    #         mandatory=True,
    #     )

    #     self._message_number += 1
    #     self._deliveries.append(self._message_number)
    #     self._message_queue.task_done()
    #     self._log.info(f"Published message # {self._message_number}")

    #     self._send_if_queued()

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
                # time_limit=None leads to the connection being dropped for inactivity
                # not sure if this should be while not self._stopping
                while True:
                    self._connection.process_data_events(time_limit=1)
            except:
                if self._stopping:
                    self._log.debug("Producer caught exception while stopping as expected.")
                    break
                else:
                    self._log.warn("Producer caught exception but not told to stop!")
                    # time.sleep(1))
                    continue

            break

    def stop(self):
        self._log.info("Stopping producer as instructed...")
        # probably have to say we're closing so run doesn't try to reopen connection
        self._stopping = True

        self._connection.add_callback_threadsafe(
            functools.partial(self._connection.process_data_events, time_limit=1)
        )

        self._connection.add_callback_threadsafe(
            self._channel.close
        )
        self._connection.add_callback_threadsafe(
            self._connection.close
        )

        self._log.debug("Stopping producer logger...")
        self._stop_logger()

        self._log.info("Stopped producer as instructed.")
