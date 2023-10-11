from threading import Thread

import pika


class Process(Thread):
    def __init__(self):
        super().__init__()

        Thread.daemon = True

        self._connection = None
        self._channel = None

    def _connect(self):
        self._log.info("Connecting to broker")
        return pika.SelectConnection(
            self._parameters,
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed,
        )

    def _on_connection_open(self, _unused_connection):
        self._log.info("Successfully opened connection to broker")
        self._open_channel()

    def _open_channel(self):
        self._log.info("Creating a new channel")
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel):
        self._log.info("Channel successfully opened")
        self._channel = channel
        self._add_on_channel_close_callback()
        self._setup_exchange(self._exchange)

    def _add_on_channel_close_callback(self):
        self._log.info("Adding channel close callback")
        self._channel.add_on_close_callback(self._on_channel_closed)

    def _setup_exchange(self, exchange_name):
        self._log.info(f"Declaring exchange: {exchange_name}")
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self._exchange_type,
            callback=self._on_declare_exchangeok,
            durable=True,
        )

    def _on_declare_exchangeok(self, _unused_frame):
        self._log.info("Exchange declared")
        self._setup_queue(self._queue)
