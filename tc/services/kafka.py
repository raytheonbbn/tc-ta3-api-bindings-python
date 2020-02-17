# Copyright (c) 2020 Raytheon BBN Technologies Corp.
# See LICENSE.txt for details.

"""
Example classes for Kafka producers and consumers.
"""

import logging
from threading import Thread
import time
import random

import confluent_kafka

from tc.schema.serialization import Utils
from tc.schema.serialization.kafka import KafkaAvroGenericDeserializer
from tc.schema.serialization.kafka import KafkaAvroGenericSerializer
from tc.schema.records.record_generator import RecordGeneratorFactory

LOG_TRACE = logging.DEBUG - 1


class Producer(Thread):
    """
    Kafka producer that sends dummy records with unique IDs to the Kafka
    broker cluster.  Partitions are chosen by hashing over a provided key.
    """

    _END_OF_TIME = 0xFFFFFFFFFFFFFFFF
    _LOGGER = logging.getLogger(__name__ + ".Producer")

    def __init__(
            self, server_address, producer_id, topic, is_async, duration,
            delay, record_file, producer_schema_filename, num_kv_pairs, 
            security_protocol=None, ca_location=None, cert_location=None,
            key_location=None, key_pass=None, skip_validate=True):
        """Create a simple producer.

        :param server_address:
        :param producer_id: ID for this producer to use.
        :param topic: Topic to produce records to.
        :param is_async: Whether or not to send records asyncronously.
        :param duration: Amount of seconds to produce for.
        :param delay: Number of milliseconds to delay between records.
        :param record_file: File to produce records from.
        :param producer_schema_filename: Filename for producer schema.
        :param num_kv_pairs: Number of arbitrary key-value paris to append.
        """

        super(Producer, self).__init__()
        self.topic = topic
        self.producer_id = producer_id
        self.is_async = is_async
        self.duration = duration
        self._delay = delay * .001
        self.record_file = record_file
        self.record_num = 1
        self.producer_schema_filename = producer_schema_filename
        self.num_kv_pairs = num_kv_pairs

        self.max_mb = -1
        self.max_records = -1
        self.static_records = False
        self.no_avro = False

        # Handle a sigint shutdown cleanly.
        self._shutdown = False

        # Initialize a serializer for Kafka.
        self.serializer = KafkaAvroGenericSerializer(producer_schema_filename, skip_validate)

        config = {}
        config["bootstrap.servers"] = server_address
        config["api.version.request"] = True
        config["client.id"] = self.producer_id
        if security_protocol:
            if security_protocol.lower() == "ssl":
                config["security.protocol"] = security_protocol
                config["ssl.ca.location"] = ca_location
                config["ssl.certificate.location"] = cert_location
                config["ssl.key.location"] = key_location
                config["ssl.key.password"] = key_pass
            elif security_protocol.lower() == "plaintext":
                config["security.protocol"] = security_protocol
            else:
                msg = "Unsupported security protocol type for TC APIs: " + security_protocol
                raise ValueError(msg)

        self.producer = confluent_kafka.Producer(config)

    def throughput_stats_init(self, maxMB, maxRecords, recordSize,
                              static_records):
        """Initialize the throughput statistics initialization."""
        self.max_mb = maxMB
        self.max_records = maxRecords
        self.static_records = static_records
        self.record_size = recordSize

    def set_no_avro(self):
        """Setter for the no_avro flag."""
        self.no_avro = True

    @property
    def delay(self):
        return self._delay

    @delay.setter
    def delay(self, delay):
        # Convert from integer number of ms to float representation of seconds.
        self._delay = delay * .001

    def shutdown(self):
        """Perform a clean shutdown of this producer."""
        self._shutdown = True

    def run(self):
        """Produce messages for the specified duration.

        Produce messages for a specified duration based on the provided schema.
        """
        end_time = Producer._get_end_time(self.duration)
        Producer._LOGGER.info("Starting producer.")

        # Create some nodes with properties.
        edgeGen = RecordGeneratorFactory.get_record_generator(self.serializer)
        edge = edgeGen.generate_random_record(self.num_kv_pairs)

        msg = "Serialized record size: {size}".format(size=self.record_size)
        Producer._LOGGER.info(msg)

        if self.no_avro:
            message = self.serializer.serialize(self.topic, edge)

        edge_size = -1
        elapsed_start = Producer._current_time_millis()
        window_start = elapsed_start

        count = 0
        window_count = 0

        total_bytes = 0
        window_bytes = 0

        reporting_interval = 5000  # MS, 5 sec

        # Terminate either if we get a SIGINT signal or if the duration
        # runs out.
        now = Producer._current_time_millis()
        while not self._shutdown and \
              now <= end_time and \
              (self.max_records == -1 or count < self.max_records):

            # Provide a key based on the current millisecond time.  Note
            # that this must be of type bytes, and not an integer.
            kafka_key = str(now).encode()

            # if no_avro is on, we won't serialize and use the same record
            if not self.no_avro:
                message = self.serializer.serialize(self.topic, edge)

            # If we don't know the message length, call len here
            if edge_size == -1:
                edge_size = len(message)

            if Producer._LOGGER.isEnabledFor(LOG_TRACE):
                msg = "Attempting to send record k: {key}, value: {value}" \
                      .format(key=kafka_key, value=self.serializer.serialize_to_json(edge))
                Producer._LOGGER.log(LOG_TRACE, msg)

            # Produce our message to the Kafka cluster.
            try:
                self.producer.produce(self.topic, value=message, key=kafka_key)
                count += 1
                if Producer._LOGGER.isEnabledFor(logging.DEBUG):
                    msg = "Sent message: ({count}) with key {key}".format(
                        count=count, key=kafka_key)
                    Producer._LOGGER.debug(msg)
            except BufferError as e:
                Producer._LOGGER.warn("Local producer queue is full.  Dropping message")
                # FIXME: should we block here and flush to risk losing data further upstream,
                # or should we just tail drop here (which is what this does now)?

            self.producer.poll(0)

            # Sleep for specified delay between producing records.
            if self.delay > 0:
                time.sleep(self.delay)

            # Update stats
            window_count += 1
            window_bytes += edge_size
            total_bytes += edge_size

            if (now - window_start) >= reporting_interval:
                self.report_stats(window_start, window_count,
                                  window_bytes, True)
                window_count = 0
                window_bytes = 0
                window_start = Producer._current_time_millis()

            # Update the records to make them look like new ones.
            if not self.static_records:
                edge = edgeGen.generate_random_record(self.num_kv_pairs)
                edge_size = -1  # We don't know the actual size anymore

            now = Producer._current_time_millis()

        # Do a blocking flush.
        Producer._LOGGER.debug("Waiting for local queue to flush before closing...")
        self.producer.flush()
        # Stop the producer so that our thread (and application) can terminate.
        Producer._LOGGER.debug("Waiting for thread to stop...")
        if window_count > 0:
            self.report_stats(window_start, window_count, window_bytes, True)
        self.report_stats(elapsed_start, count, total_bytes, False)
        Producer._LOGGER.info("Closed producer.")

    def report_stats(self, window_start, window_count,
                     window_bytes, window_flag):
        """Report statistics that were collected.

        :param window_start: Time at which measurement window started.
        :param window_count: Count of messages which were measured.
        :param window_bytes: Bytes which were measured.
        :param window_flag: Window flag.
        """
        elapsed = Producer._current_time_millis() - window_start
        recs_per_sec = 1000.0 * window_count / elapsed
        mb_per_sec = 1000.0 * window_bytes / elapsed / (1024.0 * 1024.0)
        if window_flag:
            msg = ("window {rsent} records sent, {bytes} bytes sent,"
                   "{recsec} records/sec ({mbsec} MB/sec)").format(
                    rsent=window_count, bytes=window_bytes, recsec=recs_per_sec, mbsec=mb_per_sec)
            Producer._LOGGER.info(msg)
        else:
            msg = ("{rsent} records sent, "
                   "{recsec} records/sec ({mbsec} MB/sec)").format(
                    rsent=window_count, recsec=recs_per_sec, mbsec=mb_per_sec)
            Producer._LOGGER.info(msg)

    @staticmethod
    def _get_end_time(duration):
        if duration > 0:
            return Producer._current_time_millis() + duration
        else:
            return Producer._END_OF_TIME

    @staticmethod
    def _current_time_millis():
        return int(time.time() * 1000)


class Consumer(Thread):
    """
    Simple Kafka consumer that fetches dummy records from a all partitions in
    a provided topic in the Kafka broker cluster.
    """

    _END_OF_TIME = 0xFFFFFFFFFFFFFFFF
    _LOGGER = logging.getLogger(__name__ + ".Consumer")
    _DEFAULT_CONSUME_TIMEOUT = 1
    _DEFAULT_SESSION_TIMEOUT_MS = 6000

    def __init__(
            self, kafka_server, group_id, topic, duration, consume_all,
            consumer_schema_filename, producer_schema_filename, auto_offset,
            security_protocol=None, ca_cert=None, cert_location=None,
            key_location=None, key_pass=None, session_timeout=_DEFAULT_SESSION_TIMEOUT_MS):
        """Create a simple consumer.

        :param kafka_server: Connection string for bootstrap Kafka server.
        :param group_id: Group ID to use for distributed consumers.
        :param topic: Topic to consume from.
        :param duration: Duration to run for.
        :param consumer_schema_filename: Filename for consumer schema.
        :param producer_schema_filename: Filename for producer schema.
        :param auto_offset: Offset reset method to use for consumers.
        """
        super(Consumer, self).__init__()
        self.kafka_server = kafka_server
        self.group_id = group_id
        self.topic = topic
        self.duration = duration
        self.consume_all = consume_all
        self.consumer_schema_filename = consumer_schema_filename
        self.producer_schema_filename = producer_schema_filename
        self.serializer = KafkaAvroGenericSerializer(self.consumer_schema_filename)
        self.deserializer = KafkaAvroGenericDeserializer(
                self.consumer_schema_filename, self.producer_schema_filename)
        self.auto_offset = auto_offset
        self.consume_timeout = Consumer._DEFAULT_CONSUME_TIMEOUT

        # Handle a sigint shutdown cleanly.
        self._shutdown = False

        config = {}
        config["bootstrap.servers"] = self.kafka_server
        config["group.id"] = self.group_id
        config["session.timeout.ms"] = session_timeout

        if security_protocol:
            if security_protocol.lower() == "ssl":
                config["security.protocol"] = security_protocol
                config["ssl.ca.location"] = ca_cert
                config["ssl.certificate.location"] = cert_location
                config["ssl.key.location"] = key_location
                config["ssl.key.password"] = key_pass
            elif security_protocol.lower() == "plaintext":
                config["security.protocol"] = security_protocol
            else:
                msg = "Unsupported security protocol type for TC APIs: " + security_protocol
                raise ValueError(msg)

        default_topic_config = {}
        default_topic_config["auto.offset.reset"] = self.auto_offset
        config["default.topic.config"] = default_topic_config

        self.consumer = confluent_kafka.Consumer(config)
        self.consumer.subscribe([self.topic])
        self.latency_stats = Utils.Stats(
                1, "End-to-End Latency (including Avro serialization)", "ms")

    def throughput_stats_init(self, maxMB, maxRecords, recordSize):
        """Initialize variables before statistics collection."""
        self.max_mb = maxMB
        self.max_records = maxRecords
        self.record_size = recordSize

    def shutdown(self):
        """Perform a clean shutdown of this consumer."""
        self._shutdown = True

    def run(self):
        """Consume messages for the specified duration.

        Consumer messages for a specified duration.  Depending on the log
        level, messages will be printed out as they are consumed.  Finally,
        a report will be printed showing statistics collected during the run.
        """

        if self.consume_all:
            # Initial duration (before we have consumed a single record is 5x)
            # This lets us account for some startup delay
            end_time = Consumer._get_end_time(self.duration * 5)
        else:
            end_time = Consumer._get_end_time(self.duration)

        count = 0
        Consumer._LOGGER.info("Started consumer.")
        if self.max_records > 0:
            Consumer._LOGGER.info("Receiving up to " + str(self.max_records) +
                    " records, please wait...")
        else:
            Consumer._LOGGER.info("Receiving records, please wait...")

        # Terminate either if we get a SIGINT signal or if the duration
        # runs out.
        now = Consumer._current_time_millis()
        start_time = now

        while not self._shutdown and \
              now <= end_time and \
              (self.max_records == -1 or count < self.max_records):

            kafka_message = self.consumer.poll(self.consume_timeout)
            if kafka_message and not kafka_message.error():
                if kafka_message.value():
                    if self.consume_all:
                        # Reset the timeout clock
                        end_time = Consumer._get_end_time(self.duration)

                    message = self.deserializer.deserialize(self.topic,
                            kafka_message.value())

                    # Sample the end to end latency.
                    if kafka_message.key():
                        try:
                            sent_ts = int(kafka_message.key())
                            time_diff = Consumer._current_time_millis() - sent_ts
                            self.latency_stats.sample(time_diff)
                            if Consumer._LOGGER.isEnabledFor(logging.DEBUG):
                                msg = ("Consumed a message {key} count: "
                                       "{edges} edges").format(
                                        key=kafka_message.key(),
                                        edges=count)
                                Consumer._LOGGER.debug(msg)

                            if Consumer._LOGGER.isEnabledFor(LOG_TRACE):
                                msg = "Consumed Record: {value}".format(
                                        value=self.serializer.serialize_to_json(message))
                                Consumer._LOGGER.log(LOG_TRACE, msg)

                        except ValueError:
                            pass

                    else:
                        if Consumer._LOGGER.isEnabledFor(logging.DEBUG):
                            msg = ("Consumed a message count: "
                                   "{edges} edges").format(
                                    edges=count)
                            Consumer._LOGGER.debug(msg)

                        if Consumer._LOGGER.isEnabledFor(LOG_TRACE):
                            msg = "Consumed Record: {value}".format(
                                    value=self.serializer.serialize_to_json(message))
                            Consumer._LOGGER.log(LOG_TRACE, msg)

                    count += 1

                else:
                    if kafka_message.key() is not None:
                        msg = "Received null record {key}".format(
                                key=kafka_message.key())
                        Consumer._LOGGER.debug(msg)
                    else:
                        msg = "Received null record"
                        Consumer._LOGGER.debug(msg)

                now = Consumer._current_time_millis()
                if self._shutdown or \
                        now > end_time or \
                        (self.max_records > -1 and
                         count >= self.max_records):
                    break

            # If our consumer times out because it didn't receive a message,
            # note it and move on.
            elif not kafka_message or kafka_message.error().code() == confluent_kafka.KafkaError.REQUEST_TIMED_OUT:
                Consumer._LOGGER.debug("Comsumer timeout reached.")

            elif kafka_message.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                Consumer._LOGGER.info("End of partition reached.")

            # If our consumer tried to consume from an offset that doesn't
            # exist yet, note it and move on.
            elif kafka_message.error().code() == confluent_kafka.KafkaError.OFFSET_OUT_OF_RANGE:
                Consumer._LOGGER.debug("Offset out of range.")

            now = Consumer._current_time_millis()

        # Upon completion, print report.
        msg = "Duration {dur} (sec) elapsed. Printing stats.".format(
                dur=self.duration)
        Consumer._LOGGER.info(msg)
        Consumer._LOGGER.info(str(self.latency_stats))

        now = Consumer._current_time_millis()
        elapsed = now - start_time
        Consumer._LOGGER.info("Real elapsed time: " + str(elapsed))
        Consumer._LOGGER.info("Record Count: " + str(count))
        if self.record_size > -1:
            rbytes = count * self.record_size
            mb = rbytes / (1024.0 * 1024.0)
            Consumer._LOGGER.info("Consumed MB: " + str(mb))
            Consumer._LOGGER.info("Consumed MB/s: " +
                    str(mb / (elapsed / 1000.0)))

        # Clean up the consumer.
        Consumer._LOGGER.info("Closing consumer session...")
        self.consumer.close()
        Consumer._LOGGER.info("Consumer session closed.")
        Consumer._LOGGER.info("Done.")

    @staticmethod
    def _get_end_time(duration):
        if duration > 0:
            return Consumer._current_time_millis() + duration
        else:
            return Consumer._END_OF_TIME

    @staticmethod
    def _current_time_millis():
        return int(time.time() * 1000)
