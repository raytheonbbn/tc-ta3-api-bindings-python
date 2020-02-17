#!/usr/bin/python

# Copyright (c) 2020 Raytheon BBN Technologies Corp.
# See LICENSE.txt for details.

"""
A demo cli tool for creating a producer and/or consumer, depending on the
provided command line arguments.
"""

import argparse
import logging
from logging.config import fileConfig

from tc.schema.serialization import Utils
from tc.services.kafka import Consumer, Producer
from tc.services import kafka
from tc.schema.serialization.kafka import KafkaAvroGenericSerializer
from tc.schema.records.record_generator import RecordGeneratorFactory

# Default values
KAFKA_SERVER = "localhost:9092"
PRODUCER_ID = "demoProducer"
NO_PRODUCER = False
IS_ASYNC = False
DURATION = 0
MAX_MB = -1
MAX_RECORDS = -1
DELAY = 1
NEW_CONSUMER = False
GROUP_ID = "demo-group"
NO_CONSUMER = False
#SECURITY_PROTOCOL = "SSL"
SECURITY_PROTOCOL = None
CA_LOCATION = "/var/private/ssl/ca-cert"
CERT_LOCATION = "/var/private/ssl/kafka.client.pem"
KEY_LOCATION = "/var/private/ssl/kafka.client.key"
KEY_PASS = "TransparentComputing"

def get_arg_parser():
    """ Create an ArgumentParser instance for this cli tool. """
    # Instantiate argument parser
    parser = argparse.ArgumentParser(
            description="Run a Kafka producer and/or consumer.")
    parser.add_argument("topic", action="store", type=str,
            help="Kafka topic to use for this run.")
    parser.add_argument("-nc", action="store_true", default=NO_CONSUMER,
            help="Do not enable the consumer thread.")
    parser.add_argument("-np", action="store_true", default=NO_PRODUCER,
            help="Do not enable the producer thread.")
    parser.add_argument("-as", dest="async", action="store_true",
            default=IS_ASYNC, help="Enable asychronous producing.")
    parser.add_argument("-v", action="store_true", default=False,
            help="Turn up verbosity.")
    parser.add_argument("-vv", action="store_true", default=False,
            help="Turn up verbosity further.")
    parser.add_argument("-pid", action="store", type=str, default=PRODUCER_ID,
            help="Manually set the producer ID.")
    parser.add_argument("-psf", action="store", type=str,
            help="Set the producer's schema file.")
    parser.add_argument("-csf", action="store", type=str,
            help="Set the consumer's schema file.")
    parser.add_argument("-g", action="store", type=str, default=GROUP_ID,
            help="Set the group ID.")
    parser.add_argument("-ks", action="store", type=str, default=KAFKA_SERVER,
            help="The Kafka server's connection string.")
    parser.add_argument("-d", action="store", type=int, default=DURATION,
            help="Duration for the producer to run.")
    parser.add_argument("-mb", action="store", type=int, default=MAX_MB,
            help="max MB of data to publish and generate throughput " +
                 "statistics (default -1, no limit, use duration)")
    parser.add_argument("-mr", action="store", type=int, default=MAX_RECORDS,
            help="max Records to publish (default -1, no limit)")
    parser.add_argument("-sr", action="store_true", default=False,
            help="static record data, publish the same record data each " +
                 "time (default false, random record data)")
    parser.add_argument("-co", action="store", type=str, default="latest",
            help="consumer property AUTO_OFFSET_RESET_CONFIG (default latest)")
    parser.add_argument("-call", action="store_true", default=False,
            help="consumer all records that are available, don't start the timeout" +
                 "clock until there's nothing left")
    parser.add_argument("-offset", action="store", type=str, default="",
            help="set specific offset for each partition at first assignment")
    parser.add_argument("-rg", action="store_true", default=False,
            help="random consumer group (default false, use -g value)")
    parser.add_argument("-delay", action="store", type=int, default=DELAY,
            help="Producer publish delay between sends in ms (default 1).")
    parser.add_argument("-n", action="store", type=int,
            help="Number of key/value property pairs to append to records.")
    parser.add_argument("-noavro", action="store_true", default=False,
            help="Don't do avro serialization, instead produce the same " +
                 "bytes each time.  This is only for throughput testing")
    parser.add_argument("-ev", action="store_true", default=False,
            help="Run an explicit Validate on each record before serializing it")
    parser.add_argument("-sp", action="store", type=str, default=SECURITY_PROTOCOL,
            help="Security protocol for connection")
    parser.add_argument("-ca", action="store", type=str, default=CA_LOCATION,
            help="Location of certificate authority")
    parser.add_argument("-cl", action="store", type=str, default=CERT_LOCATION,
            help="Location of client certificate")
    parser.add_argument("-kl", action="store", type=str, default=KEY_LOCATION,
            help="Location of the client key")
    parser.add_argument("-kp", action="store", type=str, default=KEY_PASS,
            help="Password for client key")
    return parser


def main():
    """ Run the cli tool. """
    # Create an argument parser and get the argument values.
    parser = get_arg_parser()
    args = parser.parse_args()
    producer = None
    consumer = None

    # Provided duration argument is in seconds -- convert it to milliseconds.
    duration_ms = args.d * 1000

    # Set log config, taking into account verbosity argument.
    fileConfig("logging.conf")
    if args.v:
        logging.getLogger("tc").setLevel(logging.DEBUG)
    if args.vv:
        logging.addLevelName(kafka.LOG_TRACE, "TRACE")
        logging.getLogger("tc").setLevel(kafka.LOG_TRACE)

    logger = logging.getLogger("tc")

    max_records = args.mr
    max_mb = args.mb

    p_schema = Utils.load_schema(args.psf)
    producer = Producer(args.ks, args.pid, args.topic, args.async,
                        duration_ms, args.delay, None, p_schema, args.n, args.sp, args.ca, args.cl,
                        args.kl, args.kp, skip_validate=(not args.ev))
    recordSize = -1

    if max_mb > 0 or max_records > 0:
        edgeGen = RecordGeneratorFactory.get_record_generator(
                producer.serializer)
        logger.info("Num kv pairs: "+str(args.n))
        recordSize = edgeGen.get_average_record_size(args.n)
        msg = "Serialized record size for {n} pairs: {size}".format(
                n=args.n, size=recordSize)
        logger.info(msg)

        if max_mb > 0:
            max_records_by_mb = (max_mb * 1024 * 1024) / recordSize
            msg = "Max Records by MB: {maxbymb}".format(
                  maxbymb=max_records_by_mb)
            logging.info(msg)

            if max_records == -1:
                max_records = max_records_by_mb
            else:
                # We have both maxMB defined and maxRecords, pick the min
                if max_records > max_records_by_mb:
                    max_records = max_records_by_mb

        msg = "Max records: {maxr}".format(maxr=max_records)
        logger.info(msg)

    # Run a single producer if we weren't asked to disable it.
    if not args.np:
        producer.throughput_stats_init(max_mb, max_records,
                                       recordSize, args.sr)
        if args.noavro:
            producer.set_no_avro()

        if args.ev:
            logger.info("Explicit Validate on")

        producer.start()

    # Run a single consumer if we weren't asked to disable it.
    if not args.nc:
        c_schema = Utils.load_schema(args.csf)
        p_schema = Utils.load_schema(args.psf)
        consumer = Consumer(args.ks, args.g, args.topic, duration_ms, args.call, c_schema,
                            p_schema, args.co, args.sp, args.ca, args.cl, args.kl, args.kp)
        consumer.throughput_stats_init(max_mb, max_records, recordSize)
        consumer.start()

    # Wait for the producer and consumer to complete, but periodically check
    # for an interrupt.
    if producer is not None:
        try:
            while producer.isAlive():
                producer.join(1)
        except KeyboardInterrupt:
            producer.shutdown()
    if consumer is not None:
        try:
            while consumer.isAlive():
                consumer.join(1)
        except KeyboardInterrupt:
            consumer.shutdown()


main()
