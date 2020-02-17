#!/usr/bin/python

# Copyright (c) 2020 Raytheon BBN Technologies Corp.
# See LICENSE.txt for details.

"""
A demo producer that loads serialized records from a file and publishes them
"""

import logging
import argparse
from logging.config import fileConfig
import sys

from tc.services import kafka
from tc.schema.serialization import Utils
from tc.schema.serialization.kafka import KafkaAvroGenericSerializer, KafkaAvroGenericDeserializer
from tc.schema.records.record_generator import RecordGeneratorFactory

import confluent_kafka

# Default values, replace or use command line arguments
KAFKA_SERVER = "localhost:9094"
PRODUCER_ID = "pyProducer2"
IS_ASYNC = True
DELAY = 0
FILENAME = "random.records.CDM.bin"
SCHEMA = "/opt/starc/avro/TCCDMDatum.avsc"
TOPIC = "TestFileRecords"
SECURITY_PROTOCOL = "SSL"
CA_LOCATION = "/var/private/ssl/ca-cert"
CERT_LOCATION = "/var/private/ssl/kafka.client.pem"
KEY_LOCATION = "/var/private/ssl/kafka.client.key"
KEY_PASS = "TransparentComputing"

def get_arg_parser():
    parser = argparse.ArgumentParser(description="Publish records read from a file")

    parser.add_argument("-topic", action="store", type=str, default=TOPIC,
                        help="Kafka topic to use for this run.")
    parser.add_argument("-as", dest="async", action="store_true",
            default=IS_ASYNC, help="Enable asychronous producing.")
    parser.add_argument("-pid", action="store", type=str, default=PRODUCER_ID,
                        help="Proudcer ID")
    parser.add_argument("-psf", action="store", type=str,
            default=SCHEMA,
            help="Set the producer's schema file.")
    parser.add_argument("-v", action="store_true", default=False,
            help="Turn up verbosity.")
    parser.add_argument("-ks", action="store", type=str, default=KAFKA_SERVER,
            help="The Kafka server's connection string.")
    parser.add_argument("-delay", action="store", type=int, default=DELAY,
            help="Producer publish delay between sends in ms (default 1).")
    parser.add_argument("-f", action="store", type=str, default=FILENAME,
                        help="Filename to write the generated records to")
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
    parser = get_arg_parser()
    args = parser.parse_args()

    fileConfig("logging.conf")
    if args.v:
        logging.getLogger("tc").setLevel(logging.DEBUG)
   
    logger = logging.getLogger("tc")
    
    # Load the avro schema
    p_schema = Utils.load_schema(args.psf)

    # Kafka topic to publish to
    topic = args.topic

    # My producer ID 
    producer_id = args.pid

    # Initialize an avro serializer

    rfile = open(args.f, 'rb')
    serializer = KafkaAvroGenericSerializer(p_schema, skip_validate=not args.ev)
    deserializer = KafkaAvroGenericDeserializer(p_schema, input_file=rfile)
    
    # Set up the config for the Kafka producer
    config = {}
    config["bootstrap.servers"] = args.ks
    config["api.version.request"] = True
    config["client.id"] = args.pid

    if args.sp.lower() == "ssl":
        config["security.protocol"] = args.sp
        config["ssl.ca.location"] = args.ca
        config["ssl.certificate.location"] = args.cl
        config["ssl.key.location"] = args.kl
        config["ssl.key.password"] = args.kp
    elif args.sp.lower() == "plaintext":
        config["security.protocol"] = args.sp
    else:
        msg = "Unsupported security protocol: " + args.sp
        logger.error(msg)
        sys.exit(1)

    producer = confluent_kafka.Producer(config)

    logger.info("Starting producer.")

    records = deserializer.deserialize_from_file()

    i = 0
    for edge in records:
        # Provide a key for the record, this will determine which partition the record goes to
        kafka_key = str(i).encode()
        i = i + 1

        # Serialize the record
        message = serializer.serialize(topic, edge)

        if logger.isEnabledFor(logging.DEBUG):
            msg = "Attempting to send record k: {key}, value: {value}" \
                .format(key=kafka_key, value=edge)
            logger.debug(msg)

        producer.produce(topic, value=message, key=kafka_key)
        producer.poll(0)
            
    producer.flush()
    rfile.close()
    logger.info("Wrote "+str(i)+" records to "+str(topic))

main()
