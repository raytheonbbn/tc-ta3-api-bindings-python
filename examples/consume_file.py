#!/usr/bin/python

# Copyright (c) 2020 Raytheon BBN Technologies Corp.
# See LICENSE.txt for details.

"""
A demo consumer that consumes up to n CDM records from the specified topic and writes the deserialized records to a file in JSON
"""

import logging
import argparse
from logging.config import fileConfig
import sys

from tc.services import kafka
from tc.schema.serialization import Utils
from tc.schema.serialization.kafka import KafkaAvroGenericDeserializer

import confluent_kafka

import uuid

# Default values, replace or use command line arguments
KAFKA_SERVER = "localhost:9094"
PRODUCER_ID = "pyConsumer1"
IS_ASYNC = True
MAX_RECORDS = 100
DELAY = 0
GROUP_ID = "CG_"+str(uuid.uuid4())
FILENAME = "consumed.records.CDM"
SCHEMA = "/opt/starc/avro/TCCDMDatum.avsc"
TOPIC = "TestFileRecords"
SECURITY_PROTOCOL = "SSL"
CA_LOCATION = "/var/private/ssl/ca-cert"
CERT_LOCATION = "/var/private/ssl/kafka.client.pem"
KEY_LOCATION = "/var/private/ssl/kafka.client.key"
KEY_PASS = "TransparentComputing"

def get_arg_parser():
    parser = argparse.ArgumentParser(description="Consume CDM records and write to a file")

    parser.add_argument("-topic", action="store", type=str, default=TOPIC,
                        help="Kafka topic to use for this run.")
    parser.add_argument("-as", dest="async", action="store_true",
            default=IS_ASYNC, help="Enable asychronous producing.")
    parser.add_argument("-pid", action="store", type=str, default=PRODUCER_ID,
                        help="Proudcer ID")
    parser.add_argument("-psf", action="store", type=str,
            default=SCHEMA,
            help="Set the producer schema file.")
    parser.add_argument("-csf", action="store", type=str,
            default=SCHEMA,
            help="Set the consumer schema file.")
    parser.add_argument("-v", action="store_true", default=False,
            help="Turn up verbosity.")
    parser.add_argument("-ks", action="store", type=str, default=KAFKA_SERVER,
            help="The Kafka server's connection string.")
    parser.add_argument("-mr", action="store", type=int, default=MAX_RECORDS,
            help="max Records to publish (default -1, no limit)")
    parser.add_argument("-delay", action="store", type=int, default=DELAY,
            help="Producer publish delay between sends in ms (default 1).")
    parser.add_argument("-f", action="store", type=str, default=FILENAME,
                        help="Filename to write the generated records to")
    parser.add_argument("-g", action="store", type=str, default=GROUP_ID,
                        help="Set the consumer group ID.")
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

    max_records = args.mr
    
    # Load the avro schema
    p_schema = Utils.load_schema(args.psf)
    c_schema = Utils.load_schema(args.csf)

    # Kafka topic to publish to
    topic = args.topic

    # My producer ID 
    group_id = args.g

    # Initialize an avro serializer
    deserializer = KafkaAvroGenericDeserializer(c_schema, p_schema)
    
    config = {}
    config["bootstrap.servers"] = args.ks
    config["group.id"] = group_id
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

    default_topic_config = {}
    default_topic_config["auto.offset.reset"] = "earliest"
    config["default.topic.config"] = default_topic_config

    consumer = confluent_kafka.Consumer(config)
    consumer.subscribe([topic])

    logger.info("Starting Consumer.")

    jsonout = open(args.f+".json", 'w')
    count = 0
    
    while count < args.mr:
        kafka_message = consumer.poll(1)
        
        if kafka_message and not kafka_message.error():
            message = deserializer.deserialize(topic, kafka_message.value())
            count += 1

            if logger.isEnabledFor(logging.DEBUG):
                msg = "Consumed record k: {key}, value: {value}" \
                        .format(key=kafka_message.key(), value=message)
                logger.debug(msg)

            jsonout.write(str(message)+"\n")
        
            if args.delay > 0:
                time.sleep(args.delay)
 

        elif not kafka_message or kafka_message.error().code() == confluent_kafka.KafkaError.REQUEST_TIMED_OUT:
            logger.debug("Comsumer timeout reached.")

        elif kafka_message.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
            logger.debug("End of partition reached.")

        elif kafka_message.error().code() == confluent_kafka.KafkaError.OFFSET_OUT_OF_RANGE:        
            logger.debug("Offset out of range.")

    consumer.close()
    jsonout.close()

    logger.info("Wrote "+str(count)+" records to "+args.f)

main()
