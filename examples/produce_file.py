#!/usr/bin/python

# Copyright (c) 2020 Raytheon BBN Technologies Corp.
# See LICENSE.txt for details.

"""
A demo producer that generates n CDM records, publishes them to the specified topic, and also copies them to a file in JSON
"""

import logging
import argparse
from logging.config import fileConfig

from tc.services import kafka
from tc.schema.serialization import AvroGenericSerializer, Utils
from tc.schema.serialization.kafka import KafkaAvroGenericSerializer
from tc.schema.records.record_generator import RecordGeneratorFactory

import confluent_kafka

# Default values, replace or use command line arguments
KAFKA_SERVER = "localhost:9094"
PRODUCER_ID = "pyProducer1"
IS_ASYNC = True
MAX_RECORDS = 100
DELAY = 0
FILENAME = "random.records.CDM"
SCHEMA = "/opt/starc/avro/TCCDMDatum.avsc"
TOPIC = "TestRandomRecords"
SECURITY_PROTOCOL = "SSL"
CA_LOCATION = "/var/private/ssl/ca-cert"
CERT_LOCATION = "/var/private/ssl/kafka.client.pem"
KEY_LOCATION = "/var/private/ssl/kafka.client.key"
KEY_PASS = "TransparentComputing"

def get_arg_parser():
    parser = argparse.ArgumentParser(description="Produce random CDM records and write to a file")

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
    parser.add_argument("-mr", action="store", type=int, default=MAX_RECORDS,
            help="max Records to publish (default -1, no limit)")
    parser.add_argument("-delay", action="store", type=int, default=DELAY,
            help="Producer publish delay between sends in ms (default 1).")
    parser.add_argument("-f", action="store", type=str, default=FILENAME,
                        help="Filename to write the generated records to")
    parser.add_argument("-n", action="store", type=int, default=0,
                        help="Number of key/value property pairs to append to records.")
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

    # Kafka topic to publish to
    topic = args.topic

    # My producer ID 
    producer_id = args.pid

    # Security protocol
    if args.sp is not None:
        SECURITY_PROTOCOL = args.sp

    # Initialize an avro serializer
    serializer = KafkaAvroGenericSerializer(p_schema)
    
    # Initialize a random record generator based on the given schema
    edgeGen = RecordGeneratorFactory.get_record_generator(serializer)

    # Set up the config for the Kafka producer
    config = {}
    config["bootstrap.servers"] = args.ks
    config["api.version.request"] = True
    config["client.id"] = args.pid

    if SECURITY_PROTOCOL.lower() == "ssl":
        config["security.protocol"] = SECURITY_PROTOCOL
        config["ssl.ca.location"] = CA_LOCATION
        config["ssl.certificate.location"] = CERT_LOCATION
        config["ssl.key.location"] = KEY_LOCATION
        config["ssl.key.password"] = KEY_PASS
    elif SECURITY_PROTOCOL.lower() == "plaintext":
        config["security.protocol"] = SECURITY_PROTOCOL
    else:
        msg = "Unsupported security protocol type for TC APIs: " + SECURITY_PROTOCOL
        raise ValueError(msg)
    
    producer = confluent_kafka.Producer(config)

    logger.info("Starting producer.")

    jsonout = open(args.f+".json", 'w')
    binout = open(args.f+".bin", 'wb')
    
    # Create a file writer and serialize all provided records to it.
    file_serializer = AvroGenericSerializer(p_schema, binout)
    
    for i in range(args.mr):
        edge = edgeGen.generate_random_record(args.n)

        # Provide a key for the record, this will determine which partition the record goes to
        kafka_key = str(i).encode()
        
        # Serialize the record
        message = serializer.serialize(topic, edge)

        if logger.isEnabledFor(logging.DEBUG):
            msg = "Attempting to send record k: {key}, value: {value}" \
                .format(key=kafka_key, value=edge)
            logger.debug(msg)

        producer.produce(topic, value=message, key=kafka_key)
        
        # serialize_to_json
        json = serializer.serialize_to_json(edge)
        jsonout.write(json+"\n")

        # write to binary file
        file_serializer.serialize_to_file(edge)
        
        if args.delay > 0:
            time.sleep(args.delay)

        producer.poll(0)
    
    producer.flush()
    jsonout.close()

    file_serializer.close_file_serializer()

    logger.info("Wrote "+str(args.mr)+" records to "+args.f)

main()
