# Copyright (c) 2020 Raytheon BBN Technologies Corp.
# See LICENSE.txt for details.

"""
Serializer and deserializer for interfacing with Kafka clients.
"""

from tc.schema.serialization import AvroGenericDeserializer
from tc.schema.serialization import AvroGenericSerializer


class KafkaAvroGenericDeserializer(AvroGenericDeserializer):
    """
    Avro deserializer object for interfacing with Kafka.
    """

    def __init__(self, reader_schema_path, writer_schema_path=None, input_file=None):
        """Create a Kafka generic deserializer.

        :param reader_schema_path: Path to the reader schema.
        :param writer_schema_file: Optional parameter to the writer schema
                                   file.  If no value is provided, use the same
                                   file as the reader_schema_path.
        """
        super(KafkaAvroGenericDeserializer, self).__init__(reader_schema_path,
                                                           writer_schema_path, input_file)

    def deserialize(self, topic, data):
        """
        Deserialize Avro graph byte stream from Kafka.
        :param topic: Name of the kafka topic that this data was fetched from.
                      Note that this is not currently used, and it will likely
                      be removed in future versions.
        :param data: Data to deserialize.
        :returns: Record from deserialized data.
        """
        return super(KafkaAvroGenericDeserializer, self).deserialize_bytes(
                data)


class KafkaAvroGenericSerializer(AvroGenericSerializer):
    """
    Avro serializer object for interfacing with Kafka.
    """

    def __init__(self, writer_schema_path, skip_validate=True):
        """Create a Kafka generic serializer.

        :param writer_schema_path: Path to the writer schema.
        """
        super(KafkaAvroGenericSerializer, self).__init__(writer_schema_path, output_file=None, skip_validate=skip_validate)

    def serialize(self, topic, data):
        """
        Serialize graph record with Avro to byte stream for Kafka.
        :param topic: Name of the kafka topic that this data will be pushed to.
                      Note that this is not currently used, and it will likely
                      be removed in future versions.
        :param data: Record to serialize.
        :returns: Serialized bytes from data.
        """
        return super(KafkaAvroGenericSerializer, self).serialize_to_bytes(data)
