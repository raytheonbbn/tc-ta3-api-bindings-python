# Copyright (c) 2020 Raytheon BBN Technologies Corp.
# See LICENSE.txt for details.

"""
Module containing unit tests for record package's CDM-specific code.
"""

import os
import sys
import unittest

from tc.schema.serialization import Utils, AvroGenericSerializer, AvroGenericDeserializer
import tc.schema.records.record_generator as record_generator
import tc.schema.records.parsing as parsing


class TestCDMTypeParsing(unittest.TestCase):

    _KV_PAIRS = 5

    def setUp(self):
        schema_file = os.path.dirname(os.path.realpath(__file__)) + \
                "/TCCDMDatum.avsc"
        self.reader_schema = Utils.load_schema(schema_file)
        self.writer_schema = Utils.load_schema(schema_file)
        self.serializer = AvroGenericSerializer(self.writer_schema)
        self.deserializer = AvroGenericDeserializer(self.reader_schema)

    def _run_record_type_test(self, generator, expected_value):
        parser = parsing.CDMParser(self.reader_schema)
        for i in range(20):
            record = generator.generate_random_record(TestCDMTypeParsing._KV_PAIRS)
            self.assertTrue(Utils.validate(self.writer_schema, record))
            self.assertTrue(parser.get_record_type(record) == expected_value)
            serialized = self.serializer.serialize_to_bytes(record)
            deserialized = self.deserializer.deserialize_bytes(serialized)
            self.assertTrue(parser.get_record_type(deserialized) == expected_value)

    def test_provenance_tag_node(self):
        generator = \
                record_generator.CDMProvenanceTagNodeGenerator(self.serializer)
        self._run_record_type_test(generator, "ProvenanceTagNode")

    def test_event(self):
        generator = record_generator.CDMEventGenerator(self.serializer)
        self._run_record_type_test(generator, "Event")

    def test_net_flow_object(self):
        generator = record_generator.CDMNetFlowObjectGenerator(self.serializer)
        self._run_record_type_test(generator, "NetFlowObject")

    def test_file_object(self):
        generator = record_generator.CDMFileObjectGenerator(self.serializer)
        self._run_record_type_test(generator, "FileObject")

    def test_src_sink_object(self):
        generator = record_generator.CDMSrcSinkObjectGenerator(self.serializer)
        self._run_record_type_test(generator, "SrcSinkObject")

    def test_ipc_object(self):
        generator = record_generator.CDMIpcObjectGenerator(self.serializer)
        self._run_record_type_test(generator, "IpcObject")

    def test_memory_object(self):
        generator = record_generator.CDMMemoryObjectGenerator(self.serializer)
        self._run_record_type_test(generator, "MemoryObject")

    def test_principal(self):
        generator = record_generator.CDMPrincipalGenerator(self.serializer)
        self._run_record_type_test(generator, "Principal")

    def test_time_marker(self):
        generator = record_generator.CDMTimeMarkerGenerator(self.serializer)
        self._run_record_type_test(generator, "TimeMarker")

    def test_unit_dependency_marker(self):
        generator = record_generator.CDMUnitDependencyGenerator(self.serializer)
        self._run_record_type_test(generator, "UnitDependency")

    def test_registry_key_object(self):
        generator = record_generator.CDMRegistryKeyObjectGenerator(self.serializer)
        self._run_record_type_test(generator, "RegistryKeyObject")

    def test_host(self):
        generator = record_generator.CDMHostGenerator(self.serializer)
        self._run_record_type_test(generator, "Host")

    def test_packet_socket_object(self):
        generator = record_generator.CDMPacketSocketObjectGenerator(self.serializer)
        self._run_record_type_test(generator, "PacketSocketObject")

    def test_end_marker(self):
        generator = record_generator.CDMEndMarkerGenerator(self.serializer)
        self._run_record_type_test(generator, "EndMarker")

    def test_unknown_provenance_node(self):
        generator = record_generator.CDMUnknownProvenanceNodeGenerator(self.serializer)
        self._run_record_type_test(generator, "UnknownProvenanceNode")

class TestCDMUnionParsing(unittest.TestCase):

    def setUp(self):
        schema_file = os.path.dirname(os.path.realpath(__file__)) + \
                "/TCCDMDatum.avsc"
        self.reader_schema = Utils.load_schema(schema_file)
        self.writer_schema = Utils.load_schema(schema_file)
        self.serializer = AvroGenericSerializer(self.writer_schema)

    def test_record_type(self):
        generator = record_generator.CDMEventGenerator(self.serializer)
        record = generator.generate_random_record(5)
        parser = parsing.CDMParser(self.reader_schema)
        self.assertTrue(Utils.validate(self.writer_schema, record))
        self.assertTrue(parser.get_union_branch_type(record) == "Event")

    def test_all_record_types(self):
        # We generate a large number of random records and verify that
        # attempting to parse those records doesn't result in an error.  This
        # is a safety catch to ensure that our parser has support for all top
        # level record types.
        NUM_RECORDS = 100
        generator = record_generator.CDMRecordGenerator(self.serializer)
        try:
            for _ in range(NUM_RECORDS):
                record = generator.generate_random_record(5)
                parser = parsing.CDMParser(self.reader_schema)
        except ValueError:
            self.fail("A record was generated with a type which was not understood by the parser!")

    def test_ambiguous_cdm18_parsing(self):
        deserializer = AvroGenericDeserializer(self.reader_schema, self.reader_schema)
        fog = record_generator.CDMFileObjectGenerator(self.serializer)
        ssog = record_generator.CDMSrcSinkObjectGenerator(self.serializer)
        rkog = record_generator.CDMRegistryKeyObjectGenerator(self.serializer)
        parser = parsing.CDMParser(self.reader_schema)

        file_object_record = fog.generate_random_record(5)
        # Remove all of the unique optional attributes
        file_object_record["datum"].pop("localPrincipal", None)
        file_object_record["datum"].pop("size", None)
        file_object_record["datum"].pop("peInfo", None)
        file_object_record["datum"].pop("hashes", None)

        src_sink_object_record = ssog.generate_random_record(5)
        registry_key_object_record = rkog.generate_random_record(5)

        sfo = self.serializer.serialize_to_bytes(file_object_record)
        ssso = self.serializer.serialize_to_bytes(src_sink_object_record)
        srko = self.serializer.serialize_to_bytes(registry_key_object_record)

        file_object_record2 = deserializer.deserialize_bytes(sfo)
        src_sink_object_record2 = deserializer.deserialize_bytes(ssso)
        registry_key_object_record2 = deserializer.deserialize_bytes(srko)

        self.assertTrue(parser.get_union_branch_type(file_object_record2) == "FileObject")
        self.assertTrue(parser.get_union_branch_type(src_sink_object_record2) == "SrcSinkObject")
        self.assertTrue(parser.get_union_branch_type(registry_key_object_record2) == "RegistryKeyObject")


class TestCDMUtil(unittest.TestCase):

    def _py3_mod(self, value):
        return value

    def _py2_mod(self, value):
        return chr(value)

    def test_uuid_valid(self):
        value = 0xdeadbea7dad5
        uuid = record_generator.Util.get_uuid_from_value(value)
        wrapper = self._py3_mod if sys.version_info >= (3, 0) else self._py2_mod

        self.assertTrue(uuid[15] == wrapper(0xd5))
        self.assertTrue(uuid[14] == wrapper(0xda))
        self.assertTrue(uuid[13] == wrapper(0xa7))
        self.assertTrue(uuid[12] == wrapper(0xbe))
        self.assertTrue(uuid[11] == wrapper(0xad))
        self.assertTrue(uuid[10] == wrapper(0xde))
        for i in range(10):
            self.assertTrue(uuid[i] == wrapper(0x00))

    def test_uuid_too_long(self):
        value = 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        with self.assertRaises(ValueError):
            uuid = record_generator.Util.get_uuid_from_value(value)

    def test_short_valid(self):
        value = 0x1234
        short = record_generator.Util.get_short_from_value(value)
        wrapper = self._py3_mod if sys.version_info >= (3, 0) else self._py2_mod

        self.assertTrue(short[1] == wrapper(0x34))
        self.assertTrue(short[0] == wrapper(0x12))

    def test_short_too_long(self):
        value = 0x10000
        with self.assertRaises(ValueError):
            short = record_generator.Util.get_short_from_value(value)

    def test_byte_valid(self):
        value = 0x12
        byte = record_generator.Util.get_byte_from_value(value)
        wrapper = self._py3_mod if sys.version_info >= (3, 0) else self._py2_mod

        self.assertTrue(byte[0] == wrapper(0x12))

    def test_byte_too_long(self):
        value = 0x10000
        with self.assertRaises(ValueError):
            byte = record_generator.Util.get_byte_from_value(value)

if __name__ == "__main__":
    unittest.main()
