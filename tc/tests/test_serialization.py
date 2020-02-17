# Copyright (c) 2020 Raytheon BBN Technologies Corp.
# See LICENSE.txt for details.

"""
Module containing unit tests for serialization package.
"""

from io import BytesIO
import os
import sys
import unittest

import avro.io

from tc.schema.serialization import Utils
from tc.schema.serialization import AvroGenericDeserializer
from tc.schema.serialization import AvroGenericSerializer


class TestBase(unittest.TestCase):
    """
    Base class for Avro unit tests, providing common setup and helper methods.
    """

    def setUp(self):
        union_schema_file = os.path.dirname(os.path.realpath(__file__)) + \
                "/LabeledGraph.avsc"
        self.reader_schema = Utils.load_schema(union_schema_file)
        self.writer_schema = Utils.load_schema(union_schema_file)
        self.serializer = AvroGenericSerializer(self.writer_schema)
        self.deserializer = AvroGenericDeserializer(self.reader_schema,
                self.writer_schema)

    def compare_nodes(self, node1, node2):
        self.assertTrue(node1["id"] == node2["id"])
        if "role" in node2.keys():
            self.assertTrue(node1["label"] == node2["role"])
        else:
            self.assertTrue(node1["label"] == node2["label"])

        self.assertTrue(len(node1["properties"]) == len(node2["properties"]))

        for property_ in node2["properties"]:
            self.assertTrue(node1["properties"][property_] ==
                    node2["properties"][property_])

    def compare_edges(self, edge1, edge2):
        if "role" in edge2.keys():
            self.assertTrue(edge1["label"] == edge2["role"])
        else:
            self.assertTrue(edge1["label"] == edge2["label"])

        self.compare_nodes(edge1["fromNode"], edge2["fromNode"])
        self.compare_nodes(edge1["toNode"], edge2["toNode"])


class TestBadLabel(TestBase):
    """
    Test for whether or not a label which is not part of the schema will be
    accepted by the Avro code.
    """

    def test_bad_node_label(self):
        schema = self.writer_schema
        bad_label = "badNodeLabel"
        bad_node = Utils.create_node(1, bad_label, schema)
        is_valid = Utils.validate(schema, bad_node)
        self.assertFalse(is_valid)

    def test_bad_edge_label(self):
        schema = self.writer_schema
        node1 = Utils.create_node(1, "unitOfExecution", schema)
        node2 = Utils.create_node(2, "agent", schema)
        bad_label = "badEdgeLabel"
        bad_edge = Utils.create_edge(node1, node2, bad_label, schema)
        is_valid = Utils.validate(schema, bad_edge)
        self.assertFalse(is_valid)


class TestByteArrayOutputStream(TestBase):
    """
    Test of whther or not a Python byte stream is reusable.
    """

    # NOTE: This test shows that the python BytesIO object can be
    # reused, but I am not sure if this is actually more efficient
    # than just creating a new one.  We should test if it is before
    # we start using it.
    def test_reusable(self):
        byte_stream = BytesIO()

        # Write one message to the stream
        byte_stream.write(b"hello")
        byte_stream.seek(0)
        result = byte_stream.read()
        self.assertTrue(len(result) == 5)

        # Reset the stream
        byte_stream.seek(0)
        byte_stream.truncate(0)

        # Write a different message to the steram
        byte_stream.write(b"hi")
        byte_stream.seek(0)
        result = byte_stream.read()
        self.assertTrue(len(result) == 2)


class TestByteSerialization(TestBase):
    """
    Test of serializing to and deserializing from a byte stream.
    """

    def test_serialization_union(self):
        self.serialization_test_helper(self.writer_schema, True)

    def test_serialization_nested(self):
        schema_file = os.path.dirname(os.path.realpath(__file__)) + \
                "/LabeledEdge.avsc"
        schema = Utils.load_schema(schema_file)

        self.serializer = AvroGenericSerializer(schema)
        self.deserializer = AvroGenericDeserializer(schema, schema)
        self.serialization_test_helper(schema, False)

    def serialization_test_helper(self, schema, is_union):
        node1 = Utils.create_node(1, "unitOfExecution", schema, True)
        node2 = Utils.create_node(2, "artifact", schema, True)
        edge1 = Utils.create_edge(node1, node2, "read", schema)

        if is_union:
            serialized_node1 = self.serializer.serialize_to_bytes(node1)
            deserialized_node1 = \
                    self.deserializer.deserialize_bytes(serialized_node1)
            self.compare_nodes(node1, deserialized_node1)

            serialized_node2 = self.serializer.serialize_to_bytes(node2)
            deserialized_node2 = \
                    self.deserializer.deserialize_bytes(serialized_node2)
            self.compare_nodes(node2, deserialized_node2)

        serialized_edge1 = self.serializer.serialize_to_bytes(edge1)
        deserialized_edge1 = \
                self.deserializer.deserialize_bytes(serialized_edge1)
        self.compare_edges(edge1, deserialized_edge1)


class TestNewSchemaByteSerialization(TestByteSerialization):
    """
    Test of serializing to and deserializing from a byte stream using an
    alternate schema.
    """

    def test_serialization_union(self):
        schema_file = os.path.dirname(os.path.realpath(__file__)) + \
                "/LabeledGraphv2.avsc"
        self.reader_schema = Utils.load_schema(schema_file)

        self.deserializer = AvroGenericDeserializer(self.reader_schema,
                self.writer_schema)
        self.serialization_test_helper(self.writer_schema, True)

    def test_serialization_nested(self):
        schema_file = os.path.dirname(os.path.realpath(__file__)) + \
                "/LabeledEdge.avsc"
        self.writer_schema = Utils.load_schema(schema_file)
        schema_file = os.path.dirname(os.path.realpath(__file__)) + \
                "/LabeledEdgev2.avsc"
        self.reader_schema = Utils.load_schema(schema_file)

        self.serializer = AvroGenericSerializer(self.writer_schema)
        self.deserializer = AvroGenericDeserializer(self.reader_schema,
                self.writer_schema)
        self.serialization_test_helper(self.writer_schema, False)

    def serialization_test_helper(self, schema, is_union):
        super(TestNewSchemaByteSerialization, self).serialization_test_helper(
                schema, is_union)


class TestFileSerialization(TestBase):
    """
    Test of serializing to and deserializing from a file.
    """

    def test_serialization_union(self):
        self.serialization_test_helper(self.writer_schema, True)

    def test_serialization_nested(self):
        schema_file = os.path.dirname(os.path.realpath(__file__)) + \
                "/LabeledEdge.avsc"
        schema = Utils.load_schema(schema_file)

        self.serializer = AvroGenericSerializer(schema)
        self.deserializer = AvroGenericDeserializer(schema, schema)
        self.serialization_test_helper(schema, False)

    def serialization_test_helper(self, schema, is_union):
        node_file_path = os.path.dirname(os.path.realpath(__file__)) + \
                "/testNodes.avro"
        edge_file_path = os.path.dirname(os.path.realpath(__file__)) + \
                "/testEdges.avro"

        # Create some nodes and an edge.
        node1 = Utils.create_node(1, "unitOfExecution", schema, True)
        node2 = Utils.create_node(2, "artifact", schema, True)
        edge1 = Utils.create_edge(node1, node2, "read", schema)

        if is_union:
            # Serialize the nodes and edge to files.
            with open(node_file_path, "wb") as node_file:
                self.serializer = AvroGenericSerializer(self.writer_schema, node_file)
                self.serializer.serialize_to_file([node1, node2])
                self.serializer.close_file_serializer()

        with open(edge_file_path, "wb") as edge_file:
            self.serializer = AvroGenericSerializer(self.writer_schema, edge_file)
            self.serializer.serialize_to_file([edge1])
            self.serializer.close_file_serializer()

        if is_union:
            # Deserialize from the files to records.
            with open(node_file_path, "rb") as node_file:
                self.deserializer = AvroGenericDeserializer(self.reader_schema, self.writer_schema, node_file)
                deserialized_nodes = \
                        self.deserializer.deserialize_from_file()
                self.deserializer.close_file_deserializer()

        with open(edge_file_path, "rb") as edge_file:
            self.deserializer = AvroGenericDeserializer(self.reader_schema, self.writer_schema, edge_file)
            deserialized_edges = \
                    self.deserializer.deserialize_from_file()
            self.deserializer.close_file_deserializer()

        if is_union:
            # Check the deserialized nodes.
            self.assertTrue(len(deserialized_nodes) == 2)
            self.compare_nodes(node1, deserialized_nodes[0])
            self.compare_nodes(node2, deserialized_nodes[1])

        # Check the deserialized edges.
        self.assertTrue(len(deserialized_edges) == 1)
        self.compare_edges(edge1, deserialized_edges[0])

        if is_union:
            # Clean up the files
            os.remove(node_file_path)

        os.remove(edge_file_path)


class TestNewSchemaFileSerialization(TestFileSerialization):
    """
    Test of serializing to and deserializing from a file using an
    alternate schema.
    """

    def test_serialization_union(self):
        schema_file = os.path.dirname(os.path.realpath(__file__)) + \
                "/LabeledGraphv2.avsc"
        self.reader_schema = Utils.load_schema(schema_file)
        self.deserializer = AvroGenericDeserializer(self.reader_schema,
                self.writer_schema)
        self.serialization_test_helper(self.writer_schema, True)

    def test_serialization_nested(self):
        schema_file = os.path.dirname(os.path.realpath(__file__)) + \
                "/LabeledEdge.avsc"
        self.writer_schema = Utils.load_schema(schema_file)
        schema_file = os.path.dirname(os.path.realpath(__file__)) + \
                "/LabeledEdgev2.avsc"
        self.reader_schema = Utils.load_schema(schema_file)

        self.serializer = AvroGenericSerializer(self.writer_schema)
        self.deserializer = AvroGenericDeserializer(self.reader_schema,
                self.writer_schema)
        self.serialization_test_helper(self.writer_schema, False)

    def serialization_test_helper(self, schema, is_union):
        super(TestNewSchemaFileSerialization, self).serialization_test_helper(
                schema, is_union)


class TestJsonSerialization(TestBase):
    """
    Test of serializing and deserializing records to and from json.
    """

    def setUp(self):
        schema_file = os.path.dirname(os.path.realpath(__file__)) + \
                "/LabeledEdge.avsc"
        self.schema = Utils.load_schema(schema_file)
        self.serializer = AvroGenericSerializer(self.schema)
        self.deserializer = AvroGenericDeserializer(self.schema, self.schema)

    def test_serialization(self):
        node1 = Utils.create_node(1, "unitOfExecution", True, self.schema)
        node2 = Utils.create_node(2, "artifact", True, self.schema)
        edge = Utils.create_edge(node1, node2, "read", True, self.schema)

        # Make sure serialization and deserialization is symmetric.
        json_edge = self.serializer.serialize_to_json(edge, True)
        deserialized_edge = self.deserializer.deserialize_json(json_edge)
        self.assertTrue(edge == deserialized_edge)

        # Make sure that the serializer can serialize to both bytes and json
        # without corrupting any internal state.  Also, test without making
        # the json serialization prettified.
        edge = Utils.create_edge(node1, node2, "modified", True, self.schema)
        self.serializer.serialize_to_bytes(edge)
        json_edge = self.serializer.serialize_to_json(edge)
        deserialized_edge = self.deserializer.deserialize_json(json_edge)
        self.assertTrue(edge == deserialized_edge)

    def test_bad_record_serialization(self):
        bad_edge = {"test": "bad"}
        with self.assertRaises(avro.io.AvroTypeException):
            self.serializer.serialize_to_json(bad_edge)

    def test_bad_deserialized_record(self):
        bad_serialized_edge = '{"test": "bad"}'
        with self.assertRaises(avro.io.AvroTypeException):
            self.deserializer.deserialize_json(bad_serialized_edge)


class TestOptionalField(TestBase):
    """
    Test of serializing and deserializing records with an optional field either
    present or absent.
    """

    def test_optional_field_absent(self):
        schema = self.writer_schema
        node1 = Utils.create_node(1, "unitOfExecution", schema, True)
        node2 = Utils.create_node(2, "agent", schema, True)
        edge1 = Utils.create_edge(node1, node2, "wasAssociatedWith", schema)
        serialized_edge = self.serializer.serialize_to_bytes(edge1)
        deserialized_edge = \
                self.deserializer.deserialize_bytes(serialized_edge)
        self.assertTrue(deserialized_edge["properties"] is None)

    def test_optional_field_present(self):
        schema = self.writer_schema
        node1 = Utils.create_node(1, "unitOfExecution", schema, True)
        node2 = Utils.create_node(2, "agent", schema, True)
        edge1 = Utils.create_edge(node1, node2, "wasAssociatedWith",
                schema, True)
        serialized_edge = self.serializer.serialize_to_bytes(edge1)
        deserialized_edge = \
                self.deserializer.deserialize_bytes(serialized_edge)
        self.assertTrue(deserialized_edge["properties"] is not None)


class TestPrettyPrint(TestBase):
    """
    Test that pretty printing works as expected.
    """

    def test_pretty_node_short(self):
        schema = self.writer_schema
        artifact_node = Utils.create_node(1, "Artifact", schema)
        artifact_node["properties"] = {}
        artifact_node["properties"]["path"] = "/dev/null"
        self.assertTrue("..." not in Utils.pretty_node(artifact_node))

    def test_pretty_node_long(self):
        schema = self.writer_schema
        artifact_node = Utils.create_node(1, "Artifact", schema)
        artifact_node["properties"] = {}
        artifact_node["properties"]["path"] = \
                "/tmp/this/is/a/long/path/and/it/should/get/broken/up.txt"
        self.assertTrue("..." in Utils.pretty_node(artifact_node))


class TestRecordBuilder(TestBase):
    """ Unimplemented test -- Python Avro library has no record builder. """
    pass


class TestNestedSchema(unittest.TestCase):
    """
    Test a complex schema with nested records.
    """

    _NODE_SCHEMA_FULLNAME = "com.bbn.tc.schema.avro.LabeledNode"
    _EDGE_SCHEMA_FULLNAME = "com.bbn.tc.schema.avro.LabeledEdge"

    def setUp(self):
        union_schema_file = os.path.dirname(os.path.realpath(__file__)) + \
                "/LabeledGraph.avsc"
        self.union_schema = Utils.load_schema(union_schema_file)

        nested_schema_file = os.path.dirname(os.path.realpath(__file__)) + \
                "/LabeledEdge.avsc"
        self.nested_schema = Utils.load_schema(nested_schema_file)

    def test_nested_schema_fields(self):
        if sys.version_info >= (3, 0):
            self._py3_test_nested_schema_fields()
        else:
            self._py2_test_nested_schema_fields()

    def _py3_test_nested_schema_fields(self):
        self.assertTrue(
                self.nested_schema.field_map["fromNode"].type.name ==
                "LabeledNode")
        self.assertTrue(self.nested_schema.field_map["fromNode"].type ==
                self.nested_schema.field_map["toNode"].type)

    def _py2_test_nested_schema_fields(self):
        self.assertTrue(
                self.nested_schema.fields_dict["fromNode"].type.name ==
                "LabeledNode")
        self.assertTrue(self.nested_schema.fields_dict["fromNode"].type ==
                self.nested_schema.fields_dict["toNode"].type)


class TestUnionSchema(unittest.TestCase):
    """
    Test that serializing and deserializing when using a union schema
    works as expected.
    """

    _NODE_SCHEMA_FULLNAME = "com.bbn.tc.schema.avro.LabeledNode"
    _EDGE_SCHEMA_FULLNAME = "com.bbn.tc.schema.avro.LabeledEdge"

    def setUp(self):
        schema_file = os.path.dirname(os.path.realpath(__file__)) + \
                "/LabeledGraph.avsc"
        schema = Utils.load_schema(schema_file)

        self.reader_schema = schema
        self.writer_schema = schema
        self.node_schema = Utils.get_schema_by_name(
                self.writer_schema, TestUnionSchema._NODE_SCHEMA_FULLNAME)
        self.edge_schema = Utils.get_schema_by_name(
                self.writer_schema, TestUnionSchema._EDGE_SCHEMA_FULLNAME)

        self.serializer = AvroGenericSerializer(self.writer_schema)
        self.deserializer = AvroGenericDeserializer(
                self.reader_schema, self.writer_schema)

    def test_union_schema(self):
        schema = self.writer_schema
        node1 = Utils.create_node(1, "unitOfExecution", schema, True)
        node2 = Utils.create_node(2, "agent", schema, True)
        edge1 = Utils.create_edge(
                node1, node2, "wasAssociatedWith", schema, True)

        serialized_node = self.serializer.serialize_to_bytes(node1)
        serialized_edge = self.serializer.serialize_to_bytes(edge1)

        deserialized_node = self.deserializer.deserialize_bytes(
                serialized_node)
        deserialized_edge = self.deserializer.deserialize_bytes(
                serialized_edge)

        # Don't convert these to strings, like in the Java code.  That results
        # in differences due to unicode strings for keys in the
        # Avro-deserialized in the Python 2.7 version, and we don't really
        # want to deal with that.
        self.assertTrue(node1 == deserialized_node)
        self.assertTrue(edge1 == deserialized_edge)


if __name__ == "__main__":
    unittest.main()
