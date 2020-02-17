# Copyright (c) 2020 Raytheon BBN Technologies Corp.
# See LICENSE.txt for details.

"""
Module providing interfaces for working with serialization and deserialization
within the TC TA3 architecture.
"""

import codecs
from io import BytesIO
import functools
import json
from json import JSONDecoder
import six
import threading
import time

import avro.io
from avro.io import AvroTypeException, SchemaResolutionException
if six.PY3:
    from avro.io import Validate as validate
else:
    from avro.io import validate
import avro.schema

import quickavro

import avro_json_serializer


class AvroGenericDeserializer(object):
    """
    Class for performing deserialization from byte streams with Avro.
    """

    def __init__(self, reader_schema, writer_schema=None, input_file=None):
        """Create a generic deserializer.

        :param reader_schema_path: Path to the reader schema.
        :param writer_schema_file: Optional parameter to the writer schema
                                   file.  If no value is provided, use the same
                                   file as the reader_schema_path.
        """

        if writer_schema is None:
            writer_schema = reader_schema
        self._reader_schema = reader_schema
        self._writer_schema = writer_schema
        self._reader_schema_json = json.loads(str(self._reader_schema))
        self._writer_schema_json = json.loads(str(self._writer_schema))
        self._input_file = input_file
        self._set_avro_readers()

    @property
    def reader_schema(self):
        return self._reader_schema

    @reader_schema.setter
    def reader_schema(self, schema):
        self._reader_schema = schema
        self._reader_schema_json = json.loads(str(self._reader_schema))
        self._same_schema = (self._reader_schema == self._writer_schema)
        self._set_avro_readers()

    @property
    def writer_schema(self):
        return self._writer_schema

    @writer_schema.setter
    def writer_schema(self, schema):
        self._writer_schema = schema
        self._writer_schema_json = json.loads(str(self._writer_schema))
        self._same_schema = (self._reader_schema == self._writer_schema)
        self._set_avro_readers()

    def _set_avro_readers(self):
        self._json_deserializer = TcJsonDeserializer(self._reader_schema)
        self._quickavro_decoder = quickavro.BinaryEncoder()
        self._quickavro_decoder.schema = self._reader_schema_json
        self._quickavro_writer_decoder = quickavro.BinaryEncoder()
        self._quickavro_writer_decoder.schema = self._writer_schema_json

    @property
    def same_schema(self):
        """Return whether or not the reader and writer schema are the same."""
        return self._same_schema

    def deserialize_bytes(self, serialized_bytes):
        """Deserialize the provided bytes into a record.

        :param serialized_bytes: Byte stream of a serialized record.
        :returns: Deserialized record.
        """
        # Do a poor-man's evolvable schema implementation.  First try to
        # deserialize with the reader schema, and if that fails do it with
        # the writer schema.  If deserialization fails with the writer schema,
        # let the exception propogate back to the caller.
        try:
            result = self._deserialize_bytes(serialized_bytes, self._quickavro_decoder)
            return result
        except SchemaResolutionException as e:
            result = self._deserialize_bytes(serialized_bytes, self._quickavro_writer_decoder)
            return result

    def _deserialize_bytes(self, serialized_bytes, decoder):
        # Quickavro returns a list of records.  We typically should only see
        # a single record.
        result = decoder.read(serialized_bytes)
        if result:
            if len(result) == 1:
                return result[0]
            else:
                if len(result) > 1:
                    raise TypeError("Deserialized bytes returned more than one record.")
                else:
                    raise TypeError("Deserialized bytes did not return any records.")
        # If None was returned, quickavro is signalling that the serialized
        # record did not match the schema.
        else:
            raise SchemaResolutionException("Failed to deserialize", self._writer_schema, self._reader_schema)

    def deserialize_next_from_file(self):
        """Deserialize next record from a file.

        :returns: The next deserialized record.
        """
        # We cannot read from a file unless the user provides it in the
        # constructor.
        if not self._input_file:
            raise Exception("No input file provided to deserialize from.")

        with quickavro.FileReader(self._input_file) as reader:
            for record in reader.records():
                yield record

    def deserialize_from_file(self):
        """Deserialize file contents into records.

        :returns: List of deserialized records.
        """

        # We cannot read from a file unless the user provides it in the
        # constructor.
        if not self._input_file:
            raise Exception("No input file provided to deserialize from.")

        # Build a record list from the file contents.
        records = []
        for record in self.deserialize_next_from_file():
            records.append(record)

        self.close_file_deserializer()

        return records

    def deserialize_json(self, json_string):
        """Deserialize a json string into a record.

        :param json_string: Json string of a serialized record.
        :returns: Deserialized record.
        :raises SchemaResolutionExcpetion: If reader and writer schema are not
                                           compatible, and exception is raised.
        :raises AvroTypeException: If deserialized record is not valid
                                   according to the reader schema, an exception
                                   is raised.
        """
        return self._json_deserializer.from_json(json_string)

    def close_file_deserializer(self):
        """Clean up resources after deserializing from file."""
        if self._input_file:
            self._input_file.close()
            self._input_file = None


class AvroGenericSerializer(object):
    """
    Class for performing serialization to byte streams with Avro.
    """

    def __init__(self, writer_schema, output_file=None, skip_validate=True):
        """Create a Kafka generic serializer.

        :param writer_schema_path: Path to the writer schema.
        :param output_file: Path to a file when calling serialize_to_file.
        """
        self._writer_schema = writer_schema
        self._writer_schema_json = json.loads(str(self._writer_schema))
        self._output_file = output_file
        self._set_avro_writers()

    @property
    def writer_schema(self):
        return self._writer_schema

    @writer_schema.setter
    def writer_schema(self, schema):
        self._writer_schema = schema
        self._set_avro_writers()

    def _set_avro_writers(self):
        self._json_serializer = TcJsonSerializer(self._writer_schema)
        self._quickavro_encoder = quickavro.BinaryEncoder()
        self._quickavro_encoder.schema = self._writer_schema_json
        if self._output_file:
            self.file_writer = quickavro.FileWriter(self._output_file)
            self.file_writer.schema = self._writer_schema_json

    def serialize_to_bytes(self, record):
        """Serialize the provided record into a byte stream.

        :param record: Record which should be serialized to a byte stream.
        :returns: Serialized record.
        """
        result = self._quickavro_encoder.write(record)
        return result

    def serialize_to_file(self, records):
        """Serialize the provided records into a file.

        :param records: List of records to serialize to the file.
        """
        # We cannot write to a file unless the user provides it in the
        # constructor.
        if not self._output_file:
            raise Exception("No output file provided to serialize to.")

        # If this is a single record, wrap it with a list.
        if isinstance(records, dict):
            records = [records]

        # Verify that the provided records are valid.
        if records is not None and len(records) > 0:
            for record in records:
                self.file_writer.write_record(record)

        else:
            # Cannot serialize the provided information.  To adhere to the
            # Java API, we should throw an exception.
            raise ValueError("No records provided to serialize to a file.")

    def serialize_to_json(self, record, pretty=False):
        """Serialize the provided record to a json string.

        :param record: Record which should be serialized to a json string.
        :param pretty: Whether or not to prettify the json string.
        :returns: Serialized record.
        :raises AvroTypeException: If provided record is not valid according to
                                   the writer schema, an exception is raised.
        """
        return self._json_serializer.to_json(record)

    def close_file_serializer(self):
        """Clean up resources after serializing to file."""
        if self.file_writer:
            self.file_writer.close()
            self._output_file = None


class Utils(object):
    """
    Utility functions for working with Avro records of nodes and edges as
    defined by the TC schema.
    """

    # Seprators used in pretty-print strings.
    _SEPARATOR = ":"
    _SUB_SEPARATOR = ","

    # Maximum provided path length that we will use in a pretty-print string.
    _MAX_PATH_LEN = 30

    # Default name of a file to use when one is not provided.
    _DEFAULT_FILENAME = "filename"

    # Avro-specific constants
    _AVRO_UNION_TYPE = "union"
    _AVRO_RECORD_TYPE = "record"
    _AVRO_ARRAY_TYPE = "array"
    _AVRO_MAP_TYPE = "map"

    @staticmethod
    def _get_time_milliseconds():
        # Get the milliseconds since the epoch timestamp.  We want a string
        # represetation of an integer.
        return str(int(time.time() * 1000))

    @staticmethod
    def create_node(id_, label, node_schema, add_ts=False):
        """Return a node record which can be serialized by Avro.

        :param id_: Long integer unique ID for this node.
        :param label: Label for this node defining its role.
        :param node_schema: Avro schema for a node to validate against.
        :param add_ts: Boolean noting whether or not this record should include
                       a timestamp in its properties.
        :returns: Record representing a node defined by provided parameters.
        """
        node_record = {}
        node_record["id"] = id_
        node_record["label"] = label

        # Add a timestamp to the properties if the caller requested it.
        if add_ts:
            node_record["properties"] = {}
            node_record["properties"]["timestamp"] = \
                    Utils._get_time_milliseconds()

        return node_record

    @staticmethod
    def create_edge(from_node, to_node, label, edge_schema, add_ts=False):
        """Return an edge record which can be serialized by Avro.

        :param from_node: Node record representing the source vertex.
        :param to_node: Node record representing the destination vertex.
        :param label: Label for this node defining its role.
        :param edge_schema: Avro schema for an edge to validate against.
        :param add_ts: Boolean noting whether or not this record should include
                       a timestamp in its properties.
        :returns: Record representing an edge defined by provided parameters.
        """
        edge_record = {}
        edge_record["label"] = label
        edge_record["fromNode"] = from_node
        edge_record["toNode"] = to_node

        # Add a timestamp to the properties if the caller requested it.
        if add_ts:
            edge_record["properties"] = {}
            edge_record["properties"]["timestamp"] = \
                    Utils._get_time_milliseconds()

        return edge_record

    @staticmethod
    def pretty_node(node_record):
        """Create a string of a node record for pretty-printing.

        :param node_record: Node record to convert to a string.
        :returns: String represenation of node record for pretty-printing.
        """
        node_str = ""
        label = node_record.get("label", None)
        properties = node_record.get("properties", None)

        if label is None or properties is None:
            return str(node_record)
        else:
            # Add the label
            node_str += label + Utils._SEPARATOR

            # If this is an artifact, add the path
            if label == "Artifact":
                path = properties.get("path", Utils._DEFAULT_FILENAME)

                if path is not None:
                    # Shorten excessively long paths
                    if len(path) >= Utils._MAX_PATH_LEN:
                        substr_len = Utils._MAX_PATH_LEN // 2
                        subpath = path[0:substr_len] + "..."
                        subpath += path[len(path)-substr_len:]
                        path = subpath
                    node_str += path

            # If this is a process, add information about it
            elif label == "Process":
                pid = properties.get("pid", None)
                uid = properties.get("uid", None)
                user = properties.get("user", None)
                proc_name = properties.get("pidname", None)
                node_str += "[uid" + Utils._SEPARATOR + str(uid)
                node_str += Utils._SUB_SEPARATOR
                node_str += "pid" + Utils._SEPARATOR + str(pid)
                if user is not None:
                    node_str += Utils._SUB_SEPARATOR + "user"
                    node_str += Utils._SEPARATOR + user
                if proc_name is not None:
                    node_str += Utils._SUB_SEPARATOR + "pidname"
                    node_str += Utils._SEPARATOR + proc_name
                node_str += "]"

            return node_str

    @staticmethod
    def pretty_edge(edge_record):
        """Create a string of an edge record for pretty-printing.

        :param edge_record: Edge record to convert to a string.
        :returns: String represenation of edge record for pretty-printing.
        """
        # String of the form (from_node) ===label===> (to_node)
        from_node = edge_record.get("fromNode", {})
        to_node = edge_record.get("toNode", {})
        label = edge_record.get("label", None)

        edge_str = "(" + Utils.pretty_node(from_node) + ")"
        edge_str += " ===" + label + "===> "
        edge_str += "(" + Utils.pretty_node(to_node) + ")"

        return edge_str

    @staticmethod
    def load_schema(schema_path):
        """Load an Avro schema based on the provided file path.

        :param schema_path: Path to the schema file.
        :returns: An Avro Schema object representing the provided schema.
        """
        with open(schema_path) as schema_file:
            return Utils.parse(schema_file.read())

    @staticmethod
    def parse(schema):
        """Parse a string schema into an Avro schema object.

        :param schema: String representation of an Avro schema.
        :returns: An Avro schema object.
        """
        if six.PY3:
            return avro.schema.Parse(schema)
        else:
            return avro.schema.parse(schema)

    @staticmethod
    def validate(schema, record):
        """ Validate whether or not a record adheres to a schema.

        :param schema: Avro schema object to check record against.
        :param record: Record we are checking.
        :returns: True if the record is valid by the schema, false otherwise.
        :raises AvroTypeException: if an unknown schema type is encountered.
        """
        if six.PY3:
            return Utils._py3_validate(schema, record)
        else:
            return Utils._py2_validate(schema, record)

    @staticmethod
    def _py3_validate(expected_schema, datum):
        return avro.io.Validate(expected_schema, datum)

    @staticmethod
    def _py2_validate(expected_schema, datum):
        return avro.io.validate(expected_schema, datum)

    @staticmethod
    def get_schema_by_name(parent_schema, full_name):
        """Return a schema object from a schema name.

        Return a schema object associated with a name from a schema which may
        either be a union schema, or a hierarchical schema with nested
        subschemas underneath.

        :param parent_schema: Parent schema containing the target schema.
        :param full_name: Schema full name to find in the union schema.
        :returns: Return the schema associated with the provided full_name if
                  it is found in the union schema.  Otherwise, return None.
        """

        # If no proper schema was provided, return None.
        if parent_schema is None:
            return None

        # Find a schema in the full schema that has a full name that
        # matches the provided one.
        # If the provided schema is a union schema, search the subschemas.
        if parent_schema.type == Utils._AVRO_UNION_TYPE:
            for schema in parent_schema.schemas:
                target_schema = Utils.get_schema_by_name(schema, full_name)
                if target_schema is not None:
                    return target_schema

        # If the provided schema has a nested record definition, search that.
        elif parent_schema.type == Utils._AVRO_RECORD_TYPE:

            # If our parent schema has the name we are looking for, return it.
            if parent_schema.fullname == full_name:
                return parent_schema

            for field in parent_schema.fields:
                # For whatever reason, the schema of a record is stored in
                # a field named type.
                schema = field.type
                target_schema = Utils.get_schema_by_name(schema, full_name)
                if target_schema is not None:
                    return target_schema

        # If no matching schema was found, return None.
        return None

    @staticmethod
    def get_schema_by_field_name(parent_schema, name, searched_records=None):
        """ Return a schema object from a schema name or field name.

        Recursively search a schema and its components for the first instance
        of a name in the schema fields.

        :param parent_schema: Parent schema containing the target schema.
        :param name: Schema short name to find in the union schema.
        :returns: Return the schema associated with the provided name if
                  it is found.  Otherwise, return None.
        """

        # Initialize searched_records if needed
        searched_records = [] if searched_records is None else searched_records

        # If no proper schema was provided, return None.
        if parent_schema is None:
            return None

        # Find a schema in the full schema that has a full name that
        # matches the provided one.
        # If the provided schema is a union schema, search the subschemas.
        if parent_schema.type == Utils._AVRO_UNION_TYPE:

            # General union case
            for schema in parent_schema.schemas:
                target_schema = Utils.get_schema_by_field_name(schema, name, searched_records)
                if target_schema is not None:
                    return target_schema

        # If the provided schema has a nested record definition, search that.
        elif parent_schema.type == Utils._AVRO_RECORD_TYPE:

            if parent_schema.name in searched_records:
                return None
            else:
                searched_records.append(parent_schema.name)

            # If our parent schema has the name we are looking for, return it.
            if parent_schema.name == name:
                return parent_schema

            for field in parent_schema.fields:
                # For whatever reason, the schema of a record is stored in
                # a field named type.
                schema = field.type

                if field.name == name:
                    # If this is an optional value, we need to get the
                    # internal type, not the union.
                    if schema.type == Utils._AVRO_UNION_TYPE and \
                            len(schema.schemas) == 2 and \
                            schema.schemas[0].type == "null":
                        return schema.schemas[1]
                    else:
                        # Otherwise, we have found our type.
                        return schema

                target_schema = Utils.get_schema_by_field_name(schema, name, searched_records)
                if target_schema is not None:
                    return target_schema

        elif parent_schema.type == Utils._AVRO_ARRAY_TYPE:
            return Utils.get_schema_by_field_name(parent_schema.items, name, searched_records)

        elif parent_schema.type == Utils._AVRO_MAP_TYPE:
            return Utils.get_schema_by_field_name(parent_schema.values, name, searched_records)

        elif isinstance(parent_schema, avro.schema.NamedSchema):
             if parent_schema.name == name:
                 return parent_schema

        # If no matching schema was found, return None.
        return None

    class Stats(object):
        """
        Container and handlers for statistics collection.
        """

        def __init__(self, n_per_sample, title, unit):
            """Create a Stats object.

            :param n_per_sample: Number of records per sample for statistics
                                 collection.
            :param title: Title of the report.
            :param unit: Unit of measure to use in the report.
            """
            self.min_sample = 0xffffffffffffffff
            self.max_sample = 0
            self.n_per_sample = n_per_sample
            self.title = title
            self.unit = unit
            self.sum_ = 0
            self.count = 0
            self.sample_lock = threading.Lock()

        def sample(self, my_sample):
            """Take a sample as part of statistics collection

            :param my_sample: The sample value to add.
            """
            with self.sample_lock:
                self.sum_ += my_sample
                if my_sample > self.max_sample:
                    self.max_sample = my_sample
                if my_sample < self.min_sample:
                    self.min_sample = my_sample
                self.count += 1

        def __str__(self):
            if self.count == 0:
                return "NO SAMPLES"
            else:
                average = float(self.sum_) / self.count
                lines = []
                lines.append("\n--------------------\n")
                lines.append("###\t")
                lines.append(self.title)
                lines.append(" (")
                lines.append(self.unit)
                lines.append(") ")
                lines.append(" per ")
                lines.append(str(self.n_per_sample))
                lines.append("\n--------------------\n")
                lines.append("Min: ")
                lines.append(str(self.min_sample))
                lines.append("\nMax: ")
                lines.append(str(self.max_sample))
                lines.append("\nAvg: ")
                lines.append(str(average))
                lines.append("\nCount: ")
                lines.append(str(self.count))
                lines.append("\n--------------------\n")
                if self.n_per_sample > 1:
                    lines.append("Avg per sample: ")
                    lines.append(str(average / self.n_per_sample))
                    lines.append("\nTotal count: ")
                    lines.append(str(self.count * self.n_per_sample))
                    lines.append("\n--------------------\n")

                return "".join(lines)


class AvroJsonDecoder(JSONDecoder):

    def __init__(self, *args, **kwargs):
        self.schema = kwargs["schema"]
        del kwargs["schema"]
        # Without setting strict to False, python3's JSON scanner can cause
        # failures for bytes and fixed field values.
        super(AvroJsonDecoder, self).__init__(object_hook=self._decode, strict=False, *args, **kwargs)

    def _contains_non_utf8(self, obj):
        if six.PY3:
            if isinstance(obj, str):
                return not (all(ord(c) < 128 and ord(c) for c in obj))
        else:
            if isinstance(obj, unicode):
                return not (all(ord(c) < 128 and ord(c) for c in obj))
        return False

    def _decode(self, obj):
        for key in obj:
            # Getting the type of the field is expensive, as we are not
            # walking the schema recursively.  Only deal with fields that
            # would cause json deserialization to fail by encoding them now,
            # and for all other bytes/fixed types we will deal with them
            # as part of the deserializer.
            if self._contains_non_utf8(obj[key]):
                obj[key] = obj[key].encode("ISO-8859-1")
        return obj


class AvroJsonDeserializer(object):
    """
    Use this class for avro json deserialization:
        deserializer = AvroJsonDeserializer(avro_schema)
        deserializer.from_json(data)
    """

    """
    This charset will be used to decode binary data for `fixed` and `bytes` types
    """
    BYTES_CHARSET = "ISO-8859-1"

    """
    Charset for JSON. Python uses "utf-8"
    """
    JSON_CHARSET = "utf-8"

    def __init__(self, avro_schema):
        """
        :param avro_schema: instance of `avro.schema.Schema`
        """
        self._avro_schema = avro_schema
        self._json_decoder = AvroJsonDecoder

    def __needs_encoding(self, json_str):
        if six.PY3:
            return not isinstance(json_str, bytes)
        else:
            return (all(ord(c) < 128 and ord(c) for c in json_str))


    def _deserialize_binary_string(self, avro_schema, json_str):
        """
        `fixed` and `bytes` datum  are encoded as UTF-8 for JSON, and we need to re-encde it as "ISO-8859-1".
        """
        if self.__needs_encoding(json_str):
            json_str = json_str.encode(self.BYTES_CHARSET)
        return json_str

    def _deserialize_null(self, *args):
        """
        Always deserialize into None, which will trigger the field to be removed.
        """
        return None

    def _deserialize_array(self, schema, annotated_datum):
        """
        Array is deserialized into array.
        Every element is deserialized recursively according to `items` schema.
        :param schema: Avro schema of `annotated_datum`
        :param datum: Data with Avro json annotation to deserialize
        :return: deserialized array (list)
        """
        if not isinstance(annotated_datum, list):
            raise AvroTypeException(schema, annotated_datum)
        if annotated_datum is None:
            raise AvroTypeException(schema, annotated_datum)
        deserialize = functools.partial(self._deserialize_data, schema.items)
        return list(map(deserialize, annotated_datum))

    def _deserialize_map(self, schema, annotated_datum):
        """
        Map is serialized into a map.
        Every value is deserialized recursively according to `values` schema.
        :param schema: Avro schema of `annotated_datum`
        :param datum: Data with Avro json annotation to deserialize.
        :return: map with deserialized values
        """
        if not isinstance(annotated_datum, dict):
            raise AvroTypeException(schema, annotated_datum)
        deserialize = functools.partial(self._deserialize_data, schema.values)
        return dict((key, deserialize(value)) for key, value in six.iteritems(annotated_datum))

    def _deserialize_union(self, schema, annotated_datum):
        """
        With union schema has multiple possible schemas.
        We iterate over possible schemas and see which one matches the key that
        was in the json.
        Union serialization:
        if null:
            None
        else:
            value
        Then used one that matches to deserialize `annotated_datum`
        :param schema: Avro schema for this union
        :param annotated_datum: Data with Avro json annotation to deserialize
        :return: dict {"type": value} or "null"
        """
        if not annotated_datum:
            return self._deserialize_null()

        if not isinstance(annotated_datum, dict):
            raise AvroTypeException(schema, annotated_datum)

        key = list(annotated_datum.keys())[0]
        for candidate_schema in schema.schemas:
            if isinstance(candidate_schema, avro.schema.NamedSchema):
                if candidate_schema.name == key:
                    return self._deserialize_data(candidate_schema, annotated_datum[key])
            else:
                if candidate_schema.type == key:
                    return self._deserialize_data(candidate_schema, annotated_datum[key])
        raise schema.AvroTypeException(schema, datum)

    def _deserialize_record(self, schema, annotated_datum):
        """
        Records are deserialized into dicts.
        Every field value is deserialized based on it's schema.
        If a value is None, remove the key from the dict.
        :param schema: Avro schema of this record
        :param annotated_datum: Data with Avro json annotation to deserialize
        """
        if not isinstance(annotated_datum, dict):
            raise AvroTypeException(schema, annotated_datum)

        result = {}
        for field in schema.fields:
            val = self._deserialize_data(field.type, annotated_datum.get(field.name))
            if val:
                result[field.name] = val
        return result

    """No need to deserialize primitives
    """
    PRIMITIVE_CONVERTERS = frozenset((
        "boolean",
        "string",
        "int",
        "long",
        "float",
        "double",
        "enum"
    ))

    """Some conversions require custom logic so we have separate functions for them.
    """
    COMPLEX_CONVERTERS = {
        "null": _deserialize_null,
        "array": _deserialize_array,
        "map": _deserialize_map,
        "union": _deserialize_union,
        "error_union": _deserialize_union,
        "record": _deserialize_record,
        "request": _deserialize_record,
        "error": _deserialize_record,
        "fixed": _deserialize_binary_string,
        "bytes": _deserialize_binary_string
    }

    def _deserialize_data(self, schema, datum):
        """
        Non-specific deserialize function.
        It checks type in the schema and calls correct deserialization.
        :param schema: Avro schema of the `datum`
        :param datum: Data to serialize
        """
        if schema.type in AvroJsonDeserializer.PRIMITIVE_CONVERTERS:
            return datum

        if schema.type in AvroJsonDeserializer.COMPLEX_CONVERTERS:
            return self.COMPLEX_CONVERTERS[schema.type](self, schema, datum)

        raise avro.schema.AvroException("Unknown type: %s" % schema.type)

    def from_json(self, json_str):
        args = ()
        kwargs = {"schema": self._avro_schema}
        annotated_datum = json.loads(json_str, cls=self._json_decoder, *args, **kwargs)
        datum = self._deserialize_data(self._avro_schema, annotated_datum)
        if not validate(self._avro_schema, datum):
            raise AvroTypeException(schema, datum)
        return datum


class TcJsonDecoder(JSONDecoder):

    def __init__(self, *args, **kwargs):
        del kwargs["schema"]
        super(TcJsonDecoder, self).__init__(*args, **kwargs)

    def _decode(self, obj):
        return obj


class TcJsonDeserializer(AvroJsonDeserializer):

    def __init__(self, avro_schema):
        super(TcJsonDeserializer, self).__init__(avro_schema)
        self._json_decoder = TcJsonDecoder
        self._update_fps()

    def _deserialize_binary_string(self, avro_schema, datum):
        result = datum
        # If schema type is UUID, which we infer through the length...
        if avro_schema.type == "fixed" and avro_schema.size == 16:
            result = result[:8] + result[9:13] + result[14:18] + result[19:23] + result[24:]

        value = codecs.decode(result, "hex_codec")
        return value

    def _update_fps(self):
        AvroJsonDeserializer.COMPLEX_CONVERTERS["fixed"] = TcJsonDeserializer._deserialize_binary_string
        AvroJsonDeserializer.COMPLEX_CONVERTERS["bytes"] = TcJsonDeserializer._deserialize_binary_string


class TcJsonSerializer(avro_json_serializer.AvroJsonSerializer):

    def __init__(self, avro_schema):
        super(TcJsonSerializer, self).__init__(avro_schema)
        self._update_fps()

    def _serialize_binary_string(self, avro_schema, datum):
        result = codecs.encode(datum, "hex_codec").decode("utf-8")

        # If schema type is UUID, which we infer through the length...
        if avro_schema.type == "fixed" and avro_schema.size == 16:
            result = result[:8] + "-" + result[8:12] + "-" + result[12:16] + "-" + result[16:20] + "-" + result[20:]

        return result

    def _update_fps(self):
        avro_json_serializer.AvroJsonSerializer.COMPLEX_CONVERTERS["fixed"] = TcJsonSerializer._serialize_binary_string
        avro_json_serializer.AvroJsonSerializer.COMPLEX_CONVERTERS["bytes"] = TcJsonSerializer._serialize_binary_string
