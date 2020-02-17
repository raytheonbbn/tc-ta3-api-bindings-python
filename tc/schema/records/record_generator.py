# Copyright (c) 2020 Raytheon BBN Technologies Corp.
# See LICENSE.txt for details.

'''
Created on Feb 22, 2016

@author: bbenyo
'''

import random
import six
import sys
import time


class RecordGenerator(object):

    def __init__(self, serializer):
        self.serializer = serializer

    def generate_random_record(self, num_kv_pairs):
        ''' Generate a record with randomized data and the
            specific number of extra properties '''
        return {}

    def get_average_record_size(self, num_kv_pairs):
        ''' Given a number of extra properties,
            estimate the avg record size. '''
        return 0

    def has_optional_attr(self):
        return (random.randint(1, 2) == 1)


class RecordGeneratorFactory(object):
    ''' Factory for creating record generators by schema name. '''

    LABELED_EDGE_SCHEMA_NAME = "com.bbn.tc.schema.avro.LabeledEdge"
    TC_CDM_DATUM_SCHEMA_NAME = "com.bbn.tc.schema.avro.cdm20.TCCDMDatum"

    @classmethod
    def get_record_generator(cls, serializer):
        ''' Returns a record generator based on the schema name.

        :param serializer: Serializer object to use.
        :returns: Implementation of the RecordGenerator.
        :raises ValueError: If an unsupported serializer is provided.
        '''

        schema_fullname = serializer.writer_schema.fullname

        if schema_fullname == cls.LABELED_EDGE_SCHEMA_NAME:
            return LabeledEdgeGenerator(serializer)
        elif schema_fullname == cls.TC_CDM_DATUM_SCHEMA_NAME:
            return CDMRecordGenerator(serializer)
        else:
            msg = "{s} has no record generator.".format(s=schema_fullname)
            raise ValueError(msg)


class LabeledEdgeGenerator(RecordGenerator):
    '''  Generates random labeled edge records '''

    def __init__(self, serializer):
        '''Create a LabeledEdgeGenerator

        :param serializer: Serializer object for the generator to use.
        '''
        super(LabeledEdgeGenerator, self).__init__(serializer)
        self.record_num = 0

    # TODO: Read these labels directly from the schema
    def random_node_label(self):
        '''Get a random label for the node.

        :returns: A randomly chosen label.
        '''
        labels = {1: 'unitOfExecution', 2: 'artifact', 3: 'agent'}
        return labels[random.randint(1, 3)]

    def generate_random_record(self, num_kv_pairs):
        ''' Generate a labeled edge record instance.

        Generate a labeled edge record instance with the specified number of
        extra kv pairs.

        :param num_kv_pairs: The number of key value pairs to add.
        :returns: A LabeledEdge record.
        '''

        # Create some nodes with properties.
        from_node = {}
        from_node["label"] = self.random_node_label()
        from_node["id"] = random.randint(0, 9223372036854775807)
        # from_node["properties"] = {}
        self.record_num += 1

        to_node = {}
        to_node["label"] = self.random_node_label()
        to_node["id"] = random.randint(0, 9223372036854775807)
        # to_node["properties"] = {}
        self.record_num += 1

        # Create the actual edge record.
        edge = {}
        edge["label"] = "generated"
        edge["fromNode"] = from_node
        edge["toNode"] = to_node
        edge["properties"] = {}

        # Add some arbitrary properties to the nodes.
        for i in range(num_kv_pairs):
            prop_key = "p" + str(i)
            prop_val = str(random.randint(0, 0xffffffff))
            edge["properties"][prop_key] = prop_val

        return edge

    def get_average_record_size(self, num_kv_pairs):
        ''' Get the average record size for a set number of kv pairs.

        Get the average record size for a set number of kv pairs by
        generating 10 random records and averaging the serialized size.

        :param num_kv_pairs: The number of key value pairs that were added.
        :returns: The average record size.
        '''
        total = 0
        for _ in range(0, 10):
            edge = self.generate_random_record(num_kv_pairs)
            message = self.serializer.serialize("dummy", edge)
            total += len(message)

        return total / 10


class CDMAbstractRecordGenerator(RecordGenerator):
    ''' Metaclass for handling AbstractObjects and CDM records '''

    def __init__(self, serializer):
        '''Create a CDMRecordGenerator

        :param serializer: Serializer object for the generator to use.
        '''
        super(CDMAbstractRecordGenerator, self).__init__(serializer)

    def _get_random_properties(self, num_kv_pairs):
        properties = {}

        # Create some arbitrary number of properties for a record.
        for i in range(num_kv_pairs):
            prop_key = "p" + str(i)
            prop_val = str(random.randint(0, 0xffffffff))
            properties[prop_key] = prop_val

        return properties

    def _get_random_instrumentation_source(self):
        #FIXME: actually randomize
        return "SOURCE_LINUX_SYSCALL_TRACE"


class CDMRecordGenerator(CDMAbstractRecordGenerator):
    ''' Generates CDM records '''

    _CDM_VERSION = "20"
    _SESSION_NUM = 0

    def __init__(self, serializer):
        '''Create a CDMRecordGenerator

        :param serializer: Serializer object for the generator to use.
        '''
        super(CDMRecordGenerator, self).__init__(serializer)
        self.record_num = 0

    def generate_random_record(self, num_kv_pairs):
        ''' Generate a CDM record instance.

        Generate a CDM record instance of random type with the specified number
        of extra kv pairs.

        :param num_kv_pairs: The number of key value pairs to add.
        :returns: A CDM record.
        '''
        # Choose a random CDM subclass and call it's generate_random_record.
        types = CDMRecordGenerator.__subclasses__()
        choice = types[random.randint(0, len(types) - 1)]
        instance = choice(self.serializer)
        return instance.generate_random_record(num_kv_pairs)

    def get_average_record_size(self, num_kv_pairs):
        ''' Get the average record size for a set number of kv pairs.

        Get the average record size for a set number of kv pairs by
        generating 10 random records and averaging the serialized size.

        :param num_kv_pairs: The number of key value pairs that were added.
        :returns: The average record size.
        '''
        total = 0
        for _ in range(0, 10):
            edge = self.generate_random_record(num_kv_pairs)
            message = self.serializer.serialize("dummy", edge)
            total += len(message)

        return total / 10


class CDMProvenanceTagNodeGenerator(CDMRecordGenerator):
    ''' Generates random CDM ProvenanceTagNode records '''

    _DEFAULT_CHILD_PERCENT = 25
    _MAX_NUM_CHILDREN = 4
    _VALUE_TYPES = ["int", "UUID", "tagop", "itag", "ctag"]

    def __init__(self, serializer, child_percent=None):
        super(CDMProvenanceTagNodeGenerator, self).__init__(serializer)
        self.child_percent = child_percent if child_percent is not None else \
                CDMProvenanceTagNodeGenerator._DEFAULT_CHILD_PERCENT

    def _get_random_tag_op_code(self):
        #FIXME: actually randomize
        return "TAG_OP_UNION"

    def _get_random_integrity_tag(self):
        #FIXME: actually randomize
        return "INTEGRITY_UNTRUSTED"

    def _get_random_confidentiality_tag(self):
        #FIXME: actually randomize
        return "CONFIDENTIALITY_SECRET"

    def _get_random_tag_id(self):
        return Util.get_random_uuid()

    def _get_random_flow_object(self):
        return Util.get_random_uuid()

    def _get_random_subject(self):
        return Util.get_random_uuid()

    def _get_random_system_call(self):
        return "open"

    def _get_random_program_point(self):
        return "MyCode"

    def generate_random_record(self, num_kv_pairs):
        ''' Generate a CDM ProvenanceTagNode record instance.

        Generate a CDM ProvenanceTagNode record instance with the specified
        number of extra kv pairs.

        :param num_kv_pairs: The number of key value pairs to add.
        :returns: A CDM ProvenanceTagNode record.
        '''
        record = {}
        record["datum"] = self.generate_random_node_record(num_kv_pairs)
        record["source"] = self._get_random_instrumentation_source()
        record["CDMVersion"] = CDMRecordGenerator._CDM_VERSION
        record["hostId"] = Util.get_random_uuid()
        record["sessionNumber"] = CDMRecordGenerator._SESSION_NUM
        record["type"] = "RECORD_PROVENANCE_TAG_NODE"
        self.record_num += 1
        return record

    def generate_random_node_record(self, num_kv_pairs):
        # Initialize fields for an ProvenanceTagNode record.
        provenance_tag_node = {}

        # Populate the ProvenanceTagNode record required attributes.
        provenance_tag_node["tagId"] = self._get_random_tag_id()
        provenance_tag_node["subject"] = self._get_random_subject()

        # Optional attributes
        if self.has_optional_attr():
            provenance_tag_node["flowObject"] = self._get_random_flow_object()

        if self.has_optional_attr():
            provenance_tag_node["systemCall"] = self._get_random_system_call()

        if self.has_optional_attr():
            provenance_tag_node["programPoint"] = self._get_random_program_point()

        if self.has_optional_attr():
            provenance_tag_node["prevTagId"] = self._get_random_tag_id()

        if self.has_optional_attr():
            provenance_tag_node["opcode"] = self._get_random_tag_op_code()
            provenance_tag_node["tagIds"] = [self._get_random_tag_id(), self._get_random_tag_id()]

        if self.has_optional_attr():
            provenance_tag_node["itag"] = self._get_random_integrity_tag()

        if self.has_optional_attr():
            provenance_tag_node["ctag"] = self._get_random_confidentiality_tag()

        provenance_tag_node["properties"] = \
                self._get_random_properties(num_kv_pairs)

        return provenance_tag_node


class CDMSubjectGenerator(CDMRecordGenerator):
    ''' Generates random CDM Subject records '''

    def __init__(self, serializer):
        super(CDMSubjectGenerator, self).__init__(serializer)

    def _get_random_subject_type(self):
        #FIXME: actually randomize
        return "SUBJECT_PROCESS"

    def _get_random_privilege_level(self):
        return "ELEVATED"

    def _get_random_parent_subject(self):
        return Util.get_random_uuid()

    def _get_random_local_principal(self):
        return Util.get_random_uuid()

    def generate_random_record(self, num_kv_pairs):
        ''' Generate a CDM Subject record instance.

        Generate a CDM Subject record instance with the specified
        number of extra kv pairs.

        :param num_kv_pairs: The number of key value pairs to add.
        :returns: A CDM Subject record.
        '''

        # Initialize fields for an Subject record.
        record = {}
        subject = {}

        # Populate the Subject record.
        subject["uuid"] = Util.get_random_uuid()
        subject["type"] = self._get_random_subject_type()
        subject["cid"] = random.randint(0, 0xffff)

        if self.has_optional_attr():
            subject["localPrincipal"] = self._get_random_local_principal()

        if self.has_optional_attr():
            subject["startTimestampNanos"] = int(time.time() * 1000000000) - 1234

        if self.has_optional_attr():
            subject["parentSubject"] = self._get_random_parent_subject()

        if self.has_optional_attr():
            subject["unitId"] = random.randint(0, 0xffff)

        if self.has_optional_attr():
            subject["iteration"] = random.randint(0, 0xffff)
            subject["count"] = random.randint(0, 0xffff)

        if self.has_optional_attr():
            subject["cmdLine"] = "fake a b c"

        if self.has_optional_attr():
            subject["privilegeLevel"] = self._get_random_privilege_level()

        if self.has_optional_attr():
            subject["importedLibraries"] = ["lib1", "lib2"]

        if self.has_optional_attr():
            subject["exportedLibraries"] = ["lib3", "lib4"]

        subject["properties"] = self._get_random_properties(num_kv_pairs)

        # Increment the record number for this record.
        self.record_num += 1

        # Wrap the subject in a datum record.
        record["datum"] = subject
        record["source"] = self._get_random_instrumentation_source()
        record["CDMVersion"] = CDMRecordGenerator._CDM_VERSION
        record["hostId"] = Util.get_random_uuid()
        record["sessionNumber"] = CDMRecordGenerator._SESSION_NUM
        record["type"] = "RECORD_SUBJECT"
        return record


class CDMTagRunLengthTupleGenerator(object):

    _MAX_NUM_ELEMENTS = 8

    def _get_random_tag_id(self):
        return Util.get_random_uuid()

    def get_random_tag_run_length_tuple(self):
        tag_run_length_tuple = {}
        tag_run_length_tuple["numValueElements"] = random.randint(0, CDMTagRunLengthTupleGenerator._MAX_NUM_ELEMENTS)
        tag_run_length_tuple["tagId"] = self._get_random_tag_id()
        return tag_run_length_tuple


class CDMCryptographicHashGenerator(object):

    def _get_random_crypto_hash_type(self):
        return "SHA256"

    def _get_random_hash(self):
        return "4ad79772ea95d6f5775f293e47bee7f47f08d5b90356490357824893bc097370"

    def get_random_cryptographic_hash(self):
        cryptographic_hash = {}
        cryptographic_hash["type"] = self._get_random_crypto_hash_type()
        cryptographic_hash["hash"] = self._get_random_hash()
        return cryptographic_hash


class CDMProvenanceAssertionGenerator(object):

    _MAX_NUM_ASSERTIONS = 5

    def has_optional_attr(self):
        return (random.randint(1, 2) == 1)

    def get_random_provenance_assertion(self, num_assertions=0):

        if num_assertions >= CDMProvenanceAssertionGenerator._MAX_NUM_ASSERTIONS:
            return None

        provenance_assertion = {}
        provenance_assertion["asserter"] = Util.get_random_uuid()

        if self.has_optional_attr():
            provenance_assertion["sources"] = [Util.get_random_uuid() for i in range(random.randint(1, 5))]

        if self.has_optional_attr():
            val = self.get_random_provenance_assertion(num_assertions + 1)
            if val is not None:
                provenance_assertion["provenance"] = [val]

        return provenance_assertion

class CDMValueGenerator(object):

    _MAX_SIZE_BYTES = 8
    _DEFAULT_COMPONENT_PERCENT = 15
    _MAX_NUM_COMPONENTS = 5

    def __init__(self, component_percent=None):
        if component_percent is not None:
            self.component_percent = component_percent
        else:
            self.component_percent = \
                    CDMValueGenerator._DEFAULT_COMPONENT_PERCENT
        self.pag = CDMProvenanceAssertionGenerator()

    def _get_random_value_type(self):
        #FIXME: actually randomize
        return "VALUE_TYPE_SRC"

    def _get_random_data_type(self):
        return "VALUE_DATA_TYPE_BYTE"

    def _get_random_bytes(self, size):
        return Util.get_random_bytes(size)

    def _get_random_tag(self):
        # For now, just create a single tag for the whole value.
        rltg = CDMTagRunLengthTupleGenerator()
        tag = [rltg.get_random_tag_run_length_tuple()]
        return tag

    def _get_components(self):
        # Generate components (or not) based on a weigted coin flip.
        components = None
        has_components = (random.randint(0, 100) < self.component_percent)

        # If the there are components, generate a random number of them with a
        # limit.  Note that each component could have sub-components.
        if has_components:
            # Exponentially decrease the chance that we have more children.
            self.component_percent /= 2

            num_components = random.randint(1,
                    CDMValueGenerator._MAX_NUM_COMPONENTS)
            components = []
            for i in range(num_components):
                components.append(self.get_random_value())

        return components

    def _get_random_name(self):
        return "PrettayPrettayPrettyGood"

    def _get_random_runtime_data_type(self):
        return "MyClass"

    def has_optional_attr(self):
        return (random.randint(1, 2) == 1)

    def get_random_value(self):
        value = {}
        size = random.randint(0, CDMValueGenerator._MAX_SIZE_BYTES)
        value["size"] = size
        value["type"] = self._get_random_value_type()
        value["valueDataType"] = self._get_random_data_type()
        value["isNull"] = False

        if self.has_optional_attr():
            value["name"] = self._get_random_name()

        if self.has_optional_attr():
            value["runtimeDataType"] = self._get_random_runtime_data_type()

        if self.has_optional_attr():
            value["valueBytes"] = self._get_random_bytes(size)

        if self.has_optional_attr():
            value["provenance"] = [self.pag.get_random_provenance_assertion()]

        if self.has_optional_attr():
            value["tag"] = self._get_random_tag()

        if self.has_optional_attr():
            value["components"] = self._get_components()

        value["valueBytes"] = self._get_random_bytes(size)
        value["tag"] = self._get_random_tag()
        value["components"] = self._get_components()
        return value


class CDMEventGenerator(CDMRecordGenerator):
    ''' Generates random CDM Event records '''

    _MAX_NUM_PARAMETERS = 5

    def __init__(self, serializer):
        super(CDMEventGenerator, self).__init__(serializer)
        self._sequence = 0

    def _get_random_value(self):
        value_generator = CDMValueGenerator()
        return value_generator.get_random_value()

    def _get_random_parameters(self):
        parameters = []
        num_parameters = random.randint(0,
                CDMEventGenerator._MAX_NUM_PARAMETERS)
        for i in range(num_parameters):
            parameters.append(self._get_random_value())

        return parameters

    def _get_random_event_type(self):
        #FIXME: actually randomize
        return "EVENT_ACCEPT"

    def _get_random_subject(self):
        return Util.get_random_uuid()

    def _get_random_predicate_object(self):
        return Util.get_random_uuid()

    def _get_random_predicate_object_path(self):
        return "/the/beaten/path"

    def _get_random_program_point(self):
        return "SomeoneElsesCode"

    def generate_random_record(self, num_kv_pairs):
        ''' Generate a CDM Event record instance.

        Generate a CDM Event record instance with the specified
        number of extra kv pairs.

        :param num_kv_pairs: The number of key value pairs to add.
        :returns: A CDM Event record.
        '''

        # Initialize fields for an Event record.
        record = {}
        event = {}

        # Populate the Event record.
        event["uuid"] = Util.get_random_uuid()
        event["type"] = self._get_random_event_type()
        event["timestampNanos"] = int(time.time() * 1000000000)

        if self.has_optional_attr():
            event["sequence"] = self._sequence
            self._sequence = self._sequence + 1

        if self.has_optional_attr():
            event["threadId"] = random.randint(0, 0xffff)

        if self.has_optional_attr():
            event["subject"] = self._get_random_subject()

        if self.has_optional_attr():
            event["predicateObject"] = self._get_random_predicate_object()
            event["predicateObjectPath"] = self._get_random_predicate_object_path()

            if self.has_optional_attr():
                event["predicateObject2"] = self._get_random_predicate_object()
                event["predicateObject2Path"] = self._get_random_predicate_object_path()

        if self.has_optional_attr():
            event["names"] = ["InsertNameHere", "name2"]

        if self.has_optional_attr():
            event["parameters"] = self._get_random_parameters()

        if self.has_optional_attr():
            event["location"] = random.randint(0, 0xfffffff)

        if self.has_optional_attr():
            event["size"] = random.randint(0, 0xfffffff)

        if self.has_optional_attr():
            event["programPoint"] = self._get_random_program_point()

        event["properties"] = self._get_random_properties(num_kv_pairs)

        # Increment the record number for this record.
        self.record_num += 1

        # Wrap the event in a datum record.
        record["datum"] = event
        record["source"] = self._get_random_instrumentation_source()
        record["CDMVersion"] = CDMRecordGenerator._CDM_VERSION
        record["hostId"] = Util.get_random_uuid()
        record["sessionNumber"] = CDMRecordGenerator._SESSION_NUM
        record["type"] = "RECORD_EVENT"
        return record


class CDMAbstractObjectGenerator(CDMAbstractRecordGenerator):
    ''' Generates random CDM AbstractObject records '''

    def __init__(self, serializer):
        super(CDMAbstractObjectGenerator, self).__init__(serializer)

    def generate_random_record(self, num_kv_pairs):
        ''' Generate a CDM AbstractObject record instance.

        Generate a CDM AbstractObject record instance with the specified
        number of extra kv pairs.

        :param num_kv_pairs: The number of key value pairs to add.
        :returns: A CDM AbstractObject record.
        '''

        abstract_object = {}

        if self.has_optional_attr():
            abstract_object["permission"] = Util.get_random_short()

        if self.has_optional_attr():
            abstract_object["epoch"] = random.randint(0, 0xffff)

        abstract_object["properties"] = \
                self._get_random_properties(num_kv_pairs)

        return abstract_object


class CDMNetFlowObjectGenerator(CDMRecordGenerator):
    ''' Generates random CDM NetFlowObject records '''

    def __init__(self, serializer):
        super(CDMNetFlowObjectGenerator, self).__init__(serializer)
        self.abstract_obj_generator = CDMAbstractObjectGenerator(serializer)

    def generate_random_record(self, num_kv_pairs):
        ''' Generate a CDM NetFlowObject record instance.

        Generate a CDM NetFlowObject record instance with the specified
        number of extra kv pairs.

        :param num_kv_pairs: The number of key value pairs to add.
        :returns: A CDM NetFlowObject record.
        '''

        # Initialize fields for a NetFlowObject record.
        record = {}
        net_flow_object = {}

        # Populate the NetFlowObject record.
        net_flow_object["uuid"] = Util.get_random_uuid()
        net_flow_object["baseObject"] = \
                self.abstract_obj_generator.generate_random_record(num_kv_pairs)

        # This field is optional, but we force it to be included to avoid the case
        # where all 4 fields are null.
        net_flow_object["localAddress"] = "1.2.3.4"

        if self.has_optional_attr():
            net_flow_object["localPort"] = random.randint(0, 0xffff)

        if self.has_optional_attr():
            net_flow_object["remoteAddress"] = "5.6.7.8"

        if self.has_optional_attr():
            net_flow_object["remotePort"] = random.randint(0, 0xffff)

        if self.has_optional_attr():
            net_flow_object["ipProtocol"] = 6

        if self.has_optional_attr():
            net_flow_object["fileDescriptor"] = random.randint(0, 0xffff)

        if self.has_optional_attr():
            net_flow_object["initTcpSeqNum"] = random.randint(0, 0xffff)

        # Increment the record number for this record.
        self.record_num += 1

        # Wrap the net_flow_object in a datum record.
        record["datum"] = net_flow_object
        record["source"] = self._get_random_instrumentation_source()
        record["CDMVersion"] = CDMRecordGenerator._CDM_VERSION
        record["hostId"] = Util.get_random_uuid()
        record["sessionNumber"] = CDMRecordGenerator._SESSION_NUM
        record["type"] = "RECORD_NET_FLOW_OBJECT"
        return record


class CDMFileObjectGenerator(CDMRecordGenerator):
    ''' Generates random CDM FileObject records '''

    def __init__(self, serializer):
        super(CDMFileObjectGenerator, self).__init__(serializer)
        self.abstract_obj_generator = CDMAbstractObjectGenerator(serializer)

    def _get_random_local_principal(self):
        return Util.get_random_uuid()

    def _get_random_pe_info(self):
        return "RandomPeInfo"

    def _get_random_file_object_type(self):
        return "FILE_OBJECT_FILE"

    def _get_random_hashes(self):
        chg = CDMCryptographicHashGenerator()
        return [chg.get_random_cryptographic_hash()]

    def generate_random_record(self, num_kv_pairs):
        ''' Generate a CDM FileObject record instance.

        Generate a CDM FileObject record instance with the specified
        number of extra kv pairs.

        :param num_kv_pairs: The number of key value pairs to add.
        :returns: A CDM FileObject record.
        '''

        # Initialize fields for a FileObject record.
        record = {}
        file_object = {}

        # Populate the FileObject record.
        file_object["uuid"] = Util.get_random_uuid()
        file_object["baseObject"] = \
                self.abstract_obj_generator.generate_random_record(num_kv_pairs)
        file_object["type"] = self._get_random_file_object_type()

        if self.has_optional_attr():
            file_object["fileDescriptor"] = random.randint(0, 0xffff)

        if self.has_optional_attr():
            file_object["localPrincipal"] = self._get_random_local_principal()

        if self.has_optional_attr():
            file_object["size"] = random.randint(0, 0xffffffff)

        if self.has_optional_attr():
            file_object["peInfo"] = self._get_random_pe_info()

        if self.has_optional_attr():
            file_object["hashes"] = self._get_random_hashes()

        # Increment the record number for this record.
        self.record_num += 1

        # Wrap the file_object in a datum record.
        record["datum"] = file_object
        record["source"] = self._get_random_instrumentation_source()
        record["CDMVersion"] = CDMRecordGenerator._CDM_VERSION
        record["hostId"] = Util.get_random_uuid()
        record["sessionNumber"] = CDMRecordGenerator._SESSION_NUM
        record["type"] = "RECORD_FILE_OBJECT"
        return record


class CDMIpcObjectGenerator(CDMRecordGenerator):

    def __init__(self, serializer):
        super(CDMIpcObjectGenerator, self).__init__(serializer)
        self.abstract_obj_generator = CDMAbstractObjectGenerator(serializer)

    def generate_random_record(self, num_kv_pairs):
        ''' Generate a CDM IpcObject record instance.

        Generate a CDM IpcObject record instance with the specified
        number of extra kv pairs.

        :param num_kv_pairs: The number of key value pairs to add.
        :returns: A CDM IpcObject record.
        '''

        # Initialize fields for a IpcObject record.
        record = {}
        ipc_object = {}

        # Populate the IpcObject record.
        ipc_object["uuid"] = Util.get_random_uuid()
        ipc_object["baseObject"] = \
                self.abstract_obj_generator.generate_random_record(num_kv_pairs)
        ipc_object["type"] = "IPC_OBJECT_PIPE_UNNAMED"

        if self.has_optional_attr():
            ipc_object["fd1"] = random.randint(0, 0xffff)
            ipc_object["fd2"] = random.randint(0, 0xffff)
        else:
            ipc_object["uuid1"] = Util.get_random_uuid()
            ipc_object["uuid2"] = Util.get_random_uuid()

        # Increment the record number for this record.
        self.record_num += 1

        # Wrap the src_sink_object in a datum record.
        record["datum"] = ipc_object
        record["source"] = self._get_random_instrumentation_source()
        record["CDMVersion"] = CDMRecordGenerator._CDM_VERSION
        record["hostId"] = Util.get_random_uuid()
        record["sessionNumber"] = CDMRecordGenerator._SESSION_NUM
        record["type"] = "RECORD_IPC_OBJECT"
        return record


class CDMSrcSinkObjectGenerator(CDMRecordGenerator):
    ''' Generates random CDM SrcSinkObject records '''

    def __init__(self, serializer):
        super(CDMSrcSinkObjectGenerator, self).__init__(serializer)
        self.abstract_obj_generator = CDMAbstractObjectGenerator(serializer)

    def _get_random_src_sink_type(self):
        #FIXME: actually randomize
        return "SRCSINK_ACCELEROMETER"

    def generate_random_record(self, num_kv_pairs):
        ''' Generate a CDM SrcSinkObject record instance.

        Generate a CDM SrcSinkObject record instance with the specified
        number of extra kv pairs.

        :param num_kv_pairs: The number of key value pairs to add.
        :returns: A CDM SrcSinkObject record.
        '''

        # Initialize fields for a SrcSinkObject record.
        record = {}
        src_sink_object = {}

        # Populate the SrcSinkObject record.
        src_sink_object["uuid"] = Util.get_random_uuid()
        src_sink_object["baseObject"] = \
                self.abstract_obj_generator.generate_random_record(num_kv_pairs)
        src_sink_object["type"] = self._get_random_src_sink_type()

        if self.has_optional_attr():
            src_sink_object["fileDescriptor"] = random.randint(0, 0xffff)


        # Increment the record number for this record.
        self.record_num += 1

        # Wrap the src_sink_object in a datum record.
        record["datum"] = src_sink_object
        record["source"] = self._get_random_instrumentation_source()
        record["CDMVersion"] = CDMRecordGenerator._CDM_VERSION
        record["hostId"] = Util.get_random_uuid()
        record["sessionNumber"] = CDMRecordGenerator._SESSION_NUM
        record["type"] = "RECORD_SRC_SINK_OBJECT"
        return record


class CDMMemoryObjectGenerator(CDMRecordGenerator):
    ''' Generates random CDM MemoryObject records '''

    def __init__(self, serializer):
        super(CDMMemoryObjectGenerator, self).__init__(serializer)
        self.abstract_obj_generator = CDMAbstractObjectGenerator(serializer)

    def generate_random_record(self, num_kv_pairs):
        ''' Generate a CDM MemoryObject record instance.

        Generate a CDM MemoryObject record instance with the specified
        number of extra kv pairs.

        :param num_kv_pairs: The number of key value pairs to add.
        :returns: A CDM MemoryObject record.
        '''

        # Initialize fields for a MemoryObject record.
        record = {}
        memory_object = {}

        # Populate the MemoryObject record.
        memory_object["uuid"] = Util.get_random_uuid()
        memory_object["baseObject"] = \
                self.abstract_obj_generator.generate_random_record(num_kv_pairs)
        memory_object["memoryAddress"] = random.randint(0, 0xffffffff)

        if self.has_optional_attr():
            memory_object["pageNumber"] = random.randint(0, 0xffffffff)
            memory_object["pageOffset"] = random.randint(0, 0xffffffff)

        if self.has_optional_attr():
            memory_object["size"] = random.randint(0, 0xffffffff)

        # Increment the record number for this record.
        self.record_num += 1

        # Wrap the memory_object in a datum record.
        record["datum"] = memory_object
        record["source"] = self._get_random_instrumentation_source()
        record["CDMVersion"] = CDMRecordGenerator._CDM_VERSION
        record["hostId"] = Util.get_random_uuid()
        record["sessionNumber"] = CDMRecordGenerator._SESSION_NUM
        record["type"] = "RECORD_MEMORY_OBJECT"
        return record


class CDMPrincipalGenerator(CDMRecordGenerator):
    ''' Generates random CDM Principal records '''

    _MAX_NUM_GROUPS = 8

    def __init__(self, serializer):
        super(CDMPrincipalGenerator, self).__init__(serializer)

    def _get_random_principal_type(self):
        #FIXME: actually randomize
        return "PRINCIPAL_LOCAL"

    def _get_random_user_id(self):
        #FIXME: actually randomize
        return "1002"

    def _get_random_username(self):
        #FIXME: actually randomize
        return "someuser"

    def generate_random_record(self, num_kv_pairs):
        ''' Generate a CDM Principal record instance.

        Generate a CDM Principal record instance with the specified
        number of extra kv pairs.

        :param num_kv_pairs: The number of key value pairs to add.
        :returns: A CDM Principal record.
        '''

        # Initialize fields for a Principal record.
        record = {}
        principal = {}

        # Populate the Principal record.
        principal["uuid"] = Util.get_random_uuid()
        principal["type"] = self._get_random_principal_type()
        principal["userId"] = self._get_random_user_id()
        principal["properties"] = self._get_random_properties(num_kv_pairs)

        if self.has_optional_attr():
            principal["username"] = self._get_random_username()

        # Add some arbitrary number of group IDs.
        if self.has_optional_attr():
            principal["groupIds"] = ["123"]

            for j in range(random.randint(0,
                    CDMPrincipalGenerator._MAX_NUM_GROUPS)):
                principal["groupIds"].append(str(random.randint(0, 0xffff)))

        # Increment the record number for this record.
        self.record_num += 1

        # Wrap the principal in a datum record.
        record["datum"] = principal
        record["source"] = self._get_random_instrumentation_source()
        record["CDMVersion"] = CDMRecordGenerator._CDM_VERSION
        record["hostId"] = Util.get_random_uuid()
        record["sessionNumber"] = CDMRecordGenerator._SESSION_NUM
        record["type"] = "RECORD_PRINCIPAL"
        return record


class CDMRegistryKeyObjectGenerator(CDMRecordGenerator):
    ''' Generates random CDM RegistryKeyObject records '''

    def __init__(self, serializer):
        super(CDMRegistryKeyObjectGenerator, self).__init__(serializer)
        self.abstract_obj_generator = CDMAbstractObjectGenerator(serializer)
        self.value_generator = CDMValueGenerator()

    def generate_random_record(self, num_kv_pairs):
        ''' Generate a CDM RegistryKeyObject record instance.

        Generate a CDM RegistryKeyObject record instance with the specified
        number of extra kv pairs.

        :param num_kv_pairs: The number of key value pairs to add.
        :returns: A CDM RegistryKeyObject record.
        '''

        # Initialize fields for a RegistryKeyObject record.
        record = {}
        registry_key_object = {}

        # Populate the RegistryKeyObject record.
        registry_key_object["uuid"] = Util.get_random_uuid()
        registry_key_object["baseObject"] = \
                self.abstract_obj_generator.generate_random_record(num_kv_pairs)
        registry_key_object["key"] = "SomeKey"
        registry_key_object["value"] = self.value_generator.get_random_value()
        registry_key_object["size"] = random.randint(0, 0xffffffff)

        # Increment the record number for this record.
        self.record_num += 1

        # Wrap the registry_key_object in a datum record.
        record["datum"] = registry_key_object
        record["source"] = self._get_random_instrumentation_source()
        record["CDMVersion"] = CDMRecordGenerator._CDM_VERSION
        record["hostId"] = Util.get_random_uuid()
        record["sessionNumber"] = CDMRecordGenerator._SESSION_NUM
        record["type"] = "RECORD_REGISTRY_KEY_OBJECT"
        return record


class CDMTimeMarkerGenerator(CDMRecordGenerator):

    def __init__(self, serializer):
        super(CDMTimeMarkerGenerator, self).__init__(serializer)

    def generate_random_record(self, num_kv_pairs=None):
        record = {}
        time_marker = {}
        time_marker["tsNanos"] = int(time.time() * 1000000000)

        self.record_num += 1

        record["datum"] = time_marker
        record["source"] = self._get_random_instrumentation_source()
        record["CDMVersion"] = CDMRecordGenerator._CDM_VERSION
        record["hostId"] = Util.get_random_uuid()
        record["sessionNumber"] = CDMRecordGenerator._SESSION_NUM
        record["type"] = "RECORD_TIME_MARKER"
        return record


class CDMUnitDependencyGenerator(CDMRecordGenerator):

    def __init__(self, serializer):
        super(CDMUnitDependencyGenerator, self).__init__(serializer)

    def generate_random_record(self, num_kv_pairs=None):
        record = {}
        unit_dependency = {}
        unit_dependency["unit"] = Util.get_random_uuid()
        unit_dependency["dependentUnit"] = Util.get_random_uuid()

        self.record_num += 1

        record["datum"] = unit_dependency
        record["source"] = self._get_random_instrumentation_source()
        record["CDMVersion"] = CDMRecordGenerator._CDM_VERSION
        record["hostId"] = Util.get_random_uuid()
        record["sessionNumber"] = CDMRecordGenerator._SESSION_NUM
        record["type"] = "RECORD_UNIT_DEPENDENCY"
        return record

class CDMHostIdentifierGenerator(object):

    def get_random_serial(self):
        host_identifier = {}
        host_identifier["idType"] = "serial"
        host_identifier["idValue"] = "12345678904A"
        return host_identifier

    def get_random_imei(self):
        host_identifier = {}
        host_identifier["idType"] = "IMEI"
        host_identifier["idValue"] = "123456789012345"
        return host_identifier

    def generate_random_host_identifier(self):
        host_identifier = {}
        if random.random() > 0.5:
            return get_random_serial()
        else:
            return get_random_imei()

class CDMInterfaceGenerator(object):

    def generate_random_interface(self, num_kv_pairs=None):
        interface = {}
        randval = str(random.randint(0, 99))
        interface["name"] = "em" + randval
        interface["macAddress"] = "de:ad:be:a7:da:" + randval
        if random.random() > 0.5:
            interface["ipAddresses"] = ["1.2.%s.4" % randval, "2018:db::3%s" % randval]
        return interface

class CDMHostGenerator(CDMRecordGenerator):

    def __init__(self, serializer):
        super(CDMHostGenerator, self).__init__(serializer)
        self.hig = CDMHostIdentifierGenerator()
        self.intg = CDMInterfaceGenerator()

    def generate_random_record(self, num_kv_pairs=None):
        record = {}
        host = {}
        host["uuid"] = Util.get_random_uuid()
        host["hostName"] = "host123"
        host["hostType"] = "HOST_DESKTOP"
        host["ta1Version"] = "1234567890-2019-02-04T12:00:00"

        if self.has_optional_attr():
            host["hostIdentifiers"] = [self.hig.get_random_serial(), self.hig.get_random_imei()]

        if self.has_optional_attr():
            host["osDetails"] = "Linux host123 4.4.0-96-generic #119~14.04.1-Ubuntu SMP Wed Sep 13 08:40:48 UTC 2017 x86_64 x86_64 x86_64 GNU/Linux"

        if self.has_optional_attr():
            int1 = self.intg.generate_random_interface()
            int2 = self.intg.generate_random_interface()
            if int1["name"] != int2["name"]:
                host["interfaces"] = [int1, int2]
            else:
                host["interfaces"] = [int1]

        self.record_num += 1

        record["datum"] = host
        record["source"] = self._get_random_instrumentation_source()
        record["CDMVersion"] = CDMRecordGenerator._CDM_VERSION
        record["hostId"] = Util.get_random_uuid()
        record["sessionNumber"] = CDMRecordGenerator._SESSION_NUM
        record["type"] = "RECORD_HOST"
        return record


class CDMPacketSocketObjectGenerator(CDMRecordGenerator):

    def __init__(self, serializer):
        super(CDMPacketSocketObjectGenerator, self).__init__(serializer)
        self.abstract_obj_generator = CDMAbstractObjectGenerator(serializer)

    def generate_random_record(self, num_kv_pairs):
        record = {}
        packet_socket_object = {}
        packet_socket_object["uuid"] = Util.get_random_uuid()
        packet_socket_object["baseObject"] = \
                self.abstract_obj_generator.generate_random_record(num_kv_pairs)
        packet_socket_object["proto"] = Util.get_random_short()
        packet_socket_object["ifIndex"] = 0
        packet_socket_object["haType"] = Util.get_random_short()
        packet_socket_object["pktType"] = Util.get_random_byte()
        packet_socket_object["addr"] = Util.get_random_bytes(6)

        self.record_num += 1

        record["datum"] = packet_socket_object
        record["source"] = self._get_random_instrumentation_source()
        record["CDMVersion"] = CDMRecordGenerator._CDM_VERSION
        record["hostId"] = Util.get_random_uuid()
        record["sessionNumber"] = CDMRecordGenerator._SESSION_NUM
        record["type"] = "RECORD_PACKET_SOCKET_OBJECT"
        return record


class CDMEndMarkerGenerator(CDMRecordGenerator):

    def __init__(self, serializer):
        super(CDMEndMarkerGenerator, self).__init__(serializer)
        self.session_num = 0

    def generate_random_record(self, num_kv_pairs=None):
        record = {}
        end_marker = {}

        end_marker["sessionNumber"] = self.session_num
        self.session_num += 1
        end_marker["recordCounts"] = {"Host": "1", "Subject": "10", "FileObject": "3"}

        self.record_num += 1

        record["datum"] = end_marker
        record["source"] = self._get_random_instrumentation_source()
        record["CDMVersion"] = CDMRecordGenerator._CDM_VERSION
        record["hostId"] = Util.get_random_uuid()
        record["sessionNumber"] = CDMRecordGenerator._SESSION_NUM
        record["type"] = "RECORD_END_MARKER"
        return record


class CDMUnknownProvenanceNodeGenerator(CDMRecordGenerator):

    def __init__(self, serializer):
        super(CDMUnknownProvenanceNodeGenerator, self).__init__(serializer)

    def generate_random_record(self, num_kv_pairs):
        record = {}
        unknown_provenance_node = {}

        unknown_provenance_node["upnTagId"] = Util.get_random_uuid()

        if self.has_optional_attr():
            unknown_provenance_node["subject"] = Util.get_random_uuid()

        if self.has_optional_attr():
            unknown_provenance_node["programPoint"] = "RandomProgramPoint"

        unknown_provenance_node["properties"] = self._get_random_properties(num_kv_pairs)

        self.record_num += 1

        record["datum"] = unknown_provenance_node
        record["source"] = self._get_random_instrumentation_source()
        record["CDMVersion"] = CDMRecordGenerator._CDM_VERSION
        record["hostId"] = Util.get_random_uuid()
        record["sessionNumber"] = CDMRecordGenerator._SESSION_NUM
        record["type"] = "RECORD_UNKNOWN_PROVENANCE_NODE"
        return record


class Util(object):
    """
    Class with utility methods for working with Avro types.
    """

    # Custom types' length in bytes
    _UUID_LENGTH = 16
    _SHORT_LENGTH = 2
    _BYTE_LENGTH = 1

    @classmethod
    def _set_by_value(cls, value, length=None):
        length = length if length else cls._get_value_length(value)
        if six.PY3:
            return cls._set_by_value_py3(value, length)
        else:
            return cls._set_by_value_py2(value, length)

    @classmethod
    def _get_value_length(cls, value):
        bl = value.bit_length()
        if (bl % 8 != 0):
            return bl // 8 + 1
        else:
            return bl // 8

    @classmethod
    def _set_by_value_py3(cls, value, length):
        value = b"".join(((value >> (8 * i)) & 0xFF).to_bytes(1, "big")
                for i in reversed(range(length)))
        return value

    @classmethod
    def _set_by_value_py2(cls, value, length):
        value = "".join(chr(((value >> (8 * i)) & 0xFF))
                for i in reversed(range(length)))
        return value

    @classmethod
    def get_random_bytes(cls, size=None):
        """
        Get a random Avro bytes type with a specific size.
        """
        value = None
        if size is None:
            value = cls._set_by_value(random.randint(0, 0xffffffff), size)
        else:
            max_by_size = (0x100 ** size) - 1
            value = cls._set_by_value(random.randint(0, max_by_size), size)
        return value

    @classmethod
    def get_random_fixed(cls, size):
        """
        Get a random Avro bytes type with a specific size.
        """
        max_by_size = (0x100 ** size) - 1
        value = cls._set_by_value(random.randint(0, max_by_size), size)
        return value

    @classmethod
    def get_random_short(cls):
        """
        Get a random CDM SHORT type.
        """
        s = cls.get_random_fixed(cls._SHORT_LENGTH)
        return s

    @classmethod
    def get_random_uuid(cls):
        """
        Get a random CDM UUID type.
        """
        u = cls.get_random_fixed(cls._UUID_LENGTH)
        return u

    @classmethod
    def get_random_byte(cls):
        """
        Get a random CDM BYTE type.
        """
        b = cls.get_random_fixed(cls._BYTE_LENGTH)
        return b

    @classmethod
    def get_uuid_from_value(cls, val):
        """
        Take a numeric value and convert it to a CDM UUID.
        """
        if val.bit_length() > cls._UUID_LENGTH * 8:
            raise ValueError("Cannot make UUID from %d: too large." % val)
        u = cls._set_by_value(val, cls._UUID_LENGTH)
        return u

    @classmethod
    def get_short_from_value(cls, val):
        """
        Take a numeric value and convert it to a CDM SHORT.
        """
        if val.bit_length() > cls._SHORT_LENGTH * 8:
            raise ValueError("Cannot make SHORT from %d: too large." % val)
        s = cls._set_by_value(val, cls._SHORT_LENGTH)
        return s

    @classmethod
    def get_byte_from_value(cls, val):
        """
        Take a numeric value and convert it to a CDM BYTE.
        """
        if val.bit_length() > cls._BYTE_LENGTH * 8:
            raise ValueError("Cannot make BYTE from %d: too large." % val)
        b = cls._set_by_value(val, cls._BYTE_LENGTH)
        return b
