# Copyright (c) 2020 Raytheon BBN Technologies Corp.
# See LICENSE.txt for details.

from six import string_types

"""
Module providing interfaces for working with record parsing.
"""


class CDMParser(object):
    """
    Class providing some helpers for CDM record parsing.
    """

    def __init__(self, reader_schema):
        """Create a CDMParser.

        :param reader_schema: Reader schema object.
        """
        self.reader_schema = reader_schema

        # Dictionary defining the mapping of type enum to type short name
        self._type_checks = {
            "ProvenanceTagNode": "RECORD_PROVENANCE_TAG_NODE",
            "Subject": "RECORD_SUBJECT",
            "Event": "RECORD_EVENT",
            "NetFlowObject": "RECORD_NET_FLOW_OBJECT",
            "SrcSinkObject": "RECORD_SRC_SINK_OBJECT",
            "FileObject": "RECORD_FILE_OBJECT",
            "MemoryObject": "RECORD_MEMORY_OBJECT",
            "Principal": "RECORD_PRINCIPAL",
            "IpcObject": "RECORD_IPC_OBJECT",
            "RegistryKeyObject": "RECORD_REGISTRY_KEY_OBJECT",
            "TimeMarker": "RECORD_TIME_MARKER",
            "UnitDependency": "RECORD_UNIT_DEPENDENCY",
            "Host": "RECORD_HOST",
            "PacketSocketObject": "RECORD_PACKET_SOCKET_OBJECT",
            "EndMarker": "RECORD_END_MARKER",
            "UnknownProvenanceNode": "RECORD_UNKNOWN_PROVENANCE_NODE"
        }

    def get_union_branch_type(self, data):
        """ Returns the string format of the union's inner type.

        This function is a wrapper around the other existing union type getter
        methods.  It tries to determine which union is being passed in, and
        from that it tries to call the right function to get the string
        representation of the union's currently selected type.  This method can
        be called in place of the other union-specific methods which get types.

        :param data: Currently, this can either be a full record or a
                     provenance tag node.
        :returns: The string representation of the data's inner type if it is a
                  supported union.
        :raises ValueError: If we don't support getting the inner type of the
                            provided data.
        """

        # If we have a datum attribute, then we are looking for record type.
        if "datum" in data:
            return self.get_record_type(data)

        # We don't support any other union types at this time.
        else:
            raise ValueError("Unsupported data structure provided.")

    # Make this a method in case we evolve towards using the reader schema.
    def get_record_type(self, record):
        """ Returns the string format of the record type's short name.

        :param record: The record whose type we are trying to identify.
        :returns: String representation of the record type's short name.
        :raises ValueError: If the type cannot be identified.
        """

        # Get a handle for the datum sub-record.
        for type_ in self._type_checks.keys():
            if record["type"] == self._type_checks[type_]:
                return type_

        # If we reach this point, then no matching type was found.  Either this
        # code needs to be updated to match a new definition of the CDM, or
        # else the provided record does not adhere to the currently supported
        # CDM version.
        raise ValueError("Provided record type not supported by CDM Parser.")
