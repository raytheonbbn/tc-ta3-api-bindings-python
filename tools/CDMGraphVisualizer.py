#!/usr/bin/python

# Copyright (c) 2020 Raytheon BBN Technologies Corp.
# See LICENSE.txt for details.

"""
Loads serialized records from a avro binary input file and creates a DOT graph

Pre-requisite: install python graphviz

To run: python CDMGraphVisualizer.py \
        -psf ../../ta3-serialization-schema/avro/TCCDMDatum.avsc \
        -f testdata/CStest.avro -s
        
        # The -s will render and save the dot graph to pdf, default False no graph will be generated
        # The -e <engine> specifies the layout engine, default dot (e.g, circo, sfdp)
        # The -c is a shortcut for clean graph, def=False (nodes not filled, no overlap, splines=true, etc.)
        # The -G <k1>=<v1> <k2>=<v2> allows specifying graph properties (can specify multiple)
        # The -N <k1>=<v1> <k2>=<v2> allows specifying node properties (can specify multiple)
        # The -E <k1>=<v1> <k2>=<v2> allows specifying edge properties (can specify multiple)
        
        # Example:
        python CDMGraphVisualizer.py -psf ../../ta3-serialization-schema/avro/TCCDMDatum.avsc \
               -f testdata/CSgather.avro -e circo -G overlap=False size=20 -s

author: jkhoury@bbn.com
"""

import os
import sys
import logging
import argparse
import time
import datetime
import math
from logging.config import fileConfig

from tc.services import kafka
from tc.schema.serialization import Utils
from tc.schema.serialization.kafka import KafkaAvroGenericDeserializer
from tc.schema.records.parsing import CDMParser

from graphviz import Digraph


# Default values, replace or use command line arguments
FILENAME = "avroFile.avro"
SCHEMA = "../../ta3-serialization-schema/avro/TCCDMDatum.avsc"
STR_WIDTH=30
BOUND=31536000 # 1 year in seconds
NodeEdgeShapesColors ={
            "Subject": ["house", "lightsteelblue3"],
            "Event": ["box", "lightsteelblue1"],
            "NetFlowObject": ["ellipse", "khaki"],
            "FileObject": ["ellipse", "khaki1"],
            "SrcSinkObject": ["ellipse", "khaki2"],
            "MemoryObject": ["ellipse", "khaki3"],
            "RegistryKeyObject": ["ellipse", "khaki4"],
            "Principal": ["octagon", "rosybrown1"],
            "TagEntity": ["doublecircle", "yellow1"],
            "SimpleEdge": ["solid", "black"],
            "EDGE_EVENT_ISGENERATEDBY_SUBJECT" : ["solid", "lightsteelblue3"],
            }

def get_arg_parser():
    parser = argparse.ArgumentParser(description="Graph visualizer")
    parser.add_argument("-psf", action="store", type=str,
            default=SCHEMA,
            help="Set the producer's schema file.")
    parser.add_argument("-v", action="store_true", default=False,
            help="Turn up verbosity.")
    parser.add_argument("-s", action="store_true", default=False,
                            help="Save the rendered graph.")
    parser.add_argument("-c", action="store_true", default=False,
                            help="Keep nodes clean i.e. just show label")
    parser.add_argument("-G", action="append", type=str, nargs="*",
                    help="Graph attributes specified as k=v strings")
    parser.add_argument("-N", action="append", type=str, nargs="*",
                    help="Node attributes specified as k=v strings")
    parser.add_argument("-E", action="append", type=str, nargs="*",
                            help="Edge attributes specified as k=v strings")
    parser.add_argument("-f", action="store", type=str, required=True,
                        help="Filename to deserialize from")
    parser.add_argument("-e", action="store", type=str, default="dot",
                    help="Filename to deserialize from")
    return parser

def getTupleAsString(field):
    key=str(field[0])+":"
    if type(field[1]) == dict:
        return key+'{'+'  \n'.join([strTrim(str(x[0])+"::"+str(x[1])) for x in field[1].items()
                                    if x[1] != None])+'}'
    return key+strTrim(str(field[1]))

def getRecordAttributeDesciption(rtype, record):
    return '\n'.join([getTupleAsString(x) for x in record.items()
                      if x[0] not in ['uuid', 'parameters', 'source', 'baseObject']
                      and x[1] != None])

def strTrim(s):
    if len(s) <= STR_WIDTH: return s
    return s[:STR_WIDTH/2] + '...' + s[len(s)-STR_WIDTH/2-1:]

def setProperties(mylist, mydict):
    if not mylist == None:
        for arglist in mylist:
            for arg in arglist:
                k,v=arg.split("=")
                mydict[k]=v

# check if the timestamp is valid assuming ts in micros
# compare it to now and make sure it is within a bound
def isValidTimestamp(ts):
    timestamp = time.time()
    ts_sec = ts / 1000000.0
    if math.fabs(timestamp - ts_sec) > BOUND:
        return False
    return True

def tsToDate(ts):
    ts_sec = ts / 1000000.0
    return datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

def main():
    numnodes = 0
    numedges = 0
    numtags = 0
    minTS = sys.maxint
    maxTS = 0
    parser = get_arg_parser()
    args = parser.parse_args()
    fileConfig("logging.conf")
    if args.v:
        logging.getLogger("tc").setLevel(logging.DEBUG)
   
    logger = logging.getLogger("tc")
    
    # Load the avro schema
    p_schema = Utils.load_schema(args.psf)

    # The cdm parser
    cdmparser = CDMParser(p_schema)

    # Initialize an avro serializer
    rfile = open(args.f, 'rb')
    deserializer = KafkaAvroGenericDeserializer(p_schema, input_file=rfile)

    fname = os.path.basename(rfile.name)

    if args.s:
        # Create the graph
        dot = Digraph(name=fname, comment="CDM DOT digraph", engine=args.e)
        # default attributes we add
        dot.graph_attr['rankdir'] = 'RL'
        dot.node_attr['fontname'] = "Helvetica"
        dot.node_attr['fontsize'] = "7"
        dot.node_attr['margin'] = "0.0,0.0"
        dot.edge_attr['fontname'] = "Helvetica"
        dot.edge_attr['fontsize'] = "6"
        dot.node_attr['style'] = "filled"
        if args.c:
            dot.graph_attr['overlap'] = "False"
            dot.graph_attr['splines'] = "True"
            #dot.node_attr['style'] = ""
        # attributes specified by the user
        setProperties(args.G, dot.graph_attr)
        setProperties(args.N, dot.node_attr)
        setProperties(args.E, dot.edge_attr)
        # some debugging
        logger.debug(dot.graph_attr)
        logger.debug(dot.node_attr)
        logger.debug(dot.edge_attr)

    records = deserializer.deserialize_from_file()

    i = 0
    for edge in records:
        i = i + 1
        rtype = cdmparser.get_record_type(edge)
        
        logger.debug("parsed record of type " + rtype)
        logger.debug(edge)
        datum = edge["datum"]
        
        if rtype == 'SimpleEdge':
            fuuid = repr(datum["fromUuid"])
            tuuid = repr(datum["toUuid"])
            if args.s:
                dot.edge(fuuid, tuuid, constraint='false',
                     style=(datum['type'] in NodeEdgeShapesColors and NodeEdgeShapesColors[datum['type']][0]
                            or NodeEdgeShapesColors[rtype][0]),
                     color=(datum['type'] in NodeEdgeShapesColors and NodeEdgeShapesColors[datum['type']][1]
                            or NodeEdgeShapesColors[rtype][1]))
            numedges += 1
        
        elif rtype == 'ProvenanceTagNode' or rtype == 'TagEntity':
            numtags += 1
            continue

        else: # a node
            uuid = repr(datum["uuid"])
            descr = ('\n' + getRecordAttributeDesciption(rtype, datum)) if not args.c else ''
            shape = NodeEdgeShapesColors[rtype][0]
            color = NodeEdgeShapesColors[rtype][1]
            if args.s:
                dot.node(uuid, label=rtype+":"+uuid[len(uuid)-6:]+descr, shape=shape, color=color)
            if rtype == 'Event' and 'timestampMicros' in datum:
                ts = datum['timestampMicros']
                if not isValidTimestamp(ts):
                    print('Warning: invalid timestamp '+str(ts))
                else:
                    if ts > 0 and ts < minTS: minTS = ts
                    elif ts > maxTS: maxTS = ts
            numnodes += 1

    rfile.close()
    logger.info(minTS)
    logger.info(maxTS)
    tSpanMicros = 1.0*(maxTS-minTS)
    tSpanSec =  tSpanMicros / 1e6
    logger.info("Read "+str(i)+" records {" + str(numnodes) + " nodes, " + str(numedges) + " edges, "
                + str(numtags) + " tags}")
    if tSpanSec > 0:
        logger.info("Event duration: " + str(tSpanMicros) + " micros, " + str(tSpanSec) + " sec "
                    + (str(tSpanSec/3600.0)+" hrs" if tSpanSec > 3600 else ""))
    else:
        logger.info("Event timestamps are not available, can't determine run duration")
    # render the graph
    if args.s: dot.render(args.f+'.'+args.e+'.gv', view=True)

main()
