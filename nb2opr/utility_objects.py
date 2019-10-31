#from sapdi.pipeline.port import PortKind, ContentType
from typing import Mapping, MutableMapping, Sequence, Iterable, List, Set
from enum import Enum


class PortKind(Enum):
    INPUT = "input"
    OUTPUT = "output"


class ContentType(Enum):
    MESSAGE = "message"
    ARTIFACT = "message.artifact"
    STRING = "string"
    ANY = "any.*"
    BLOB = "blob"
    INT64 = "int64"
    UINT64 = "uint64"
    FLOAT64 = "float64"
    BYTE = "byte"
    STREAM = "stream"
    UNKNOWN = "unknown"


class OperatorObject:
    def __init__(self, name: str, desc: str=None, instance_name: str=None, ports: List=[], connections: List=[]):
        self.name = name
        self.desc = desc
        self.instance_name = instance_name
        self.ports : List[PortObject] = ports
        self.connections: List[ConnectionObject] = connections


class PortObject:
    def __init__(self, name: str, kind: PortKind, content_type: ContentType):
        self.name = name
        if not isinstance(kind, PortKind):
            raise Exception("Invalid Argument Type for PortKind")

        if not isinstance(content_type, ContentType):
            raise Exception("Invalid Argument Type for ContentType")

        self.kind = kind
        self.content_type = content_type


class ConnectionObject:
    def __init__(self, src_opr_name: str, src_port_name: str, tgt_opr_name: str, tgt_port_name: str):
        self.src_opr_inst_name = src_opr_name
        self.src_port_name = src_port_name
        self.tgt_opr_inst_name = tgt_opr_name
        self.tgt_port_name = tgt_port_name


