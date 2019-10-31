from .dimanager import DIManager
from prettytable import PrettyTable
import logging
import inspect
import coloredlogs
from .utility_objects import OperatorObject, PortObject, ConnectionObject
from sapdi.pipeline.port import PortKind, ContentType

coloredlogs.install()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s\\n %(levelname)s %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger('di_logger')
DIManager.di_connect()


def set_log_level(level):
    log.level = level


def add_code_to_operator(di_mode: bool = False, pipeline_id: str = None, operator_name: str = None, ports: dict = {}):
    def wrapper(fn):
        def inner_wrapper(*args, **kwargs):
            if di_mode:
                instance = DIManager.get_instance()
                instance.set_di_mode_on()
                if pipeline_id is not None:
                    instance.set_pipeline(pipeline_id)
                else:
                    pipeline = instance.get_pipeline()
                    log.debug("Pipeline : '{}'".format(pipeline.id))

                assert operator_name is not None, "Invalid or Empty operator-name"

                code = inspect.getsource(fn).split('\n', 1)[1]
                fn_name = fn.__name__

                code = instance.add_code_to_operator(op=operator_name, fn_name=fn_name, code=code, ports=ports)
                instance.save_graph()
                log.info("Code Added to Operator '{}'".format(operator_name))
                return code
            else:
                return fn(*args, **kwargs)

        return inner_wrapper
    return wrapper


def add_connections_to_port(src_opr_name: str, src_port_name: str, tgt_opr_name: str, tgt_port_name: str, pipeline_id: str = None):
    conn_added = False
    instance = DIManager.get_instance()
    instance.set_di_mode_on()
    if pipeline_id is not None:
        instance.set_pipeline(pipeline_id)
    else:
        pipeline = instance.get_pipeline()
        log.debug("Pipeline : '{}'".format(pipeline.id))

    instance.add_connections_to_port(src_opr_name=src_opr_name, src_port_name=src_port_name, tgt_opr_name=tgt_opr_name, tgt_port_name=tgt_port_name)
    conn_added = True
    log.info("Connections Added Successfully!!")
    return conn_added


def add_port_to_operator(opr_name: str,  port: PortObject,  target:tuple=(), pipeline_id: str=None):
    port_added = False
    instance = DIManager.get_instance()
    instance.set_di_mode_on()
    if pipeline_id is not None:
        instance.set_pipeline(pipeline_id)
    else:
        pipeline = instance.get_pipeline()
        log.debug("Pipeline : '{}'".format(pipeline.id))

    instance.add_port_to_operator(opr=opr_name, port=port, target=target)
    log.info("Port '{}' created successfully".format(port.name.lower()))
    port_added = True
    return port_added


def create_operator(opr: OperatorObject):
    instance = DIManager.get_instance()
    instance.set_di_mode_on()
    log.debug(opr.ports)
    log.debug(opr.connections)
    log.debug(opr.name)
    op = instance.create_operator(operator=opr)
    log.info("Operator '{}' created successfully !!".format(op.instance_name))
    return op


def create_pipeline(name:str, desc:str=None, template=None):
    instance = DIManager.get_instance()
    instance.set_di_mode_on()
    log.debug("Pipeline Given Name: {}".format(name))
    log.debug("Pipeline Template: {}".format(template))
    pipeline = instance.create_pipeline(name=name, desc=desc, template=template)
    log.info("Pipeline '{}' has been created successfully".format(pipeline.id))
    return pipeline


def get_pipelines():
    pipelines = DIManager.list_pipelines()
    outtab = PrettyTable()
    outtab.field_names = ['Id', 'Name']
    for p in pipelines:
        outtab.add_row([p.id, p.name])

    log.info(outtab)
    return pipelines










