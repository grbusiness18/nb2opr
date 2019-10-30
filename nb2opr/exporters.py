from .dimanager import DIManager
from prettytable import PrettyTable
import logging
import inspect
import coloredlogs

coloredlogs.install()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s\\n %(levelname)s %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger('di_logger')
DIManager.di_connect()


def add_code_to_existing_pipeline_operator(di_mode: bool = False, pipeline_id: str = None, operator_name: str = None, ports: dict = {}):
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


def add_port_to_operator(opr_name: str, porttype: str, portname: str, portkind:str, target:tuple=(), pipeline_id: str=None):
    port_added = False
    instance = DIManager.get_instance()
    instance.set_di_mode_on()
    if pipeline_id is not None:
        instance.set_pipeline(pipeline_id)
    else:
        pipeline = instance.get_pipeline()
        log.debug("Pipeline : '{}'".format(pipeline.id))

    instance.add_port_to_operator(opr=opr_name, porttype=porttype, portname=portname.lower(), portkind=portkind, target=target)
    log.info("Port '{}' created successfully".format(portname.lower()))
    port_added = True
    return port_added

def create_operator(opr: str, new_pipeline: bool=False, pipeline_id: str=None):
    instance = DIManager.get_instance()
    instance.set_di_mode_on()
    if new_pipeline:
        pipeline = instance.create_pipeline()
        print("New pipeline {} created.".format(pipeline.name))
        instance.set_pipeline(pipeline.id)
    else:
        assert pipeline_id is not None, "Invalid or Empty pipeline-id"
        instance.set_pipeline(pipeline_id)

    instance.create_operator()
    print("Operator '{}' has been created successfully".format(opr))


def create_pipeline(name:str, desc:str=None, template=None):
    instance = DIManager.get_instance()
    instance.set_di_mode_on()
    log.debug("Pipeline Given Name: {}".format(name))
    log.debug("Pipeline Template: {}".format(template))
    pipeline = instance.create_pipeline(name=name, desc=desc, template=template)
    log.info("Pipeline '{}' has been created successfully".format(pipeline.id))
    return pipeline


def get_pipelines():
    instance = DIManager.get_instance()
    instance.set_di_mode_on()
    pipelines = instance.list_pipelines()
    outtab = PrettyTable()
    outtab.field_names = ['Id', 'Name']
    for p in pipelines:
        outtab.add_row([p.id, p.name])

    log.info(outtab)
    return pipelines










