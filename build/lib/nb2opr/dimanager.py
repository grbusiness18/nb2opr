import sapdi as di
import logging
import os
from sapdi.internal.di_client import DIClient
from .di_code_generator import DIOperatorCodeGen
from dotenv import load_dotenv, find_dotenv
from jq import jq
from sapdi.pipeline.connectableports import ConnectablePorts
from.utility_objects import OperatorObject, PortObject, PortKind, ContentType
import re

log = logging.getLogger('di_logger')
#logging.basicConfig(level=logging.DEBUG, format='%(asctime)s\\n %(levelname)s %(message)s', datefmt='%H:%M:%S')


def set_log_level(level):
    log.level = level




class DIManager:
    _instance = None

    @staticmethod
    def get_instance():
        if DIManager._instance is None:
            DIManager()
        return DIManager._instance

    def __init__(self):
        self.__model_manager = None
        self.__graph = None
        self.__pipeline_id = None
        self.__di_mode = False
        self.__operators_code = dict()
        self.__operators_port = dict()
        self.__graph_connections = []
       # self.__port_types = ['INPORT', 'OUTPORT']
        self.__ignore_validation = False
        self.__operators_names = []
        DIManager._instance = self

    @property
    def ignore_validation(self):
        return self.__ignore_validation

    @ignore_validation.setter
    def ignore_validation(self, value: bool):
        self.__ignore_validation = value

    @property
    def operator_names(self):
        return self.__operators_names

    @operator_names.setter
    def operator_names(self, value: list):
        self.__operators_names = value

    @property
    def model_manager(self):
        return self.__model_manager

    @model_manager.setter
    def model_manager(self, value):
        self.__model_manager = value

    @staticmethod
    def di_connect():
        load_dotenv(find_dotenv())
        di.connect(url=os.environ['DI_CLUSTER_URL'],
                   tenant=os.environ['DI_TENANT'],
                   username=os.environ['DI_USERNAME'],
                   password=os.environ['DI_PASSWORD'])

        # print(di.list_scenarios())
        scenario = None
        if 'DI_SCENARIO_ID' in os.environ:
            scenario = di.scenario.Scenario.get(scenario_id=os.environ['DI_SCENARIO_ID']);
            di.set_current_scenario(scenario)
        else:
            scenario = di.get_current_scenario()

        log.warning("Current Scenario : {}".format(scenario.name))

        instance = DIManager.get_instance()

        if 'DI_PIPELINE_ID' in os.environ:
            instance.set_pipeline(os.environ['DI_PIPELINE_ID'])
            log.warning("Current Pipeline : {}".format(di.get_pipeline(pipeline_id=os.environ['DI_PIPELINE_ID']).name))

        instance.model_manager = DIClient.getInstance().getModelerManager()
        instance.operator_names = [x.name for x in instance.model_manager.repo.list_operators()]

    def validate(method_names: list=None, **mkwargs):
        def wrapper(fn):
            def inner_wrapper(self, *args, **kwargs):
                for m in method_names:
                    self.validate(m, src=mkwargs, tar=kwargs)
                ret = fn(self, *args, **kwargs)
                return ret

            return inner_wrapper
        return wrapper

    def set_di_mode_on(self):
        self.__di_mode = True

    def set_validation_off(self):
        self.ignore_validation = True

    def set_validation_on(self):
        self.ignore_validation = False

    @validate(method_names=['__check_pipeline'])
    def get_pipeline(self):
        return di.get_pipeline(pipeline_id=self.__pipeline_id)

    def set_pipeline(self, value):
        try:
            pipeline = di.get_pipeline(pipeline_id=value)
            self.__pipeline_id = value
        except:
            raise Exception("Invalid Pipeline : '{}'".format(value))

    def create_pipeline(self, name: str = None, desc: str = None, template=None):
        pipeline_name = DIManager.generate_pipeline_name(name)
        pipeline_desc = desc
        if not pipeline_desc:
            pipeline_desc = 'Generated Pipeline : "{}"'.format(pipeline_name)

        pipeline = di.create_pipeline(name=pipeline_name, description=pipeline_desc, from_template=template)
        self.set_pipeline(pipeline.id)
        return pipeline

    def set_graph(self, value):
        self.__graph = value

    #def list_pipeline_templates(self):
    #    return di.list_pipeline_templates()

    @staticmethod
    def list_pipelines():
        return di.list_pipelines()

    @staticmethod
    def generate_pipeline_name(name: str = None):
        names = [p.name for p in di.list_pipelines()]
        if name:
            if name in names:
                raise Exception("Pipeline Name {} already exists".format(name))
            else:
                return name
        else:
            gen_name = 'gen-pipeline-{}'
            for i in len(names):
                if gen_name.format(i + 1) not in names:
                    gen_name = gen_name.format(i + 1)
                    break

            return gen_name.format(i + 1)

    @validate(method_names=['__check_pipeline'])
    def create_operator(self, operator: OperatorObject):
        if not isinstance(operator, OperatorObject):
            raise Exception("Invalid Argument")

        mm = self.model_manager
        graph = self.get_graph()

        # validate name of operators
        rex = re.compile(r'.*'+ operator.name, re.IGNORECASE)
        if len(list(filter(rex.match, self.__operators_names))) == 0:
            raise Exception("Invalid Operator")

        opi = mm.find_operator_info(operator.name)

        # find instance name
        if operator.instance_name:
            opr_inst_name = operator.instance_name
        else:
            opr_inst_name = opi.component_name.rsplit('.', 1)[1]

        rex = re.compile(r'.*' + opr_inst_name, re.IGNORECASE)
        operator_count = len(list(filter(rex.match, list(graph.operators.keys()))))

        if operator_count == 0:
            opr_inst_name = opr_inst_name + "1"
        else:
            opr_inst_name = opr_inst_name + str(operator_count + 1)

        op = mm.create_operator(instance_name=opr_inst_name, operatorinfo=opi)
        graph.add_operator(op)

        for p in operator.ports:
            cp = ConnectablePorts(portKind=p.kind, operator=op)
            cp.create_port(name=p.name, contentType=p.content_type)

        for c in operator.connections:
            graph.add_connection(graph.operators[c.src_opr_inst_name], c.src_port_name, graph.operators[c.tgt_opr_inst_name], c.tgt_port_name)

        graph.save()
        return op

    @validate(method_names=['__check_port_exists'], **{'opr': 'opr', 'port': 'port'})
    @validate(method_names=['__check_operator_exists'], **{'opr': 'op'})
    def add_port_to_operator(self, opr: str, port: PortObject, target: tuple = ()):

        log.debug("Operator Name: {}".format(opr))
        log.debug("Port Name: {}".format(port.name))
        log.debug("Port Type: {}".format(port.content_type))
        log.debug("Port Kind:{}".format(port.kind))
        log.debug("Target Operator and Port: {}".format(target))

        graph = self.get_graph()

        #if port.kind == PortKind.INPORT:
            #graph.operators[opr].operatorinfo.add_inport(port={'name': port.name.lower(), 'type': port.kind})

        #else:
            #graph.operators[opr].operatorinfo.add_outport(port={'name': port.name.lower(), 'type': portkind})

        cp = ConnectablePorts(portKind=port.kind, operator=graph.operators[opr])
        cp.create_port(name=port.name.lower(), contentType=port.content_type)

        if target:
            self.set_validation_off()
            self.add_connections_to_port(src_opr_name=opr, src_port_name=port.name, tgt_opr_name=target[0],
                                         tgt_port_name=target[1])
            self.set_validation_on()
        else:
            self.save_graph()

    @validate(method_names=['__check_port_exists_by_name'],
              **{'tgt_opr_name': 'opr', 'tgt_port_name': 'port_name', '$kind': PortKind.INPUT })
    @validate(method_names=['__check_operator_exists'], **{'tgt_opr_name': 'op'})
    @validate(method_names=['__check_port_exists_by_name'],
              **{'src_opr_name': 'opr', 'src_port_name': 'port_name', '$kind': PortKind.OUTPUT})
    @validate(method_names=['__check_operator_exists'], **{'src_opr_name': 'op'})
    @validate(method_names=['__check_pipeline'])
    def add_connections_to_port(self, src_opr_name: str, src_port_name: str, tgt_opr_name: str, tgt_port_name: str):
        log.debug("Source: Operator Name {}, Port Name {}".format(src_opr_name, src_port_name))
        log.debug("Target: Operator Name {}, Port Name {}".format(tgt_opr_name, tgt_port_name))
        graph = self.get_graph()
        graph.add_connection(src_operator=graph.operators[src_opr_name], src_port_name=src_port_name,
                             tgt_operator=graph.operators[tgt_opr_name],
                             tgt_port_name=tgt_port_name)
        self.save_graph()

    @validate(method_names=['__check_operator_exists'], **{'op': 'op'})
    @validate(method_names=['__check_operator_scriptable'], **{'op': 'op'})
    @validate(method_names=['__check_pipeline'])
    def add_code_to_operator(self, op: str, fn_name: str, code: str, ports: dict = {}):
        log.debug("Source Code to Be Added is : {}".format(code))
        log.debug("Function Name to Be Added is : {}".format(fn_name))
        log.debug("Operator Name : {}".format(op))
        log.debug("Ports {}".format(ports))

        src_code = None
        outports = self.get_ports_for_operator(op, porttype='OUT')
        inports = self.get_ports_for_operator(op, porttype='IN')
        src_code = DIOperatorCodeGen(fn_name=fn_name, code=code, inports=inports, outports=outports, ports=ports).process()

        self.get_graph().operators[op].config['script'] = src_code

        return src_code

    def save_graph(self, ):
        # self.get_graph().check()
        self.get_graph().save()
        #self.get_graph(force_fetch=True)

    def execute_pipeline(self):
        pipeline = self.get_pipeline()
        test = di.scenario.Configuration.create(pipeline.name + '-conf', [], pipeline,
                                                scenario_version=di.create_version())
        return pipeline.execute(test).id

    def get_graph(self, force_fetch: bool=False):
        if not self.__graph or force_fetch:
            __mm = self.model_manager
            pipeline = self.get_pipeline()
            self.__graph = __mm.find_graph("{}".format("com.sap.dsp." + pipeline.id))

        return self.__graph

    def get_ports_for_operator(self, op: str, porttype: str = 'IN'):
        if porttype.upper() == 'IN':
            port_type = '.tgt'
        else:
            port_type = '.src'

        connections = self.get_graph().to_json()['connections']
        trns = '.[] | select(&1.process == "&2")'.replace("&1", port_type).replace("&2", op)
        try:
            cops = jq(trns).transform(connections, multiple_output=True)
            return cops
        except:
            return None

    def validate(self, m, src={}, tar={}):
        if self.ignore_validation is False:
            arguments = {}
            for k, v in src.items():
                if "$" in k:
                    arguments[k.split("$")[1]] = v
                else:
                    arguments[v] = tar[k]

            getattr(self, '_DIManager' + m)(**arguments)

    #### validate
    def __check_operator_exists(self, op: str):
        log.debug("Processing Method {}".format("__check_operator_exists"))
        graph = self.get_graph()
        if op not in graph.operators.keys():
            log.info("***** ------- available operators ----*****")
            log.info(graph.operators.keys())
            raise Exception("Invalid operator: '{}'".format(op))

    def __check_operator_scriptable(self, op: str):
        log.debug("Processing Method {}".format("__check_operator_scriptable"))
        graph = self.get_graph()
        if 'script' not in graph.operators[op].config:
            raise Exception("Operator '{}' doesn't have SCRIPT metadata".format(op))

   # def __check_port_type(self, port: PortObject):
    #    log.debug("Processing Method {}".format("__check_port_type"))
    #    if porttype.upper() not in self.__port_types:
    #        log.warning("Allowed Port types are :", self.__port_types)
    #        raise Exception("Invalid Port type '{}'".format(porttype))

    def __check_port_exists_by_name(self, opr: str, port_name: str, kind: PortKind):
        port = PortObject(name=port_name, kind=kind, content_type=ContentType.STRING)
        self.__check_port_exists(opr, port)

    def __check_port_exists(self, opr: str, port: PortObject):
        log.debug("Processing Method {}".format("__check_port_exists"))
        graph = self.get_graph()
        ports = []
        try:
            if port.kind == 'INPUT':
                ports = graph.operators[opr].operatorinfo.inports
            else:
                ports = graph.operators[opr].operatorinfo.outports
        except KeyError:
            raise Exception("Invalid Operator '{}' ".format(opr))

        for p in ports:
            port_name = None
            if type(p) == di.internal.modeler.port_info.PortInfo:
                port_name = p.name
            else:
                port_name = p['name']

            if port_name == port.name:
                raise Exception("Port '{}' already exists in {}".format(port.name, port.kind))

    def __check_pipeline(self):
        log.debug("Processing Method {}".format("__check_pipeline"))
        if self.__pipeline_id is None:
            raise Exception("Invalid Pipeline : '{}'".format(self.__pipeline_id))
