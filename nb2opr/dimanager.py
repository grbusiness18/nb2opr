import sapdi as di
import logging
import os
from sapdi.internal.di_client import DIClient
from .di_code_generator import DIOperatorCodeGen
from dotenv import load_dotenv, find_dotenv
from jq import jq

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
        self.__port_types = ['INPORT', 'OUTPORT']
        self.__ignore_validation = False
        DIManager._instance = self

    @property
    def ignore_validation(self):
        return self.__ignore_validation

    @ignore_validation.setter
    def ignore_validation(self, value: bool):
        self.__ignore_validation = value

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

        if 'DI_PIPELINE_ID' in os.environ:
            instance = DIManager.get_instance()
            instance.set_pipeline(os.environ['DI_PIPELINE_ID'])
            log.warning("Current Pipeline : {}".format(instance.get_pipeline(pipeline_id=os.environ['PIPELINE_ID']).name))

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

    def list_pipelines(self):
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

    def create_operator(self, opr: list = [], inports: list = [], outports: list = [], connections: list = []):
        #graph = self.get_graph()
        #oper = eval('di.pipeline.operators' + opr)()
        #graph = di.pipeline.Graph(operators=[oper])
        #self.save_graph()
        pass

    @validate(method_names=['__check_port_exists'], **{'opr': 'opr', 'portname': 'port', 'porttype': 'porttype'})
    @validate(method_names=['__check_port_type'], **{'porttype': 'porttype'})
    @validate(method_names=['__check_operator_exists'], **{'opr': 'op'})
    def add_port_to_operator(self, opr: str, porttype: str, portname: str, portkind: str, target: tuple = ()):

        log.debug("Operator Name: {}".format(opr))
        log.debug("Port Name: {}".format(portname))
        log.debug("Port Type: {}".format(porttype))
        log.debug("Port Kind:{}".format(portkind))
        log.debug("Target Operator and Port: {}".format(target))

        graph = self.get_graph()

        if porttype.upper() == 'INPORT':
            graph.operators[opr].operatorinfo.add_inport(port={'name': portname, 'type': portkind})
        else:
            graph.operators[opr].operatorinfo.add_outport(port={'name': portname, 'type': portkind})

        if target:
            self.set_validation_off()
            self.add_connections_to_port(src_opr_name=opr, src_port_name=portname, tgt_opr_name=target[0],
                                         tgt_port_name=target[1])
            self.set_validation_on()
        else:
            self.save_graph()

    @validate(method_names=['__check_port_exists'],
              **{'tgt_opr_name': 'opr', 'tgt_port_name': 'port', '$porttype': 'INPORT'})
    @validate(method_names=['__check_operator_exists'], **{'tgt_opr_name': 'op'})
    @validate(method_names=['__check_port_exists'],
              **{'src_opr_name': 'opr', 'src_port_name': 'port', '$porttype': 'OUTPORT'})
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

    def execute_pipeline(self):
        pipeline = self.get_pipeline()
        test = di.scenario.Configuration.create(pipeline.name + '-conf', [], pipeline,
                                                scenario_version=di.create_version())
        return pipeline.execute(test).id

    def get_graph(self):
        if not self.__graph:
            __mm = DIClient.getInstance().getModelerManager()
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

    def __check_port_type(self, porttype: str):
        log.debug("Processing Method {}".format("__check_port_type"))
        if porttype.upper() not in self.__port_types:
            log.warning("Allowed Port types are :", self.__port_types)
            raise Exception("Invalid Port type '{}'".format(porttype))

    def __check_port_exists(self, opr: str, port: str, porttype: str):
        log.debug("Processing Method {}".format("__check_port_exists"))
        graph = self.get_graph()
        ports = []
        try:
            if porttype.upper() == 'INPORT':
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

            if port_name.upper() == port.upper():
                raise Exception("Port '{}' already exists".format(port))

    def __check_pipeline(self):
        log.debug("Processing Method {}".format("__check_pipeline"))
        if self.__pipeline_id is None:
            raise Exception("Invalid Pipeline : '{}'".format(self.__pipeline_id))
