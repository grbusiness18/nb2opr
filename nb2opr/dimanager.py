import sapdi as di
from sapdi.internal.di_client import DIClient
from dotenv import load_dotenv, find_dotenv
import os
from jq import jq
import functools


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
        DIManager._instance = self

    @staticmethod
    def di_connect():
        load_dotenv(find_dotenv())
        di.connect(url=os.environ['DI_CLUSTER_URL'],
                   tenant=os.environ['DI_TENANT'],
                   username=os.environ['DI_USERNAME'],
                   password=os.environ['DI_PASSWORD'])

        # print(di.list_scenarios())
        s = di.scenario.Scenario.get(scenario_id='60961093-af52-4b3a-be55-1acbc04d9f56');
        di.set_current_scenario(s)
        print("Current Scenario : ", s.name)

    def validate(method_names: list = None, **mkwargs):
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

    def get_pipeline(self):
        if self.__pipeline_id is None:
            raise Exception("Invalid Pipeline : '{}'".format(self.__pipeline_id))

        return di.get_pipeline(pipeline_id=self.__pipeline_id)

    def set_pipeline(self, value):
        try:
            pipeline = di.get_pipeline(pipeline_id=value)
            self.__pipeline_id = value
        except:
            raise Exception("Invalid Pipeline : '{}'".format(value))

    def create_pipeline(self, name: str = None, desc: str = None, template=None):
        pipeline_name = self.generate_pipeline_name(name)
        pipeline_desc = desc
        if not pipeline_desc:
            pipeline_desc = 'Generated Pipeline : "{}"'.format(pipeline_name)

        pipeline = di.create_pipeline(name=pipeline_name, description=pipeline_desc, from_template=template)
        return pipeline

    def list_pipeline_templates(self):
        return di.list_pipeline_templates()

    def generate_pipeline_name(self, name: str = None):
        names = [p.name for p in di.list_pipeline()]
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
        graph = self.get_graph()
        oper = eval('di.pipeline.operators' + opr)()
        graph = di.pipeline.Graph(operators=[oper])
        self.save_graph()

    @validate(method_names=['__check_port_exists'], **{'opr': 'opr', 'portname': 'port', 'porttype': 'porttype'})
    @validate(method_names=['__check_port_type'], **{'porttype': 'porttype'})
    @validate(method_names=['__check_operator_exists'], **{'opr': 'op'})
    def add_port_to_operator(self, opr: str, porttype: str, portname: str, portkind: str, target: tuple = ()):
        graph = self.get_graph()
        if porttype.upper() == 'INPORT':
            graph.operators[opr].operatorinfo.add_inport(port={'name': portname, 'type': portkind})
        else:
            graph.operators[opr].operatorinfo.add_outport(port={'name': portname, 'type': portkind})

        print(target)
        if target:
            self.add_connections_to_port(src_opr_name=opr, src_port_name=portname, tgt_opr_name=target[0],
                                         tgt_port_name=target[1])
        else:
            self.save_graph()

    @validate(method_names=['__check_port_exists'],
              **{'tgt_opr_name': 'opr', 'tgt_port_name': 'port', '$porttype': 'INPORT'})
    @validate(method_names=['__check_operator_exists'], **{'tgt_opr_name': 'op'})
    @validate(method_names=['__check_port_exists'],
              **{'src_opr_name': 'opr', 'src_port_name': 'port', '$porttype': 'OUTPORT'})
    @validate(method_names=['__check_operator_exists'], **{'src_opr_name': 'op'})
    def add_connections_to_port(self, src_opr_name: str, src_port_name: str, tgt_opr_name: str, tgt_port_name: str):
        graph = self.get_graph()
        graph.add_connection(src_operator=graph.operators[src_opr_name], src_port_name=src_port_name,
                             tgt_operator=graph.operators[tgt_opr_name],
                             tgt_port_name=tgt_port_name)
        self.save_graph()

    @validate(method_names=['__check_operator_exists'], **{'op': 'op'})
    @validate(method_names=['__check_operator_scriptable'], **{'op': 'op'})
    def add_code_to_operator(self, op: str, fn_name: str, code: str, ports: dict = {}):
        src_code = None
        ## Outport add

        outports = self.get_ports_for_operator(op, porttype='OUT')
        outport_template = 'api.send("{}",&outportval)'
        outport_message = 'api.Message(&outportval)'

        for p in outports:
            outport_values = None
            outport_values = outport_template.format(p['src']['port'])
            if p['src']['port'] in ports.keys():
                # when just string is passed.
                if isinstance(ports[p['src']['port']], str):
                    outport_values = outport_values.replace("&outportval", ports[p['src']['port']])

                # when a dict is passed.
                if isinstance(ports[p['src']['port']], dict):
                    port_dict = ports[p['src']['port']]
                    print(port_dict)
                    if 'message' in port_dict:
                        if port_dict['message']:
                            outport_values = outport_values.replace("&outportval", outport_message)

                    if 'value' in port_dict:
                        if 'is_string' in port_dict:
                            if port_dict['is_string']:
                                outport_values = outport_values.replace("&outportval", '"{}"')
                                outport_values = outport_values.format(port_dict['value'])
                            else:
                                outport_values = outport_values.replace("&outportval", port_dict['value'])
                                # check variable in code
                        else:
                            outport_values = outport_values.replace("&outportval", port_dict['value'])
                            # check variable in code
                    else:
                        outport_values = outport_values.replace("&outportval", '""')

            else:
                # print("Before :", outport_values)
                outport_values = outport_values.replace("&outportval", '""')

                # print("After :", outport_values)

            if src_code:
                src_code = src_code + '\n    ' + outport_values
            else:
                src_code = '\n    ' + outport_values
                src_code = code + src_code

        # print("Outport Src code")
        # print(src_code)

        inports = self.get_ports_for_operator(op, porttype='IN')
        inport_template = 'api.set_port_callback(&inport_template, &fn_name)'
        inport_values = None
        params = None
        for p in inports:
            if params:
                params = params + ', ' + p['tgt']['port']
            else:
                params = p['tgt']['port']

            # print(p['tgt']['port'])
            if inport_values:
                inport_values = inport_values + ',' + '"{}"'.format(p['tgt']['port'])
            else:
                inport_values = '"{}"'.format(p['tgt']['port'])

        if len(inports) > 1:
            inport_values = '[ ' + inport_values + ' ]'

        # print("Inport temp", inport_template)
        # print("Inport values", inport_values)
        if inport_values is None:
            inport_values = ""

        inport_template = inport_template.replace("&inport_template", inport_values).replace("&fn_name", fn_name)

        if src_code:
            src_code = src_code + '\n\n' + inport_template
        else:
            src_code = code + '\n\n' + inport_template

        # print("Fn_name", fn_name, params)

        ## inject inports to function params.
        if params:
            src_code = 'def ' + fn_name + '(' + params + ', ' + src_code.split('def ' + fn_name + '(', 1)[1]

        # print("Inport Code")
        # print(src_code)

        self.get_graph().operators[op].config['script'] = code

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
        arguments = {}
        for k, v in src.items():
            if "$" in k:
                arguments[k.split("$")[1]] = v
            else:
                arguments[v] = tar[k]

        print("arguments", arguments)
        getattr(self, '_DIManager' + m)(**arguments)

    #### validate
    def __check_operator_exists(self, op: str):
        print("__check_operator_exists")
        graph = self.get_graph()
        if op not in graph.operators.keys():
            print("***** ------- available operators ----*****")
            print(graph.operators.keys())
            raise Exception("Invalid operator: '{}'".format(op))

    def __check_operator_scriptable(self, op: str):
        print("__check_operator_scriptable")
        graph = self.get_graph()
        if 'script' not in graph.operators[op].config:
            raise Exception("Operator '{}' doesn't have SCRIPT metadata".format(op))

    def __check_port_type(self, porttype: str):
        if porttype.upper() not in ['INPORT', 'OUTPORT']:
            print("Allowed Port types are : INPORT, OUTPORT")
            raise Exception("Invalid Port type '{}'".format(porttype))

    def __check_port_exists(self, opr: str, port: str, porttype: str):
        graph = self.get_graph()
        ports = []
        if porttype.upper() == 'INPORT':
            ports = graph.operators[opr].operatorinfo.inports
        else:
            ports = graph.operators[opr].operatorinfo.outports

        for p in ports:
            port_name = None
            if type(p) == di.internal.modeler.port_info.PortInfo:
                port_name = p.name
            else:
                port_name = p['name']

            if port_name.upper() == port:
                raise Exception("Port '{}' already exists".format(port))
            continue