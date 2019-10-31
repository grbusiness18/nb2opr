import sapdi as di
from sapdi.internal.di_client import DIClient
from dotenv import load_dotenv, find_dotenv
import os
from jq import jq
import functools


class DIObjectHandler:
    _instance = None

    @staticmethod
    def get_instance():
        if DIObjectHandler._instance is None:
            DIObjectHandler()
        return DIObjectHandler._instance

    def __init__(self):
        self.__model_manager = None
        self.__graph = None
        self.__pipeline_id = None
        self.__di_mode = False
        self.__operators_code = dict()
        self.__operators_port = dict()
        self.__graph_connections = []
        DIObjectHandler._instance = self
        DIObjectHandler.di_connect()
        # self.pipeline_id = "04dd8d8f-e495-410a-aac1-582fc3286537"

    @property
    def pipeline_id(self):
        return self.__pipeline_id

    @pipeline_id.setter
    def pipeline_id(self, value):
        self.__pipeline_id = value
        if value:
            self.__set_model_manager()
            self.__set_graph()

    @property
    def graph(self):
        return self.__graph

    @graph.setter
    def graph(self, value):
        self.__graph = value

    @property
    def graph_connections(self):
        return self.__graph_connections

    @graph_connections.setter
    def graph_connections(self, value):
        self.__graph_connections = value

    @property
    def di_mode(self):
        return self.__di_mode

    @di_mode.setter
    def di_mode(self, value):
        self.__di_mode = value

    @property
    def model_manager(self):
        return self.__model_manager

    @model_manager.setter
    def model_manager(self, value):
        self.__model_manager = value

    def reset_instance_values(self):
        self.model_manager = None
        self.graph = None
        self.pipeline_id = None
        self.di_mode = False
        self.graph_connections = []
        self.__operators_code = dict()
        self.__operators_port = dict()

    @staticmethod
    def di_connect():
        load_dotenv(find_dotenv())
        di.connect(url=os.environ['DI_CLUSTER_URL'],
                   tenant=os.environ['DI_TENANT'],
                   username=os.environ['DI_USERNAME'],
                   password=os.environ['DI_PASSWORD'])

    def __set_model_manager(self):
        self.__model_manager = DIClient.getInstance().getModelerManager()

    def __set_graph(self):
        try:
            print("com.sap.dsp." + self.pipeline_id)
            self.graph = self.model_manager.find_graph("com.sap.dsp." + self.pipeline_id)
            self.graph_connections = self.graph.to_json()['connections']
        except:
            self.pipeline_id = None
            raise "Invalid Pipeline ID : {}".format(self.pipeline_id)

    def validate_di_mode(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            ret = f(self, *args, **kwargs)
            self.__check_di_mode_is_on()
            return ret

        return wrapper

    def validate_di_graph(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            ret = f(self, *args, **kwargs)
            self.__check_graph()
            return ret

        return wrapper

    def validate_di_operators(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            ret = f(self, *args, **kwargs)
            operator = None

            if args:
                operator = args[0]
            if kwargs:
                operator = kwargs['op']

            self.__check_operator_exists(operator)
            self.__check_operator_scriptable(operator)
            return ret

        return wrapper

    def validate_di_pipeline(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            ret = f(self, *args, **kwargs)
            self.__check_pipeline()
            return ret

        return wrapper

    ## public
    @validate_di_mode
    @validate_di_pipeline
    @validate_di_graph
    @validate_di_operators
    def add_code_to_operator(self, op: str, code: str):
        # self.__check_di_mode_is_on()
        # self.__check_operator_exists(op)
        # self.__check_operator_scriptable(op)

        if op not in self.__operators_code:
            self.__operators_code[op] = []

        self.__operators_code[op].append(code)

    @validate_di_mode
    @validate_di_pipeline
    @validate_di_graph
    @validate_di_operators
    def add_out_port_val_to_operator(self, op: str, port: str, val: str, outtype: str = "DEFAULT",
                                     port_context: str = "IN"):
        # self.__check_di_mode_is_on()
        # self.__check_operator_exists(op)
        # self.__check_operator_scriptable(op)

        if op not in self.__operators_port:
            self.__operators_port[op] = {}

        if port not in self.__operators_port[op]:
            self.__operators_port[op][port] = {}

        self.__operators_port[op][port] = {
            "value": val,
            "outtype": outtype.upper(),
            "port_context": port_context.upper()
        }

        if not outtype.upper() == 'DEFAULT':
            self.__operators_port[op][port]['outtype'] = 'MESSAGE'
        else:
            self.__operators_port[op][port]['outtype'] = 'DEFAULT'

        if not port_context.upper() == 'IN':
            self.__operators_port[op][port]['port_context'] = 'OUT'
        else:
            self.__operators_port[op][port]['port_context'] = 'IN'

        print(self.__operators_port[op])

    def set_di_mode_on(self):
        self.di_mode = True

    def set_di_mode_off(self):
        self.di_mode = False

    @validate_di_mode
    @validate_di_pipeline
    @validate_di_graph
    def save_graph_to_di(self):
        self.__prepare_save()
        self.graph.check()
        self.graph.save()
        # pipeline = di.create_pipeline("GRPipeline_v5", description="Some Test description")
        # pipeline.graph = self.graph
        # version = di.create_version("1.0")
        # conf = di.scenario.Configuration.create("GR_test", [], pipeline, version)
        # handle = pipeline.execute(conf).id

    @validate_di_mode
    @validate_di_pipeline
    @validate_di_graph
    def get_operators(self):
        return self.graph.operators.keys()

    @validate_di_mode
    @validate_di_pipeline
    @validate_di_graph
    def preview(self):
        self.__prepare_save(preview=True)

    ## private methods

    def __prepare_save(self, preview=False):
        for op in self.__operators_code:
            # replace the ports to ENUM
            inports = self.__get_connections_for_operator(op, 'INPORT')
            outports = self.__get_connections_for_operator(op, 'OUTPORT')
            make_fn = False
            if inports:
                make_fn = True

            code = self.__prepare_code(op, make_fn)
            if outports:
                code_in, code_out = self.__prepare_outport_code(op, outports)

                if code_in:
                    code = code + '\n' + code_in

                if code_out:
                    code = code + '\n\n' + code_out

            if inports:
                code = self.__prepare_inports_code(op, inports, code)

            # add code to temp

            if not preview:
                self.graph.operators[op].config['script'] = code

    def __prepare_code(self, op, make_fn=False):
        src_code = None
        fn_template = 'def on_input(&input_params):'
        indent = ""
        if make_fn:
            src_code = fn_template
            indent = "    "

        for cd in self.__operators_code[op]:
            if src_code:
                src_code = src_code + '\n' + indent + cd
            else:
                src_code = cd

        return src_code

    def __prepare_inports_code(self, op, inports, code):
        src_code = None
        inport_template = 'api.set_port_callback(&inport_template, on_input)'
        inport_params = None
        inport_values = None

        for p in inports:
            if inport_params:
                inport_params = inport_params + ', ' + '"{}"'.format(p['tgt']['port'])
            else:
                inport_params = '"{}"'.format(p['tgt']['port'])

            if inport_values:
                inport_values = inport_values + ', ' + p['tgt']['port']
            else:
                inport_values = p['tgt']['port']

        inport_params = '[ ' + inport_params + ' ]'
        inport_template = inport_template.replace("&inport_template", inport_params)
        src_code = code.replace("&input_params", inport_values)
        src_code = src_code + '\n\n' + inport_template
        return src_code

    def __prepare_outport_code(self, op, outports):
        src_code_with_gaps = None
        src_code_wo_gaps = None
        outport_template = 'api.send("{}",&outport_value)'
        outport_message = 'api.Message(&outport_value)'

        for p in outports:
            outport_value = '""'
            port = p['src']['port']
            outport_template

            if op not in self.__operators_port or port not in self.__operators_port[op]:
                if src_code_wo_gaps:
                    src_code_wo_gaps = src_code_wo_gaps + '\n' + outport_template.format(port).replace("&outport_value",
                                                                                                       outport_value)
                else:
                    src_code_wo_gaps = outport_template.format(port).replace("&outport_value", outport_value)

                continue

            if port in self.__operators_port[op]:
                # check value in src-code
                outport_value = self.__operators_port[op][port]['value']
                self.__check_var_in_src_code(op=op, var=outport_value)

            if self.__operators_port[op][port]['outtype'] == 'MESSAGE':
                outport_value = outport_message.replace("&outport_value", outport_value)

            if self.__operators_port[op][port]['port_context'] == 'OUT':
                if src_code_wo_gaps:
                    src_code_wo_gaps = src_code_wo_gaps + '\n' + outport_template.format(port).replace("&outport_value",
                                                                                                       outport_value)
                else:
                    src_code_wo_gaps = outport_template.format(port).replace("&outport_value", outport_value)

            else:
                if src_code_with_gaps:
                    src_code_with_gaps = src_code_with_gaps + '\n    ' + outport_template.format(port).replace(
                        "&outport_value", outport_value)
                else:
                    src_code_with_gaps = '    ' + outport_template.format(port).replace("&outport_value", outport_value)

        return src_code_with_gaps, src_code_wo_gaps

    def __check_var_in_src_code(self, op: str, var: str):
        if op not in self.__operators_code:
            raise Exception("Operator '{}' doesn't have any Source code added.".format(op))

        src_code = self.__operators_code[op]
        found_var = False
        for cd in src_code:
            if var in cd:
                found_var = True
                break

        if not found_var:
            print(self.__operators_code[op])
            raise Exception("Variable '{}' isn't declared or added to source code".format(var))

    def __check_pipeline(self):
        if self.pipeline_id is None:
            print("Please use following command to set pipeline.\n%set_di_pipeline <pipeline-id>")
            raise Exception("Invalid DI Pipeline.")

    def __check_graph(self):
        if not isinstance(self.graph, di.internal.modeler.graph.Graph):
            raise Exception("Invalid Graph instance {}".format(type(self.graph)))

    def __check_operator_scriptable(self, op):
        if 'script' not in self.graph.operators[op].config:
            raise Exception("Operator '{}' doesn't have SCRIPT metadata".format(op))

    def __check_di_mode_is_on(self):
        if not self.di_mode:
            raise Exception("DI Mode is not enabled")

    def __check_operator_exists(self, op):
        if op not in self.graph.operators.keys():
            raise Exception("Operator Invalid: Pipeline doesn't have Operator {}".format(op))

    def __check_port_exists_for_operator(self, op: str, typ: str, port: str):
        if self.__get_connections_for_operator(op, typ, port):
            return True
        else:
            return False

    def __get_connections_for_operator(self, op: str, typ: str, port=None):
        port_type = '.tgt'
        if typ.upper() == 'OUTPORT':
            port_type = '.src'

        if port is None:
            trns = '.[] | select(&1.process == "&2")'.replace("&1", port_type).replace("&2", op)
        else:
            trns = '.[] | select((&1.process == "&2") and (&1.port == "&3"))'.replace("&1", port_type).replace("&2",
                                                                                                               op).replace(
                "&3", port)

        # print(self.graph_connections)

        try:
            cops = jq(trns).transform(self.graph_connections, multiple_output=True)
            return cops
        except StopIteration:
            return None