import logging

log = logging.getLogger('di_logger')

def set_log_level(level):
    log.level = level

class DIOperatorCodeGen(object):
    def __init__(self, fn_name: str, code: str, inports: list, outports: list, ports: dict = {}):
        self.__code = code
        self.__fn_name = fn_name
        self.__inports = inports
        self.__outports = outports
        self.__ports = ports
        self.OUTPORT_TEMPLATE = 'api.send("{}", &outportval)'
        self.OUTPORT_MSG_TEMPLATE = 'api.Message(&outportval)'
        self.INPORT_TEMPLATE = 'api.set_port_callback(&inport_template, &fn_name)'

    def process(self):
        src_code = None
        if self.__outports:
            src_code = self.prepare_outport_code()
            log.debug("SRC CODE FROM Outport : {}".format(src_code))

        if self.__inports:
            src_code = self.prepare_inport_code(src_code)
            log.debug("SRC CODE FROM INPORT : {}".format(src_code))

        return src_code

    def prepare_inport_code(self, code: str=None):
        inport_values = None
        params = None
        src_code = code
        for p in self.__inports:
            if params:
                params = params + ', ' + p['tgt']['port']
            else:
                params = p['tgt']['port']

            # print(p['tgt']['port'])
            if inport_values:
                inport_values = inport_values + ',' + '"{}"'.format(p['tgt']['port'])
            else:
                inport_values = '"{}"'.format(p['tgt']['port'])

        if len(self.__inports) > 1:
            inport_values = '[ ' + inport_values + ' ]'

        # print("Inport temp", inport_template)
        # print("Inport values", inport_values)
        if inport_values is None:
            inport_values = ""

        inport_template = self.INPORT_TEMPLATE.replace("&inport_template", inport_values).replace("&fn_name",
                                                                                                  self.__fn_name)

        if src_code:
            src_code = src_code + '\n\n' + inport_template
        else:
            src_code = self.__code + '\n\n' + inport_template

        if params:
            src_code = 'def ' + self.__fn_name + '(' + params + ') :\n' + '    ' + src_code.split('):', 1)[1].strip()

        return src_code

    def prepare_outport_code(self):
        src_code = None
        for p in self.__outports:
            outport_values = None
            outport_values = self.OUTPORT_TEMPLATE.format(p['src']['port'])

            if p['src']['port'] not in self.__ports.keys():
                outport_values = outport_values.replace("&outportval", '""')
            else:
                outport_values = self.__prepare_from_port_dictionary(p)

            if src_code:
                src_code = src_code + '\n    ' + outport_values
            else:
                src_code = '\n    ' + outport_values
                src_code = self.__code + src_code

        return src_code

    def __prepare_from_port_dictionary(self, p:dict):
        if isinstance(self.__ports[p['src']['port']], str):
            return self.__prepare_outport_from_str(p)

        if isinstance(self.__ports[p['src']['port']], dict):
            return self.__prepare_outport_from_dict(p)

    def __prepare_outport_from_str(self, p:dict):
        outport_values = self.OUTPORT_TEMPLATE.format(p['src']['port'])
        return outport_values.replace("&outportval", self.__ports[p['src']['port']])

    def __prepare_outport_from_dict(self, p: dict):
        outport_values = self.OUTPORT_TEMPLATE.format(p['src']['port'])

        # when a dict is passed.
        port_dict = self.__ports[p['src']['port']]
        if 'message' in port_dict:
            if port_dict['message']:
                outport_values = outport_values.replace("&outportval", self.OUTPORT_MSG_TEMPLATE)

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

        return outport_values
