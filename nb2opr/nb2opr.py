from __future__ import print_function
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)

from IPython.core.interactiveshell import InteractiveShell


class StopExecution(Exception):
    def _render_traceback_(self):
        pass


# The class MUST call this class decorator at creation time
@magics_class
class DIMagic(Magics):

    @line_magic
    def code_to_operator(self, line):
        "my line magic"
        # print("Full access to the main IPython object:", self.shell)
        # print("Variables in the user namespace:", list(self.shell.user_ns.keys()))
        # print(eval(eval('train'), self.shell.user_ns))
        inst = DIObjectHolder.getInstance()

        if not inst.get_mode():
            raise Exception('Enable DI mode using: {}'.format('%set_di_mode True'))
            # find intersection between userdefined variables and operator code
        if not list(set(self.shell.user_ns.keys()) & set(inst._operator_code.keys())):
            raise Exception('No uniform operator declarations and injections.')

        for k in inst._operator_code.keys():
            op = eval(k, self.shell.user_ns)
            print("Op is", op)
            op.config.script = inst.get_code(k)
            self.shell.user_ns[k] = op
            print(eval(k, self.shell.user_ns))
        InteractiveShell().push(self.shell.user_ns, True)
        print("Code-Pushed-to_operator")

    @cell_magic
    def add_code_to_operator(self, line, cell):

        inst = DIObjectHolder.getInstance()
        if inst.get_mode():
            inst.set_code(eval(line), cell)
        else:
            InteractiveShell().run_cell(cell)

    @line_magic
    def set_di_mode(self, line):

        inst = DIObjectHolder.getInstance()
        if eval(line):
            inst.set_on_cloud_mode()
            print("DI Mode is ON")
        else:
            inst.set_off_cloud_mode()
            print("DI Mode is OFF")

    @line_magic
    def reset_operator_code(self, line):
        inst = DIObjectHolder.getInstance()
        inst.reset_code(line)

    @line_magic
    def get_operator_code(self, line):
        inst = DIObjectHolder.getInstance()
        return inst.get_code(line)

    @line_cell_magic
    def stop_here(self, line, cell=None):
        raise StopExecution


class DIObjectHolder:
    _instance = None

    @staticmethod
    def getInstance():
        if DIObjectHolder._instance is None:
            DIObjectHolder()
        return DIObjectHolder._instance

    def __init__(self):

        self._cloud_mode = False
        self._operator_code = dict()
        DIObjectHolder._instance = self

    def set_code(self, op, code):
        if op in self._operator_code:
            src_code = self._operator_code[op]
            src_code.append('\n' + code)
            self._operator_code[op] = src_code
        else:
            self._operator_code[op] = [code]

    def get_code(self, op):
        src_code = None
        for cd in self._operator_code[op]:
            if src_code is None:
                src_code = cd
            else:
                src_code = src_code + " " + cd
        return src_code

    def reset_code(self, op):
        self._operator_code[op] = []

    def _reset_operator(self):
        self._operator_code = dict()

    def print_code(self, op):
        print(self._operator_code[op])

    def get_mode(self):
        return self._cloud_mode

    def set_off_cloud_mode(self):
        self._cloud_mode = False
        self._reset_operator()

    def set_on_cloud_mode(self):
        self._cloud_mode = True
        self._reset_operator()