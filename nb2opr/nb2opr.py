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
    def lmagic(self, line):
        "my line magic"
        print("Full access to the main IPython object:", self.shell)
        print("Variables in the user namespace:", list(self.shell.user_ns.keys()))
        return line

    @cell_magic
    def add_code_to_operator(self, line, cell):

        inst = DIObjectHolder.getInstance()
        if inst.get_mode():
            inst.set_code(line, cell)
        else:
            InteractiveShell().run_cell(cell)

    @line_magic
    def set_cloud_mode(self, line):

        inst = DIObjectHolder.getInstance()
        if eval(line):
            inst.set_on_cloud_mode()
        else:
            inst.set_off_cloud_mode()

    @line_magic
    def reset_operator_code(self, line):
        inst = DIObjectHolder.getInstance()
        inst.reset_code(line)

    @line_magic
    def get_operator_code(self, line):
        inst = DIObjectHolder.getInstance()
        return inst.get_code(line)

    @line_cell_magic
    def stop_here(self, line, cell):
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

    def set_on_cloud_mode(self):
        self._cloud_mode = True
        self._reset_operator()