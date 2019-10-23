from __future__ import print_function
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)

from IPython.core.interactiveshell import InteractiveShell
from IPython.core.magic_arguments import (argument, magic_arguments,
    parse_argstring)

from .diobjecthandler import DIObjectHandler

class StopExecution(Exception):
    def _render_traceback_(self):
        pass


@magics_class
class DIMagic(Magics):

    @cell_magic
    @magic_arguments()
    @argument('operator_id', type=str, help='Operator ID in DI.')
    def add_code_to_operator(self, operator_id, cell):
        """ Add code to DI operator which is defined already in Pipeline."""
        args = parse_argstring(self.add_code_to_operator, operator_id)
        print(args.operator_id)
        print(cell)
        instance = DIObjectHandler.get_instance()
        if instance.di_mode:
            instance.add_code_to_operator(op=eval(operator_id), code=cell)
        else:
            InteractiveShell().run_cell(cell)

    @line_magic
    @magic_arguments()
    @argument('-p', '--port', type=str, help='An optional argument.')
    @argument('-v', '--val', type=str, help='An optional argument.')
    @argument('-t', '--typ', type=str, help='An optional argument.')
    @argument('-c', '--context', type=str, help='An optional argument.')
    @argument('operator_id', type=str, help='An integer positional argument.')
    def add_port_to_code(self, operator_id):
        """ A really cool magic command."""
        args = parse_argstring(self.add_port_to_code, operator_id)

        if args.val is None:
            raise Exception("Variable is missing")

        if args.port is None:
            raise Exception("Port is missing")

            # print(self.shell.user_ns)

        if eval(args.val) not in self.shell.user_ns:
            raise Exception("Variable {} is not declared before".format(eval(args.val)))

        ## check variable declaration in source code of operator

        instance = DIObjectHandler.get_instance()
        instance.add_out_port_val_to_operator(eval(args.operator_id), eval(args.port), eval(args.val),
                                              eval(args.typ.upper()), eval(args.context.upper()))

    @line_magic
    def save(self, line):
        instance = DIObjectHandler.get_instance()
        instance.save_graph_to_di()
