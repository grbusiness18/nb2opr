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
    def update_outport_to_operator(self, operator_id):
        """ Update outport to operator"""
        instance = DIObjectHandler.get_instance()

        if not instance.di_mode:
            print("Set DI mode to ON..")
        else:
            args = parse_argstring(self.update_outport_to_operator, operator_id)
            operator_id = eval(args.operator_id)
            port = None
            val = None
            typ = ""
            context = ""

            if args.val is None:
                raise Exception("Variable is missing")
            else:
                val = eval(args.val)

            if args.port is None:
                raise Exception("Port is missing")
            else:
                port = eval(args.port)

            if not args.typ is None:
                typ = eval(args.typ)

            if not args.context is None:
                context = eval(args.context)

            # print(self.shell.user_ns)

            # if eval(args.val) not in self.shell.user_ns:
            #    raise Exception("Variable {} is not declared before".format(eval(args.val)))

            ## check variable declaration in source code of operator

            instance = DIObjectHandler.get_instance()
            instance.add_out_port_val_to_operator(operator_id, port, val, typ, context)

    @line_magic
    def save(self, line):
        instance = DIObjectHandler.get_instance()
        if instance.di_mode:
            instance.save_graph_to_di()
        else:
            print("Set DI Mode to ON..")

    @line_magic
    def set_di_mode_on(self, line):
        instance = DIObjectHandler.get_instance()
        instance.set_di_mode_on()
        print("DI Upload Mode is ON")

    @line_magic
    def set_di_mode_off(self, line):
        instance = DIObjectHandler.get_instance()
        instance.set_di_mode_off()
        print("DI Upload Mode is OFF")

    @line_magic
    def set_di_pipeline(self, pipeline_id):
        instance = DIObjectHandler.get_instance()
        if instance.di_mode:
            instance.pipeline_id = eval(pipeline_id)
        else:
            print("Set DI Mode to ON..")

    @line_magic
    def get_operators(self, line):
        instance = DIObjectHandler.get_instance()
        if instance.di_mode:
            print(instance.get_operators())
        else:
            print("Set DI Mode to ON..")

    @line_magic
    def preview(self, line):
        instance = DIObjectHandler.get_instance()
        if instance.di_mode:
            instance.preview()
        else:
            print("Set DI Mode to ON..")
