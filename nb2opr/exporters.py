from .dimanager import DIManager

DIManager.di_connect()


def add_code_to_existing_pipeline_operator(di_mode: bool = False, pipeline_id: str = None, operator_name: str = None, ports: dict = {}):
    def wrapper(fn):
        def inner_wrapper(*args, **kwargs):
            if di_mode:
                instance = DIManager.get_instance()
                instance.set_di_mode_on()

                assert pipeline_id is not None, "Invalid or Empty pipeline-id"

                instance.set_pipeline(pipeline_id)

                assert operator_name is not None, "Invalid or Empty operator-name"

                import inspect

                code = inspect.getsource(fn).split('\n', 1)[1]
                fn_name = fn.__name__

                instance.add_code_to_operator(op=operator_name, fn_name=fn_name, code=code, ports=ports)
                print("code added to operator")
                return None
            else:
                return fn(*args, **kwargs)

        return inner_wrapper
    return wrapper


def add_connections_to_port(src_opr_name: str, src_port_name: str, tgt_opr_name: str, tgt_port_name: str, pipeline_id: str = None):
    instance = DIManager.get_instance()
    instance.set_di_mode_on()
    assert pipeline_id is not None, "Invalid or Empty pipeline-id"
    instance.set_pipeline(pipeline_id)
    instance.add_connections_to_port(src_opr_name=src_opr_name, src_port_name=src_port_name, tgt_opr_name=tgt_opr_name, tgt_port_name=tgt_port_name)
    print("Conncetions Updated Successfully !!")


def add_port_to_operator(opr_name: str, porttype: str, portname: str, portkind:str, target:tuple=(), pipeline_id: str=None):
    instance = DIManager.get_instance()
    instance.set_di_mode_on()
    assert pipeline_id is not None, "Invalid or Empty pipeline-id"
    instance.set_pipeline(pipeline_id)
    instance.add_port_to_operator(opr=opr_name, porttype=porttype, portname=portname.lower(), portkind=portkind, target=target)
    print("Port '{}' created successfully".format(portname.lower()))


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
    pipeline = instance.create_pipeline(name=name,desc=desc, template=template)
    print("Pipeline '{}' has been created successfully".format(pipeline.id))







