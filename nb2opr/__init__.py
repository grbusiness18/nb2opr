""" DI Notebook 2 Operator Helper """
__version__ = '0.0.1'

from .nb2opr import DIMagic

try:
    ip = get_ipython()
    ip.register_magics(DIMagic)
except:
    pass