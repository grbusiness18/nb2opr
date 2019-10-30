import unittest
import os
from nb2opr.exporters import add_code_to_operator
from nb2opr.exporters import add_connections_to_port, add_port_to_operator, get_pipelines
from nb2opr.dimanager import DIManager
import sapdi as di
import warnings
import urllib3
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('di_logger')
urllib3.disable_warnings()

class TestProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        #logger.setLevel(logging.DEBUG)
        scenario = di.get_current_scenario()
        pipeline = di.create_pipeline(name="testv1", description="test generated", from_template='com.sap.dsp.templates.python_producer_template')
        cls.pipeline_id = pipeline.id
        cls.scenario_id = scenario.id
        cls.s1 = {
            'operator_name': 'python3operator1',
            'invalid_operator': 'python3OPERATO1',
            'non_script_operator': 'readfile1'
        }

        cls.s2 = {
            'src_opr_name':  'python3operator1',
            'src_port_name': 'metrics',
            'tgt_opr_name': 'submitmetrics1',
            'tgt_port_name': 'response',
            'invalid_port_name': 'testport',
            'invalid_opr_name': 'testopr'
        }

        cls.s3 = {
            'operator_name': 'python3operator1',
            'port_name': 'metrics2',
            'target': ('submitmetrics1', 'response'),
            'invalid_opr_name': 'testopr',
            'invalid_port_name': 'testport1',
        }
        cls.di_manager = DIManager.get_instance()
        cls.di_manager.set_pipeline(cls.pipeline_id)
        warnings.simplefilter('ignore', category=ImportWarning)
        warnings.filterwarnings('ignore', message='Unverified HTTPS request')
        warnings.simplefilter("ignore", ResourceWarning)

    @classmethod
    def tearDownClass(cls):
        p = di.get_pipeline(pipeline_id=cls.pipeline_id)
        p.delete()

    def test_s1_add_code_to_operator_success(self):
        @add_code_to_operator(di_mode=True, pipeline_id=self.pipeline_id, operator_name=self.s1['operator_name'])
        def test_function():
            print("Hello World !!")

        code = test_function()
        graph = self.di_manager.get_graph()
        print(code)
        print(graph.operators[self.s1['operator_name']].config['script'])
        self.assertEqual(graph.operators[self.s1['operator_name']].config['script'], code)

    def test_s1_add_code_to_operator_di_mode_off(self):
        @add_code_to_operator(di_mode=False, pipeline_id=self.pipeline_id, operator_name=self.s1['operator_name'])
        def test_function():
            print("Hello World !!")

        code = test_function()
        self.assertIsNone(code)

    def test_s1_add_code_to_operator_invalid_operator(self):
        @add_code_to_operator(di_mode=True, pipeline_id=self.pipeline_id, operator_name=self.s1['invalid_operator'])
        def test_function():
            print("Hello World !!")

        self.assertRaises(Exception, lambda: test_function())

    def test_s1_add_code_to_operator_non_script_operator(self):
        @add_code_to_operator(di_mode=True, pipeline_id=self.pipeline_id, operator_name=self.s1['non_script_operator'])
        def test_function():
            print("Hello World !!")

        self.assertRaises(Exception, lambda: test_function())

    ## scenario 2

    def delete_connections(self, src_opr, src_port, tgt_opr, tgt_port):
        graph = self.di_manager.get_graph()
        graph.delete_connection(operator_src=src_opr,
                                port_src=src_port,
                                operator_tgt=tgt_opr,
                                port_tgt=tgt_port)
        self.di_manager.save_graph()

    def test_s2_add_connections_to_port_success(self):
        self.delete_connections(self.s2['src_opr_name'], self.s2['src_port_name'],
                                self.s2['tgt_opr_name'], self.s2['tgt_port_name'])
        port_added = add_connections_to_port(src_opr_name=self.s2['src_opr_name'],
                                             src_port_name=self.s2['src_port_name'],
                                             tgt_opr_name=self.s2['tgt_opr_name'],
                                             tgt_port_name=self.s2['tgt_port_name'])

        self.assertTrue(port_added)

    def test_s2_add_connections_to_port_invalid_operator(self):
        self.assertRaises(Exception, lambda: add_connections_to_port(src_opr_name=self.s2['invalid_opr_name'],
                                                                     src_port_name=self.s2['src_port_name'],
                                                                     tgt_opr_name=self.s2['tgt_opr_name'],
                                                                     tgt_port_name=self.s2['tgt_port_name'])())

    def test_s2_add_connections_to_port_invalid_port(self):
        self.assertRaises(Exception, lambda: add_connections_to_port(src_opr_name=self.s2['src_opr_name'],
                                                                     src_port_name=self.s2['src_port_name'],
                                                                     tgt_opr_name=self.s2['tgt_opr_name'],
                                                                     tgt_port_name=self.s2['invalid_port_name'])())

    def test_s2_add_connections_to_port_invalid_connections(self):
        self.assertRaises(Exception, lambda: add_connections_to_port(src_opr_name=self.s2['tgt_opr_name'],
                                                                     src_port_name=self.s2['tgt_port_name'],
                                                                     tgt_opr_name=self.s2['src_opr_name'],
                                                                     tgt_port_name=self.s2['src_port_name'])())

    def test_s2_add_connections_to_port_invalid_pipeline(self):
        self.assertRaises(Exception, lambda: add_connections_to_port(src_opr_name=self.s2['src_opr_name'],
                                                                     src_port_name=self.s2['src_port_name'],
                                                                     tgt_opr_name=self.s2['tgt_opr_name'],
                                                                     tgt_port_name=self.s2['tgt_port_name'],
                                                                     pipeline_id='shdka-dsahadasjhkd-.sdhaskdhakd'))

    ### add port

    def delete_port(self, opr_name:str, port_name:str):
        graph = self.di_manager.get_graph()
        graph.operators[opr_name].operatorinfo.delete_outport(port_name)
        self.di_manager.set_graph(graph)

        self.di_manager.save_graph()

    def test_s3_add_port_to_operator_success(self):
        #self.delete_port(opr_name=self.s3['operator_name'], port_name=self.s3['port_name'])
        port_added = add_port_to_operator(opr_name=self.s3['operator_name'],
                                          porttype='OUTPORT',
                                          portname=self.s3['port_name'],
                                          portkind='string',
                                          target=self.s3['target'])
        self.assertTrue(port_added)

    def test_s3_add_port_to_operator_invalid_operator(self):
        #self.delete_port(opr_name=self.s3['operator_name'], port_name=self.s3['metrics'])
        self.assertRaises(Exception, lambda: add_port_to_operator(opr_name=self.s3['invalid_opr_name'],
                                          porttype='OUTPORT',
                                          portname=self.s3['port_name'],
                                          portkind='string'))

    def test_s4_get_pipelines(self):
        pipelines = get_pipelines()
        self.assertEqual(len(pipelines), 1)








