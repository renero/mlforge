"""
Test the read of the configuration file and the creation of the pipeline
"""

# pylint: disable=E1101:no-member, W0201:attribute-defined-outside-init, W0511:fixme
# pylint: disable=C0103:invalid-name, W0212:protected-access
# pylint: disable=C0116:missing-function-docstring, C0115:missing-class-docstring
# pylint: disable=R0913:too-many-arguments, R0903:too-few-public-methods
# pylint: disable=R0914:too-many-locals, R0915:too-many-statements
# pylint: disable=W0106:expression-not-assigned, R1702:too-many-branches
# pylint: disable=missing-function-docstring
# pylint: disable=W0212:protected-access, C0413:wrong-import-position

import os
import sys

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

from mlforge.mlforge import Pipeline, Stage


class HostClass:
    def __init__(self, param1=None, param2=None):
        self.param1 = param1
        self.param2 = param2

    def host_method(self, param1=None, param2=None):
        print(f"  > Calling the method \'my_method()\'\n"
              f"    > I got params: param1={param1}, param2={param2}")
        self.param1 = param1
        self.param2 = param2
        return f"host_method({param1}, {param2})"


class SampleClass:
    def __init__(self, param1=None, param2=False):
        self.param1 = param1
        self.param2 = param2
        print(f"  > Called the init of class {self.__class__}\n"
              f"    > I got params: {self.param1}, {self.param2}")

    @staticmethod
    def method():
        print(
            f"  > Called static method \'{SampleClass.method.__name__}\'")
        return "Hi"

    def object_method(self):
        print(f"  > Called object method {self.object_method.__name__}\n"
              f"    > I have params: {self.param1}, {self.param2}")
        return "there!"


class Test_ProcessConfig:
    """
    Test the method `Pipeline._process_config()`.
    """

    config = {
        'step1': {
            'method': 'method_name',
            'class': 'SampleClass',
        },
        'step2': {
            'attribute': 'object',
            'class': 'SampleClass',
        },
        'step3': {
            'attribute': 'result1',
            'method': 'method',
            'class': 'SampleClass',
            'arguments': {
                'param2': 'there!'
            }
        },
        'step4': {
            'method': 'host_method'
        },
        'step5': {
            'method': 'host_method',
            'arguments': {
                'param1': 'Hello',
                'param2': 'there'
            }
        }
    }

    # Method receives a valid YAML configuration dictionary.
    def test_valid_config(self):
        """
        Test the method `Pipeline._process_config()` when it receives a valid
        configuration dictionary.
        """
        host = HostClass()
        pipeline = Pipeline(host, verbose=True, prog_bar=False)
        steps = pipeline._process_config(self.config, __name__)

        assert isinstance(steps, list)
        assert len(steps) == 5
        assert isinstance(steps[0], Stage)
        assert steps[0].attribute_name is None
        assert steps[0].method_name == 'method_name'
        assert steps[0].class_name == SampleClass
        assert steps[0].arguments is None

        assert isinstance(steps[1], Stage)
        assert steps[1].attribute_name == 'object'
        assert steps[1].method_name is None
        assert steps[1].class_name == SampleClass
        assert steps[1].arguments is None

        assert isinstance(steps[2], Stage)
        assert steps[2].attribute_name == 'result1'
        assert steps[2].method_name == 'method'
        assert steps[2].class_name == SampleClass
        assert steps[2].arguments == {'param2': 'there!'}

        assert isinstance(steps[3], Stage)
        assert steps[3].attribute_name is None
        assert steps[3].method_name == 'host_method'
        assert steps[3].class_name is None
        assert steps[3].arguments is None

        assert isinstance(steps[4], Stage)
        assert steps[4].attribute_name is None
        assert steps[4].method_name == 'host_method'
        assert steps[4].class_name is None
        assert steps[4].arguments == {'param1': 'Hello', 'param2': 'there'}

    # Method receives an empty dictionary as input.

    def test_empty_config(self):
        """
        Test the method `Pipeline._process_config()` when it receives an empty
        """
        empty_config = {}

        pipeline = Pipeline()
        steps = pipeline._process_config(empty_config, __name__)

        assert isinstance(steps, list)
        assert len(steps) == 0
