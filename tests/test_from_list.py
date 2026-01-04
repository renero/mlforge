
"""

Test the from_list method of the Pipeline class.

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

import pytest

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

from mlforge.mlforge import Pipeline, Stage


class HostClass:
    def __init__(self, param1=None, param2=None):
        self.host_param1 = param1
        self.host_param2 = param2

    def host_method(self, param1=None, param2=None):
        print(f"  > Calling the method \'my_method()\'\n"
              f"    > I got params: param1={param1}, param2={param2}")
        self.host_param1 = param1
        self.host_param2 = param2
        return f"host_method({param1}, {param2})"


class SampleClass:
    def __init__(self, param1=None, param2=False):
        self.class_param1 = param1
        self.class_param2 = param2
        print(f"  > Called the init of class {self.__class__}\n"
              f"    > I got params: {self.class_param1}, {self.class_param2}")

    def class_method(self, param1=None, param2=None):
        self.class_param1 = param1
        self.class_param2 = param2
        print(
            f"  > Called static method \'{SampleClass.method.__name__}\'")
        return "Hi"


class TestFromList:

    # can load a pipeline from a list of steps
    def test_load_pipeline_from_list(self):
        pipeline = Pipeline(host=HostClass, verbose=True)
        steps = [
            ('attribute1', 'host_method'),
            ('attribute2', SampleClass),
            ('method_name', {'argument1': 'arg_value1'}),
            ('class_method', SampleClass, {'argument1': 'arg_value1'}),
            ('attribute5', 'class_method',
             SampleClass, {'argument1': 'attribute1'})
        ]
        pipeline.from_list(steps)
        assert len(pipeline.pipeline) == len(steps)
        assert isinstance(pipeline.pipeline[0], Stage)

        assert pipeline.pipeline[0].attribute_name == 'attribute1'
        assert pipeline.pipeline[0].method_name == 'host_method'

        assert pipeline.pipeline[1].attribute_name == 'attribute2'
        assert pipeline.pipeline[1].class_name == SampleClass

        assert pipeline.pipeline[2].attribute_name is None
        assert pipeline.pipeline[2].method_name == 'method_name'
        assert pipeline.pipeline[2].class_name is None
        assert pipeline.pipeline[2].arguments == {'argument1': 'arg_value1'}

        assert pipeline.pipeline[3].attribute_name is None
        assert pipeline.pipeline[3].method_name == 'class_method'
        assert pipeline.pipeline[3].class_name == SampleClass
        assert pipeline.pipeline[3].arguments == {'argument1': 'arg_value1'}

        assert pipeline.pipeline[4].attribute_name == 'attribute5'
        assert pipeline.pipeline[4].method_name == 'class_method'
        assert pipeline.pipeline[4].class_name == SampleClass
        assert pipeline.pipeline[4].arguments == {'argument1': 'attribute1'}

    # can handle empty list of steps
    def test_empty_list_of_steps(self):
        with pytest.raises(AssertionError):
            Pipeline().from_list([])

    # can handle invalid parameter types
    def test_invalid_parameter_types(self):
        with pytest.raises(ValueError):
            Pipeline().from_list([
                ('step1', 123)  # Invalid parameter type
            ])

        with pytest.raises(AssertionError):
            Pipeline().from_list([
                ({'attr': 'value'})  # Invalid parameter type
            ])

        with pytest.raises(ValueError):
            Pipeline().from_list([
                (SampleClass, {'attr': 'value'})  # Invalid parameter type
            ])

        with pytest.raises(ValueError):
            Pipeline().from_list([
                (SampleClass, HostClass)  # Invalid parameter type
            ])

        with pytest.raises(ValueError):
            Pipeline().from_list([
                ('attr', 'attr', 'attr')  # Invalid parameter type
            ])
