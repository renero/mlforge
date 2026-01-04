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

from mlforge.mlforge import Stage, Pipeline


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


class Test_GetStepComponents:

    # Correctly parses a forge step with a method name
    def test_correctly_parses_forge_step_with_method_name(self):
        forge_step = (
            'new_attribute', 'method_name', SampleClass, {'param1': 'value1'})
        stage = Stage()
        pipeline = Pipeline()
        result = pipeline._get_step_components(forge_step, stage)

        assert result.attribute_name == 'new_attribute'
        assert result.method_name == 'method_name'
        assert result.class_name == SampleClass
        assert result.arguments == {'param1': 'value1'}

    # Raises a ValueError if the forge step is not a tuple
    def test_raises_value_error_if_forge_step_not_tuple(self):
        with pytest.raises(AssertionError):
            Pipeline()._get_step_components(None, Stage())

        with pytest.raises(AssertionError):
            Pipeline()._get_step_components((), Stage())

        with pytest.raises(AssertionError):
            Pipeline()._get_step_components((123), Stage())

        with pytest.raises(AssertionError):
            Pipeline()._get_step_components(
                (self.test_raises_value_error_if_forge_step_not_tuple), Stage())
