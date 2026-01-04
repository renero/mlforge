"""

Tests for the build_params method.

"""
# pylint: disable=E1101:no-member, W0201:attribute-defined-outside-init, W0511:fixme
# pylint: disable=C0103:invalid-name, W0212:protected-access
# pylint: disable=C0116:missing-function-docstring, C0115:missing-class-docstring
# pylint: disable=R0913:too-many-arguments, R0903:too-few-public-methods
# pylint: disable=R0914:too-many-locals, R0915:too-many-statements
# pylint: disable=W0106:expression-not-assigned, R1702:too-many-branches
# pylint: disable=W0612:unused-variable, E0602:undefined-variable
# pylint: disable=C0413:wrong-import-position

import os
import sys

import pytest


sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

from mlforge.mlforge import Pipeline


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


class Test_BuildParams:

    # Builds parameters with default values and method arguments
    def test_build_params_with_default_values_and_arguments(self):
        method_parameters = {'param1': 'value1',
                             'param2': 'value2', 'param3': 'value3'}
        method_arguments = {'param1': 'new_value1', 'param3': 'new_value3'}

        pipeline = Pipeline()
        params = pipeline._build_params(method_parameters, method_arguments)

        assert params == {'param1': 'new_value1',
                          'param2': 'value2', 'param3': 'new_value3'}

    # Raises ValueError if parameter not found in host object or globals and
    # has no default value
    def test_build_params_raises_value_error(self):
        method_parameters = {'param1': 'value1', 'param2': 'value2'}
        method_arguments = {'param1': 'new_value1', 'param3': 'value3'}

        pipeline = Pipeline()
        with pytest.raises(ValueError):
            params = pipeline._build_params(
                method_parameters, method_arguments)

    # Builds parameters with values from the host object and method arguments
    def test_build_params_with_host_object_and_arguments(self):
        pipeline = Pipeline()
        pipeline.host = HostClass()
        param1 = 'host_value1'
        pipeline.host.param1 = param1

        method_parameters = {
            'p1': 'value1', 'p2': 'value2', 'p3': 'value3'}
        method_arguments = {
            'p1': param1, 'p3': 'new_value3'}

        params = pipeline._build_params(method_parameters, method_arguments)

        assert params == {'p1': 'host_value1',
                          'p2': 'value2', 'p3': 'new_value3'}

    # Builds parameters with values from globals and method arguments
    def test_build_params_with_globals_and_arguments(self):
        pipeline = Pipeline()
        globals()['param1'] = 'global_value1'

        method_parameters = {
            'p1': 'value1', 'p2': 'value2', 'p3': 'value3'}
        method_arguments = {
            'p1': param1, 'p3': 'new_value3'}

        params = pipeline._build_params(method_parameters, method_arguments)

        assert params == {'p1': 'global_value1',
                          'p2': 'value2', 'p3': 'new_value3'}

    # Builds parameters with default values only
    def test_build_params_with_default_values_only(self):
        method_parameters = {'param1': 'value1', 'param2': 'value2'}
        method_arguments = None

        pipeline = Pipeline()

        params = pipeline._build_params(method_parameters, method_arguments)

        assert params == {'param1': 'value1', 'param2': 'value2'}
