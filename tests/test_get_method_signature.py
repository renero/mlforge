"""

Tests the get_method_signature method of the Pipeline class.

"""
# pylint: disable=E1101:no-member, W0201:attribute-defined-outside-init, W0511:fixme
# pylint: disable=C0103:invalid-name, W0212:protected-access
# pylint: disable=C0116:missing-function-docstring, C0115:missing-class-docstring
# pylint: disable=R0913:too-many-arguments, R0903:too-few-public-methods
# pylint: disable=R0914:too-many-locals, R0915:too-many-statements
# pylint: disable=W0106:expression-not-assigned, R1702:too-many-branches
# pylint: disable=missing-function-docstring, C0413:wrong-import-position
# pylint: disable=W0212:protected-access, W0613:unused-argument

import os
import sys

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

import inspect
from mlforge.mlforge import Pipeline


class Test_GetMethodSignature:

    # Returns a dictionary containing the method's parameters and their default values.
    def test_returns_method_parameters_with_default_values(self):
        # Initialize the Pipeline class object
        pipeline = Pipeline()

        # Define a method with parameters and default values
        def example_method(param1, param2=10, param3="default"):
            pass

        # Call the _get_method_signature method and pass the example_method as an
        # argument
        method_signature = pipeline._get_method_signature(example_method)

        # Assert that the method signature is a dictionary
        assert isinstance(method_signature, dict)

        # Assert that the method signature contains the correct parameters and
        # default values
        assert method_signature == {
            "param1": inspect.Parameter.empty, "param2": 10, "param3": "default"}

    # Handles methods with positional-only parameters.
    def test_handles_positional_only_parameters(self):
        # Initialize the Pipeline class object
        pipeline = Pipeline()

        # Define a method with positional-only parameters
        def example_method(param1, param2, param3):
            pass

        # Call the _get_method_signature method and pass the example_method as
        # an argument
        method_signature = pipeline._get_method_signature(example_method)

        # Assert that the method signature is a dictionary
        assert isinstance(method_signature, dict)

        # Assert that the method signature contains the correct parameters and
        # default values
        assert method_signature == {
            "param1": inspect.Parameter.empty,
            "param2": inspect.Parameter.empty,
            "param3": inspect.Parameter.empty
        }
