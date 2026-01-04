"""

Tests for the parse_step method.

"""
# pylint: disable=E1101:no-member, W0201:attribute-defined-outside-init, W0511:fixme
# pylint: disable=C0103:invalid-name, W0212:protected-access
# pylint: disable=C0116:missing-function-docstring, C0115:missing-class-docstring
# pylint: disable=R0913:too-many-arguments, R0903:too-few-public-methods
# pylint: disable=R0914:too-many-locals, R0915:too-many-statements
# pylint: disable=W0106:expression-not-assigned, R1702:too-many-branches
# pylint: disable=C0413:wrong-import-position

import os
import sys

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

from mlforge.mlforge import Pipeline


# Used to test access to global classes
class SomeClass:
    def __init__(self):
        pass

    def method_name(self):
        pass


def global_method():
    # Used to test access to global methods
    pass


class TestCallableMethod:
    def test_get_callable_method(self):
        # Pipeline without host
        forge = Pipeline()

        # method_name is a method of SomeClass
        method = forge._get_callable_method("method_name", SomeClass)
        assert method is not None

        # wrong_method is NOT a method of SomeClass
        method = forge._get_callable_method("wrong_method", SomeClass)
        assert method is None

        # The class sent does not exist
        try:
            method = forge._get_callable_method(
                "method_name", self.test_get_callable_method)
            catch = False
        except AttributeError:
            catch = True
        assert catch

        # Now, with a host object
        forge = Pipeline(host=SomeClass())

        # Test case 1: method_name is a method of the host object
        method = forge._get_callable_method("method_name")
        assert method is not None

        # Test case 2: method_name is a method in the pipeline object
        # Create a method in the pipeline object
        setattr(forge, "local_method", lambda: None)
        method = forge._get_callable_method("local_method")
        assert method is not None

        # CANNOT test this since tests are in a different namespace
        # Test case 3: method_name is a function in globals
        # globals()["global_method"] = global_function
        # method = forge._get_callable_method("global_method")
        # assert method is not None

        # Test case 4: method_name is not found
        method = forge._get_callable_method("nonexistent_method")
        assert method is None
