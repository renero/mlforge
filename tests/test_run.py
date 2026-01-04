"""

Tests for the RUN method.

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
    def function1(self):
        pass

    def function2(self):
        pass

    def function3(self):
        pass

    def method(self):
        return 1

class SampleClass:
    @staticmethod
    def method():
        return 1


class TestRun:

    # Runs the pipeline successfully with non-empty list of steps.
    def test_runs_pipeline_successfully(self):
        host_object = HostClass()
        pipeline = Pipeline(host=host_object, description="main")
        pipeline.from_list([
            ('function1'),
            ('function2'),
            ('function3')
        ])
        pipeline.run()
        pipeline.close()

    # Raises an assertion error if the pipeline is empty.
    def test_raises_assertion_error_if_pipeline_empty(self):
        pipeline = Pipeline()
        with pytest.raises(AssertionError):
            pipeline.run()
        pipeline.close()

    # Run a pipeline created with no host, and a single step that returns an attribute.
    # Check that the return value is in globals.
    def test_pipeline_with_no_host_and_single_step_returns_attribute(self):
        pipeline = Pipeline()
        pipeline.from_list([
            ('obj', SampleClass),
            ('attribute_name', 'obj.method')
        ])
        pipeline.run()
        assert 'attribute_name' in pipeline.attributes_
        assert pipeline.get_attribute('attribute_name') == 1
        pipeline.close()

    # Create a pipeline with a host object and a simple stage in the pipeline that
    # returns an attribute. check that the attribute is in the host object.
    def test_pipeline_with_host_object(self):
        # Create a host object
        class Host:
            pass
        host = Host()

        # Create a pipeline
        pipeline = Pipeline(host=host, prog_bar=False)
        pipeline.from_list([
            ('attribute_name', 'method', SampleClass)
        ])

        # Run the pipeline
        pipeline.run()

        # Check if the attribute is in the host object
        assert hasattr(host, 'attribute_name')
        pipeline.close()
