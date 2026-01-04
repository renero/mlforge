"""

Tests for the add_stages method.

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
import string


sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

from mlforge.mlforge import Pipeline, Stage

class TestAddStages:
    def test_add_stages(self):
        # Create a pipeline object
        pipeline = Pipeline()

        # Create a list of stages
        stages = [
            Stage(),
            Stage(),
            Stage()
        ]

        # Add stages to the pipeline
        pipeline.add_stages(stages)

        # Check if the stages are added correctly
        assert len(pipeline.pipeline) == len(stages)

        # Check if the stage attributes are set correctly
        for idx, stage in enumerate(pipeline.pipeline):
            assert stage._num == idx
            assert isinstance(stage._id, str)
            assert len(stage._id) == 8
            assert all(c in string.hexdigits for c in stage._id)

        # Add more stages to the pipeline
        additional_stages = [
            Stage(),
            Stage()
        ]
        pipeline.add_stages(additional_stages)

        # Check if the additional stages are added correctly
        assert len(pipeline.pipeline) == len(stages) + len(additional_stages)

        # Check if the stage attributes are set correctly for the additional stages
        for idx, stage in enumerate(pipeline.pipeline[len(stages):]):
            assert stage._num == idx + len(stages)
            assert isinstance(stage._id, str)
            assert len(stage._id) == 8
            assert all(c in string.hexdigits for c in stage._id)
