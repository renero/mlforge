"""

Tests for the progress bar functionality in the Pipeline class.

"""
# pylint: disable=E1101:no-member, W0201:attribute-defined-outside-init, W0511:fixme
# pylint: disable=C0103:invalid-name, W0212:protected-access
# pylint: disable=C0116:missing-function-docstring, C0115:missing-class-docstring
# pylint: disable=R0913:too-many-arguments, R0903:too-few-public-methods
# pylint: disable=R0914:too-many-locals, R0915:too-many-statements
# pylint: disable=W0106:expression-not-assigned, R1702:too-many-branches
# pylint: disable=missing-function-docstring
# pylint: disable=W0212:protected-access, C0413:import-misplaced

import os
import sys

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

from mlforge.mlforge import Pipeline


class Test_PbarUpdate:

    # Creates a progress bar with the total number of steps in the pipeline.
    def test_progress_bar_creation(self):
        pipeline = Pipeline()
        pipeline.pipeline = [1, 2, 3, 4, 5]
        pipeline._pbar_create("main", len(pipeline.pipeline))
        assert pipeline.pbar.progress._tasks[0].total == len(pipeline.pipeline)
        pipeline.close()

    # The pipeline has no steps, so the progress bar is not created.
    def test_no_progress_bar_creation(self):
        pipeline = Pipeline()
        pipeline._pbar_create("main", len(pipeline.pipeline))
        assert pipeline.pbar is None
        pipeline.close()

    # Can update the progress bar by a step of 1
    def test_update_progress_bar_by_step_of_1(self):
        pipeline = Pipeline()
        pipeline.pipeline = [1, 2, 3, 4, 5]
        pipeline._pbar_create("main", len(pipeline.pipeline))
        pipeline._pbar_update("main", 1)
        assert pipeline.pbar.progress._tasks[0].completed == 1
        pipeline.close()
