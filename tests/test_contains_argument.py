import pytest
from mlforge.mlforge import Pipeline

class MockStage:
    def __init__(self, arguments=None):
        self.arguments = arguments

@pytest.fixture
def pipeline():
    pipeline = Pipeline()
    pipeline.pipeline = [
        MockStage(arguments={'arg1': 'value1', 'arg2': 'value2'}),
        MockStage(arguments={'arg1': 'value3'}),
        MockStage(arguments={'arg3': 'value4'}),
        MockStage(arguments=None)
    ]
    return pipeline

def test_contains_argument_found(pipeline):
    assert pipeline.contains_argument('arg1') == 2
    assert pipeline.contains_argument('arg2') == 1
    assert pipeline.contains_argument('arg3') == 1

def test_contains_argument_not_found(pipeline):
    assert pipeline.contains_argument('arg4') == 0

def test_contains_argument_none_attribute_name(pipeline):
    with pytest.raises(ValueError, match="attribute_name must not be None"):
        pipeline.contains_argument(None)

def test_contains_argument_none_pipeline():
    pipeline = Pipeline()
    pipeline.pipeline = None
    with pytest.raises(ValueError, match="pipeline must not be None"):
        pipeline.contains_argument('arg1')

def test_contains_argument_invalid_attribute_name(pipeline):
    with pytest.raises(TypeError, match="attribute_name must be a string."):
        pipeline.contains_argument(123)