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


def test_all_argument_values_found(pipeline):
    result = pipeline.all_argument_values('arg1')
    assert result == ['value1', 'value3']


def test_all_argument_values_not_found(pipeline):
    result = pipeline.all_argument_values('arg4')
    assert result == []


def test_all_argument_values_none_arguments(pipeline):
    result = pipeline.all_argument_values('arg3')
    assert result == ['value4']


def test_all_argument_values_empty_pipeline():
    pipeline = Pipeline()
    pipeline.pipeline = []
    with pytest.raises(AssertionError, match="pipeline must be initialized"):
        pipeline.all_argument_values('arg1')


def test_all_argument_values_invalid_attribute_name(pipeline):
    with pytest.raises(AssertionError, match="attribute_name must not be None"):
        pipeline.all_argument_values(None)


def test_all_argument_values_pipeline_not_initialized():
    pipeline = Pipeline()
    with pytest.raises(AssertionError, match="pipeline must be initialized"):
        pipeline.all_argument_values('arg1')
