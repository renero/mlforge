import pytest
from mlforge.mlforge import Pipeline

class MockStage:
    def __init__(self, method_name=None):
        self.method_name = method_name

@pytest.fixture
def pipeline():
    pipeline = Pipeline()
    pipeline.pipeline = [
        MockStage(method_name="method_one"),
        MockStage(method_name="method_two"),
        MockStage(method_name="another_method"),
        MockStage(method_name=None)
    ]
    return pipeline

def test_contains_method_exact_match(pipeline):
    assert pipeline.contains_method("method_one", exact_match=True) == 1
    assert pipeline.contains_method("method_two", exact_match=True) == 1
    assert pipeline.contains_method("another_method", exact_match=True) == 1
    assert pipeline.contains_method("non_existent_method", exact_match=True) == 0

def test_contains_method_partial_match(pipeline):
    assert pipeline.contains_method("method", exact_match=False) == 3
    assert pipeline.contains_method("another", exact_match=False) == 1
    assert pipeline.contains_method("non_existent", exact_match=False) == 0

def test_contains_method_invalid_method_name(pipeline):
    with pytest.raises(ValueError):
        pipeline.contains_method(None, exact_match=True)

def test_contains_method_invalid_pipeline(pipeline):
    pipeline.pipeline = None
    with pytest.raises(ValueError):
        pipeline.contains_method("method_one", exact_match=True)

def test_contains_method_invalid_method_name_type(pipeline):
    with pytest.raises(TypeError):
        pipeline.contains_method(123, exact_match=True)

def test_contains_method_invalid_exact_match_type(pipeline):
    with pytest.raises(TypeError):
        pipeline.contains_method("method_one", exact_match="True")