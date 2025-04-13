from prs_connector_core.exceptions import (
    ConnectionError,
    ConfigValidationError,
    DataProcessingError
)

def test_exceptions():
    assert str(ConnectionError("test")) == "test"
    assert isinstance(ConfigValidationError(), Exception)
    assert issubclass(DataProcessingError, Exception)