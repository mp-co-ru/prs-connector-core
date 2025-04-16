import pytest

from prs_connector_core.connector import BaseConnector

class TestConnector(BaseConnector):
    async def read_tag(self, tag_config):
        return 42

@pytest.fixture
def connector():
    return TestConnector(config_path="tests/test_config.json")
