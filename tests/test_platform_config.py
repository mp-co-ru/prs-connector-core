from uuid import uuid4
from prs_connector_core.config import PlatformConfig, ConnectorPrsJsonConfigStringFromPlatform

def test_platform_config_from_file():
    connector_id = str(uuid4())
    plt_config = PlatformConfig.from_file(connector_id=connector_id)
    con_json_config_str = ConnectorPrsJsonConfigStringFromPlatform()
    con_json_config_str.log.fileName = f"logs/prs_connector_{connector_id}.log"
    assert plt_config.prsJsonConfigString == con_json_config_str
    assert plt_config.tags == {}