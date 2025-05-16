from typing import Any
import pytest
from uuid import uuid4
from prs_connector_core.config import PlatformConfig, PrsJsonConfigStringFromPlatform

def test_platform_config_from_file():
    plt_config = PlatformConfig.from_file(uuid4())
    assert plt_config.prsJsonConfigString == PrsJsonConfigStringFromPlatform()
    assert plt_config.tags == []