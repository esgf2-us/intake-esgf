import pytest

import intake_esgf


@pytest.fixture(autouse=True)
def reset_intake_esgf_config():
    """Reset the intake_esgf configuration before each test."""
    intake_esgf.conf.reset()
