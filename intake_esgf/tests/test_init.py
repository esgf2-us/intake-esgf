import intake_esgf


def test_supported_projects():
    projects = intake_esgf.supported_projects()
    assert isinstance(projects, list)
