from functools import partial
from typing import Any

import intake_esgf


def test_in_notebook(monkeypatch):
    def fake_get_ipython(shell_name: str) -> Any:
        if shell_name == "ZMQInteractiveShell":

            class ZMQInteractiveShell:
                pass

            return ZMQInteractiveShell()

        elif shell_name == "TerminalInteractiveShell":

            class TerminalInteractiveShell:
                pass

            return TerminalInteractiveShell()

        class NotAShellThatIsSupported:
            pass

        return NotAShellThatIsSupported()

    # We can't test being in a notebook so here we fake the possibilities
    if "get_ipython" in globals():
        for shell_name, result in zip(
            [
                "ZMQInteractiveShell",
                "TerminalInteractiveShell",
                "NotRealShell",
            ],
            [True, False, False],
        ):
            monkeypatch.setattr("get_ipython", partial(fake_get_ipython, shell_name))
            assert intake_esgf.in_notebook() == result
    else:
        assert not intake_esgf.in_notebook()


def test_supported_projects():
    projects = intake_esgf.supported_projects()
    assert isinstance(projects, list)
