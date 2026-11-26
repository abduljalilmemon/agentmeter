"""requirements.lock must pin every dependency declared in pyproject.toml, compatibly."""

import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from tests.conftest import REPO_ROOT


def _pins() -> dict[str, str]:
    pins = {}
    for line in (REPO_ROOT / "requirements.lock").read_text("utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            name, _, version = line.partition("==")
            assert version, f"not an exact pin: {line!r}"
            pins[canonicalize_name(name)] = version
    return pins


def test_lock_pins_every_declared_dependency() -> None:
    project = tomllib.loads(Path(REPO_ROOT / "pyproject.toml").read_text("utf-8"))["project"]
    declared = project["dependencies"] + project["optional-dependencies"]["dev"]
    pins = _pins()
    for spec in declared:
        req = Requirement(spec)
        name = canonicalize_name(req.name)
        assert name in pins, f"{req.name} is not in requirements.lock: regenerate it"
        assert req.specifier.contains(pins[name], prereleases=True), (
            f"{req.name}=={pins[name]} does not satisfy {req.specifier}: regenerate the lock"
        )


def test_runtime_image_does_not_install_the_demo_extra() -> None:
    """The proxy image installs the package without extras: no langgraph at runtime."""
    dockerfile = (REPO_ROOT / "Dockerfile").read_text("utf-8")
    installs = [line for line in dockerfile.splitlines() if "pip install" in line]
    assert installs, "no pip install in the Dockerfile"
    for line in installs:
        assert "[demo" not in line and "langgraph" not in line, line
        assert "[dev" not in line, line
    assert any(line.rstrip().endswith("-c requirements.lock .") for line in installs), installs
    assert "examples" not in dockerfile  # the demo code isn't copied into the image
