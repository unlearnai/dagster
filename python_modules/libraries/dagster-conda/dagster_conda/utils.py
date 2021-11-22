import hashlib
import pathlib
import subprocess

from dagster import Field, StringSource, BoolSource, check


CONDA_CONFIG_SCHEMA = {
    "env_file": Field(
        StringSource,
        is_required=True,
        description="The conda environment yaml configuration file to use.",
    ),
    "use_mamba": Field(
        BoolSource,
        is_required=False,
        default_value=False,
        description="Whether to enable the use of mamba to create the environment rather than conda.",
    ),
    "environment_cache": Field(
        StringSource,
        is_required=False,
        default_value=False,
        description="Whether to enable the use of mamba to compute and create the environment rather than conda.",
    ),
    "environment_create_timeout": Field(
        StringSource,
        is_required=False,
        default_value=False,
        description="Whether to enable the use of mamba to compute and create the environment rather than conda.",
    ),
}


def validate_conda_config(*args):
    # TODO: check that environment cache exists
    pass


_HASH_PREFIX_LENGTH = 2
_DEFAULT_CREATE_TIMEOUT = 3600


def _validate_environment_yaml(environment_yaml):
    """Check that the environment file is valid"""
    environment_yaml = pathlib.Path(environment_yaml)

    if not environment_yaml.is_file() and environment_yaml.suffix in ('yml', 'yaml'):
        raise ValueError("Invalid environment file.")

    if not environment_yaml.exists():
        raise RuntimeError("Environment file not found.")


def create_or_return_conda_executable_path(environment_yaml, cache_path, use_mamba: bool = False, create_timeout=_DEFAULT_CREATE_TIMEOUT):
    """
    Create a conda environment or return existing environment from a local filesystem.
    """
    environment_yaml = pathlib.Path(environment_yaml).expanduser().absolute()
    _validate_environment_yaml(environment_yaml)

    with open(environment_yaml, 'rb') as f:
        hasher = hashlib.sha256(usedforsecurity=False)
        hasher.update(f.read())

    ehash = hasher.hexdigest()
    env_cache = pathlib.Path(cache_path).expanduser().absolute()
    env_cache.mkdir(parents=True, exist_ok=True)

    env_prefix = (env_cache / ehash[:_HASH_PREFIX_LENGTH] / ehash[_HASH_PREFIX_LENGTH:]).absolute()
    executable_path = env_prefix / "bin" / "python"

    # check whether environment exists
    if env_prefix.exists():
        # assume path was successfully created
        # TODO: check that this works on Windows
        return executable_path

    # create an environment
    conda_binary = "mamba" if use_mamba else "conda"
    proc = subprocess.run(
        [
            conda_binary,
            "env",
            "create",
            "--yes",
            "--prefix",
            str(env_prefix),
            "--file",
            str(environment_yaml)
        ],
        timeout=create_timeout,
        check=True,
        use_shell=True
    )

    return str(executable_path)
