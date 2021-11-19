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
}


def validate_conda_config(*args):
    # TODO: check that environment cache exists
    pass


def create_or_return_conda_environment(environment_yaml_, use_mamba: bool = False):
    """
    Create a conda environment or return existing environment from a local filesystem.
    """
    pass

