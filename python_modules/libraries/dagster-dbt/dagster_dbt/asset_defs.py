import hashlib
import json
import os
import textwrap
from typing import AbstractSet, Any, Callable, Dict, Mapping, Optional, Sequence, Set, Tuple

from dagster_dbt.cli.types import DbtCliOutput
from dagster_dbt.cli.utils import execute_cli
from dagster_dbt.types import DbtOutput
from dagster_dbt.utils import generate_events

from dagster import (
    AssetKey,
    AssetMaterialization,
    AssetsDefinition,
    In,
    MetadataValue,
    Nothing,
    Out,
    Output,
    SolidExecutionContext,
    TableColumn,
    TableSchema,
)
from dagster import _check as check
from dagster import get_dagster_logger, op
from dagster.core.definitions.metadata import RawMetadataValue


def _load_manifest_for_project(
    project_dir: str, profiles_dir: str, target_dir: str, select: str
) -> Tuple[Mapping[str, Any], DbtCliOutput]:
    # running "dbt ls" regenerates the manifest.json, which includes a superset of the actual
    # "dbt ls" output
    cli_output = execute_cli(
        executable="dbt",
        command="ls",
        log=get_dagster_logger(),
        flags_dict={
            "project-dir": project_dir,
            "profiles-dir": profiles_dir,
            "select": select,
            "resource-type": "model",
            "output": "json",
        },
        warn_error=False,
        ignore_handled_error=False,
        target_path=target_dir,
    )
    manifest_path = os.path.join(target_dir, "manifest.json")
    with open(manifest_path, "r", encoding="utf8") as f:
        return json.load(f), cli_output


def _get_node_name(node_info: Mapping[str, Any]):
    return "__".join([node_info["resource_type"], node_info["package_name"], node_info["name"]])


def _get_node_asset_key(node_info):
    if node_info.get("schema") is not None:
        components = [node_info["schema"], node_info["name"]]
    else:
        components = [node_info["name"]]

    return AssetKey(components)


def _get_node_description(node_info):
    code_block = textwrap.indent(node_info["raw_sql"], "    ")
    description_sections = [
        node_info["description"],
        f"#### Raw SQL:\n```\n{code_block}\n```",
    ]
    return "\n\n".join(filter(None, description_sections))


def _dbt_nodes_to_assets(
    dbt_nodes: Mapping[str, Any],
    select: str,
    selected_unique_ids: AbstractSet[str],
    runtime_metadata_fn: Optional[
        Callable[[SolidExecutionContext, Mapping[str, Any]], Mapping[str, RawMetadataValue]]
    ] = None,
    io_manager_key: Optional[str] = None,
    node_info_to_asset_key: Callable[[Mapping[str, Any]], AssetKey] = _get_node_asset_key,
    use_build_command: bool = False,
) -> AssetsDefinition:

    outs: Dict[str, Out] = {}
    asset_ins: Dict[AssetKey, Tuple[str, In]] = {}

    asset_deps: Dict[AssetKey, Set[AssetKey]] = {}

    out_name_to_node_info: Dict[str, Mapping[str, Any]] = {}

    package_name = None
    for unique_id in selected_unique_ids:
        cur_asset_deps = set()
        node_info = dbt_nodes[unique_id]
        package_name = node_info.get("package_name", package_name)

        for dep_name in node_info["depends_on"]["nodes"]:
            dep_type = dbt_nodes[dep_name]["resource_type"]

            # ignore seeds/snapshots
            if dep_type not in ["source", "model"]:
                continue
            dep_asset_key = node_info_to_asset_key(dbt_nodes[dep_name])

            # if it's a source, it will be used as an input to this multi-asset
            if dep_type == "source":
                asset_ins[dep_asset_key] = (dep_name.replace(".", "_"), In(Nothing))

            # regardless of type, list this as a dependency for the current asset
            cur_asset_deps.add(dep_asset_key)

        # generate the Out that corresponds to this model
        node_name = node_info["name"]
        outs[node_name] = Out(
            description=_get_node_description(node_info),
            io_manager_key=io_manager_key,
            metadata=_columns_to_metadata(node_info["columns"]),
            is_required=False,
        )
        out_name_to_node_info[node_name] = node_info

        # set the asset dependencies for this asset
        asset_deps[node_info_to_asset_key(node_info)] = cur_asset_deps

    # prevent op name collisions between multiple dbt multi-assets
    op_name = f"run_dbt_{package_name}"
    if select != "*":
        op_name += "_" + hashlib.md5(select.encode()).hexdigest()[-5:]

    @op(
        name=op_name,
        tags={"kind": "dbt"},
        ins=dict(asset_ins.values()),
        out=outs,
        required_resource_keys={"dbt"},
    )
    def dbt_op(context):
        dbt_output = None
        try:
            # in the case that we're running everything, opt for the cleaner selection string
            if len(context.selected_output_names) == len(outs):
                subselect = select
            else:
                # for each output that we want to emit, translate to a dbt select string by converting
                # the out to it's corresponding fqn
                subselect = [
                    ".".join(out_name_to_node_info[out_name]["fqn"])
                    for out_name in context.selected_output_names
                ]

            if use_build_command:
                dbt_output = context.resources.dbt.build(select=subselect)
            else:
                dbt_output = context.resources.dbt.run(select=subselect)

        finally:
            # in the case that the project only partially runs successfully, still attempt to generate
            # events for the parts that were successful
            if dbt_output is None:
                dbt_output = DbtOutput(result=context.resources.dbt.get_run_results_json())

            # yield an Output for each materialization generated in the run
            for event in generate_events(
                dbt_output,
                node_info_to_asset_key=node_info_to_asset_key,
                manifest_json=context.resources.dbt.get_manifest_json(),
            ):
                # convert AssetMaterializations to outputs
                if isinstance(event, AssetMaterialization):
                    output_name = event.asset_key.path[-1]
                    if runtime_metadata_fn:
                        yield Output(
                            value=None,
                            output_name=output_name,
                            metadata=runtime_metadata_fn(
                                context, out_name_to_node_info[output_name]
                            ),
                        )
                    else:
                        yield Output(
                            value=None,
                            output_name=output_name,
                            metadata_entries=event.metadata_entries,
                        )
                # yield AssetObservations normally
                else:
                    yield event

    return AssetsDefinition(
        asset_keys_by_input_name={
            input_name: asset_key for asset_key, (input_name, _) in asset_ins.items()
        },
        asset_keys_by_output_name={
            output_name: node_info_to_asset_key(out_name_to_node_info[output_name])
            for output_name in outs.keys()
        },
        node_def=dbt_op,
        can_subset=True,
        asset_deps=asset_deps,
    )


def _columns_to_metadata(columns: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    return (
        {
            "schema": MetadataValue.table_schema(
                TableSchema(
                    columns=[
                        TableColumn(
                            name=name,
                            type=metadata.get("data_type") or "?",
                            description=metadata.get("description"),
                        )
                        for name, metadata in columns.items()
                    ]
                )
            )
        }
        if len(columns) > 0
        else None
    )


def load_assets_from_dbt_project(
    project_dir: str,
    profiles_dir: Optional[str] = None,
    target_dir: Optional[str] = None,
    select: Optional[str] = None,
    runtime_metadata_fn: Optional[
        Callable[[SolidExecutionContext, Mapping[str, Any]], Mapping[str, Any]]
    ] = None,
    io_manager_key: Optional[str] = None,
    node_info_to_asset_key: Callable[[Mapping[str, Any]], AssetKey] = _get_node_asset_key,
    use_build_command: bool = False,
) -> Sequence[AssetsDefinition]:
    """
    Loads a set of DBT models from a DBT project into Dagster assets.

    Creates one Dagster asset for each dbt model. All assets will be re-materialized using a single
    `dbt run` command.

    Args:
        project_dir (Optional[str]): The directory containing the DBT project to load.
        profiles_dir (Optional[str]): The profiles directory to use for loading the DBT project.
            Defaults to a directory called "config" inside the project_dir.
        target_dir (Optional[str]): The target directory where DBT will place compiled artifacts.
            Defaults to "target" underneath the project_dir.
        select (str): A DBT selection string for the models in a project that you want to include.
            Defaults to "*".
        runtime_metadata_fn: (Optional[Callable[[SolidExecutionContext, Mapping[str, Any]], Mapping[str, Any]]]):
            A function that will be run after any of the assets are materialized and returns
            metadata entries for the asset, to be displayed in the asset catalog for that run.
        io_manager_key (Optional[str]): The IO manager key that will be set on each of the returned
            assets. When other ops are downstream of the loaded assets, the IOManager specified
            here determines how the inputs to those ops are loaded. Defaults to "io_manager".
        node_info_to_asset_key: (Mapping[str, Any] -> AssetKey): A function that takes a dictionary
            of dbt node info and returns the AssetKey that you want to represent that node. By
            default, the asset key will simply be the name of the dbt model.
        use_build_command: (bool): Flag indicating if you want to use `dbt build` as the core computation
            for this asset, rather than `dbt run`.

    """
    check.str_param(project_dir, "project_dir")
    profiles_dir = check.opt_str_param(
        profiles_dir, "profiles_dir", os.path.join(project_dir, "config")
    )
    target_dir = check.opt_str_param(target_dir, "target_dir", os.path.join(project_dir, "target"))

    manifest_json, cli_output = _load_manifest_for_project(
        project_dir, profiles_dir, target_dir, select or "*"
    )
    selected_unique_ids: Set[str] = set(
        filter(None, (line.get("unique_id") for line in cli_output.logs))
    )

    dbt_nodes = {**manifest_json["nodes"], **manifest_json["sources"]}
    return [
        _dbt_nodes_to_assets(
            dbt_nodes,
            select=select or "*",
            selected_unique_ids=selected_unique_ids,
            runtime_metadata_fn=runtime_metadata_fn,
            io_manager_key=io_manager_key,
            node_info_to_asset_key=node_info_to_asset_key,
            use_build_command=use_build_command,
        ),
    ]


def load_assets_from_dbt_manifest(
    manifest_json: Mapping[str, Any],
    runtime_metadata_fn: Optional[
        Callable[[SolidExecutionContext, Mapping[str, Any]], Mapping[str, Any]]
    ] = None,
    io_manager_key: Optional[str] = None,
    selected_unique_ids: Optional[AbstractSet[str]] = None,
    node_info_to_asset_key: Callable[[Mapping[str, Any]], AssetKey] = _get_node_asset_key,
    use_build_command: bool = False,
) -> Sequence[AssetsDefinition]:
    """
    Loads a set of dbt models, described in a manifest.json, into Dagster assets.

    Creates one Dagster asset for each dbt model. All assets will be re-materialized using a single
    `dbt run` command.

    Args:
        manifest_json (Optional[Mapping[str, Any]]): The contents of a DBT manifest.json, which contains
            a set of models to load into assets.
        runtime_metadata_fn: (Optional[Callable[[SolidExecutionContext, Mapping[str, Any]], Mapping[str, Any]]]):
            A function that will be run after any of the assets are materialized and returns
            metadata entries for the asset, to be displayed in the asset catalog for that run.
        io_manager_key (Optional[str]): The IO manager key that will be set on each of the returned
            assets. When other ops are downstream of the loaded assets, the IOManager specified
            here determines how the inputs to those ops are loaded. Defaults to "io_manager".
        selected_unique_ids (Optional[Set[str]]): The set of dbt unique_ids that you want to load
            as assets.
        node_info_to_asset_key: (Mapping[str, Any] -> AssetKey): A function that takes a dictionary
            of dbt node info and returns the AssetKey that you want to represent that node. By
            default, the asset key will simply be the name of the dbt model.
        use_build_command: (bool): Flag indicating if you want to use `dbt build` as the core computation
            for this asset, rather than `dbt run`.
    """
    check.dict_param(manifest_json, "manifest_json", key_type=str)
    dbt_nodes = {**manifest_json["nodes"], **manifest_json["sources"]}

    def _unique_id_to_selector(uid):
        # take the fully-qualified node name and use it to select the model
        return ".".join(dbt_nodes[uid]["fqn"])

    select = (
        "*"
        if selected_unique_ids is None
        else " ".join(_unique_id_to_selector(uid) for uid in selected_unique_ids)
    )
    selected_unique_ids = selected_unique_ids or set(
        unique_id
        for unique_id, node_info in dbt_nodes.items()
        if node_info["resource_type"] == "model"
    )
    return [
        _dbt_nodes_to_assets(
            dbt_nodes,
            runtime_metadata_fn=runtime_metadata_fn,
            io_manager_key=io_manager_key,
            select=select,
            selected_unique_ids=selected_unique_ids,
            node_info_to_asset_key=node_info_to_asset_key,
            use_build_command=use_build_command,
        )
    ]
