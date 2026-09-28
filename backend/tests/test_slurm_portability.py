from pathlib import Path
import shlex
import sys

import pytest

from core.cluster_inventory import parse_scontrol_show_node
from core.resource_planner import plan_workflow_resources
from core.worker_pool import WorkerPool
from core.worker_profiles import PhysicalResources, WorkerProfile
from core.workflow_resources import build_workflow_resource_plan
from services.slurm_execution_service import slurm_policy_from_environment
from services.slurm_jobqueue_cluster import (
    BaselineSLURMJob, PlannedSLURMJob, SlurmJobSiteConfig,
    build_planned_slurm_worker_spec,
)


def test_partition_names_have_no_implicit_site_meaning():
    policy = slurm_policy_from_environment({})
    assert policy.resolve_partitions(("batch", "accelerated", "mn", "control")) == (
        "batch", "accelerated", "mn", "control",
    )
    restricted = slurm_policy_from_environment({
        "WorkFlow_SLURM_ALLOWED_PARTITIONS": "batch,accelerated",
    })
    assert restricted.resolve_partitions(("batch", "accelerated", "control")) == (
        "batch", "accelerated",
    )


def test_site_options_reach_baseline_and_elastic_scripts(tmp_path):
    class CpuNode:
        required_worker_profile = "CPU"

    workflow = build_workflow_resource_plan(
        {"cpu": {"type": "Cpu", "inputs": {}}}, ["cpu"],
        node_mappings={"Cpu": CpuNode},
    )
    profile = WorkerProfile(
        name="CPU", physical_resources=PhysicalResources(cpu=2, memory_gib=4),
        logical_resources={"CPU": 2}, threads=2,
    )
    inventory = parse_scontrol_show_node(
        "NodeName=other001 CPUTot=32 RealMemory=65536 Gres=(null) "
        "State=IDLE Partitions=batch\n"
    )
    plan = plan_workflow_resources(
        workflow, [profile],
        [WorkerPool(profile="CPU", processes=1, minimum_jobs=2, maximum_jobs=3)],
        inventory, partition="batch", time_limit="00:20:00",
    )
    setup = tmp_path / "site setup.sh"
    setup.write_text("export SITE_READY=1\n")
    site = SlurmJobSiteConfig.from_environment({
        "WorkFlow_SLURM_ACCOUNT": "research",
        "WorkFlow_SLURM_QOS": "normal",
        "WorkFlow_SLURM_RESERVATION": "experiment",
        "WorkFlow_SLURM_SRUN": "/opt/slurm/bin/srun",
        "WorkFlow_SLURM_WORKER_SETUP": str(setup),
    })
    specs = tuple(build_planned_slurm_worker_spec(
        plan, requirement, execution_id="test-portable", submission_token="wf:test:site",
        project_root=tmp_path, runtime_directory=tmp_path, run_directory=tmp_path,
        python_executable=Path(sys.executable), sbatch_executable="sbatch",
        scancel_executable="scancel", scheduler_host="service.example",
        scheduler_port=8786, protocol="tcp://", security=None,
        worker_port_range="20000:20100", nanny_port_range="21000:21100",
        site_config=site,
    ) for requirement in plan.jobs)
    baseline = BaselineSLURMJob(
        "tcp://service.example:8786", name="baseline", component_specs=specs,
        **dict(specs[0].options),
    ).job_script()
    elastic = PlannedSLURMJob(
        "tcp://service.example:8786", name="elastic", **dict(specs[0].options),
    ).job_script()
    for script, count in ((baseline, 2), (elastic, 1)):
        assert script.count("#SBATCH -A research") == count
        assert script.count("#SBATCH --qos=normal") == count
        assert script.count("#SBATCH --reservation=experiment") == count
        assert script.count("#SBATCH -p batch") == count
        assert "SITE_READY" not in script
    source_line = f"source {shlex.quote(str(setup))}"
    assert source_line in elastic
    assert baseline.index(source_line) < baseline.index("/opt/slurm/bin/srun")
    assert "/opt/slurm/bin/srun --het-group=0" in baseline
    assert "/opt/slurm/bin/srun --het-group=1" in baseline


@pytest.mark.parametrize("name", ["ACCOUNT", "QOS", "RESERVATION"])
def test_site_directive_cannot_inject_another_option(name):
    with pytest.raises(ValueError, match="single safe name"):
        SlurmJobSiteConfig.from_environment({f"WorkFlow_SLURM_{name}": "normal\n#SBATCH --exclusive"})


def test_setup_script_must_exist_on_submission_host(tmp_path):
    with pytest.raises(ValueError, match="existing absolute"):
        SlurmJobSiteConfig(setup_script=str(tmp_path / "missing.sh"))
