from types import SimpleNamespace

import pytest

from services.slurm_jobqueue_cluster import (
    BaselineSLURMJob, PlannedSlurmWorkerSpec, detect_heterogeneous_directive,
)


@pytest.mark.parametrize("version,directive,group", [
    ("slurm 19.05.7", "packjob", "pack-group"),
    ("slurm 20.02.0", "hetjob", "het-group"),
    ("slurm 25.11.1", "hetjob", "het-group"),
])
def test_detects_installed_syntax_and_generates_matching_steps(monkeypatch, version, directive, group):
    def run(argv, **kwargs):
        assert argv == ["/usr/bin/sbatch", "--version"]
        return SimpleNamespace(stdout=version)
    monkeypatch.setattr("services.slurm_jobqueue_cluster.subprocess.run", run)
    assert detect_heterogeneous_directive("/usr/bin/sbatch") == directive
    options = {"cores": 1, "memory": "4GiB", "processes": 1}
    specs = [PlannedSlurmWorkerSpec(f"CPU-{i}", "wf:test", options) for i in range(2)]
    job = BaselineSLURMJob("tcp://127.0.0.1:8786", name="base", component_specs=specs,
                          heterogeneous_directive=directive, **options)
    script = job.job_script()
    assert f"#SBATCH {directive}\n" in script
    assert f"--{group}=0" in script and f"--{group}=1" in script


@pytest.mark.parametrize("version", ["slurm 16.05.0", "not a Slurm version"])
def test_unsupported_or_unknown_version_fails_before_submission(monkeypatch, version):
    monkeypatch.setattr("services.slurm_jobqueue_cluster.subprocess.run",
                        lambda *a, **kw: SimpleNamespace(stdout=version))
    with pytest.raises(RuntimeError):
        detect_heterogeneous_directive("sbatch")
