from __future__ import annotations

from pathlib import Path

import pytest

from core.slurm_execution import (
    SlurmPolicy,
    SlurmResourceRequest,
    build_sbatch_argv,
    parse_sbatch_job_id,
    parse_sbatch_submission,
    resolve_execution_directory,
    validate_execution_id,
)


def _policy(**overrides: object) -> SlurmPolicy:
    values: dict[str, object] = {
        "partition": "gpu-compute",
        "time_limit": "1-00:00:00",
        "base_cpus": 1,
        "base_memory_gib": 2,
        "cpus_per_cpu_worker": 1,
        "cpus_per_gpu_worker": 3,
        "memory_gib_per_cpu_worker": 4,
        "memory_gib_per_gpu_worker": 24,
        "max_cpu_workers": 32,
        "max_gpu_workers": 8,
        "max_cpus": 64,
        "max_gpus": 8,
        "max_memory_gib": 256,
        "allowed_partitions": ("gpu-compute", "short"),
    }
    values.update(overrides)
    return SlurmPolicy(**values)  # type: ignore[arg-type]


def test_policy_calculates_one_node_request_from_worker_counts() -> None:
    request = _policy().resource_request(cpu_workers=2, gpu_workers=3)

    assert request == SlurmResourceRequest(
        cpu_workers=2,
        gpu_workers=3,
        nodes=1,
        cpus=12,
        gpus=3,
        memory_gib=82,
        time_limit="1-00:00:00",
        partition="gpu-compute",
    )
    assert request.time == "1-00:00:00"
    assert request.to_dict()["memoryGiB"] == 82


def test_policy_permits_cpu_only_and_gpu_only_requests() -> None:
    cpu_request = _policy().resource_request(cpu_workers=2, gpu_workers=0)
    gpu_request = _policy().resource_request(cpu_workers=0, gpu_workers=2)

    assert (cpu_request.cpus, cpu_request.gpus, cpu_request.memory_gib) == (3, 0, 10)
    assert (gpu_request.cpus, gpu_request.gpus, gpu_request.memory_gib) == (7, 2, 50)


def test_resource_request_rejects_too_few_cpu_slots_for_workers() -> None:
    with pytest.raises(ValueError, match="one CPU slot"):
        SlurmResourceRequest(
            cpu_workers=2,
            gpu_workers=1,
            nodes=1,
            cpus=2,
            gpus=1,
            memory_gib=32,
            time_limit="01:00:00",
            partition="compute",
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("base_cpus", 0),
        ("cpus_per_cpu_worker", 0),
        ("cpus_per_gpu_worker", -1),
        ("memory_gib_per_cpu_worker", True),
        ("max_cpus", 0),
        ("max_memory_gib", 0),
    ],
)
def test_policy_rejects_non_positive_integer_fields(field: str, value: object) -> None:
    with pytest.raises(ValueError, match=field):
        _policy(**{field: value})


@pytest.mark.parametrize(
    ("cpu_workers", "gpu_workers", "message"),
    [
        (-1, 0, "cpu_workers"),
        (0, -1, "gpu_workers"),
        (True, 0, "cpu_workers"),
        (0, 0, "At least one"),
        (33, 0, "cpu_workers=33"),
        (0, 9, "gpu_workers=9"),
    ],
)
def test_request_rejects_invalid_or_excess_worker_counts(
    cpu_workers: object,
    gpu_workers: object,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _policy().resource_request(  # type: ignore[arg-type]
            cpu_workers=cpu_workers,
            gpu_workers=gpu_workers,
        )


def test_request_enforces_calculated_cpu_memory_and_gpu_limits() -> None:
    with pytest.raises(ValueError, match="Calculated cpus"):
        _policy(max_cpus=6).resource_request(cpu_workers=3, gpu_workers=1)
    with pytest.raises(ValueError, match="Calculated memory_gib"):
        _policy(max_memory_gib=29).resource_request(cpu_workers=1, gpu_workers=1)
    with pytest.raises(ValueError, match="max_gpu_workers exceeds max_gpus"):
        _policy(max_gpus=1)


def test_partition_is_syntax_checked_and_allowlisted() -> None:
    assert (
        _policy().resource_request(
            cpu_workers=1,
            gpu_workers=0,
            partition="short",
        ).partition
        == "short"
    )
    with pytest.raises(ValueError, match="not allowed"):
        _policy().resource_request(
            cpu_workers=1,
            gpu_workers=0,
            partition="other",
        )
    with pytest.raises(ValueError, match="partition"):
        _policy(partition="--wrap=bad")


@pytest.mark.parametrize("time_limit", ["", "0", "1:99", "1-24:00", "UNLIMITED"])
def test_policy_rejects_invalid_or_unbounded_time(time_limit: str) -> None:
    with pytest.raises(ValueError, match="time_limit"):
        _policy(time_limit=time_limit)


def test_sbatch_argv_is_explicit_and_slurm_19_05_compatible(tmp_path: Path) -> None:
    request = _policy().resource_request(cpu_workers=2, gpu_workers=3)
    script = tmp_path / "run execution.sh"
    output = tmp_path / "slurm-%j.log"
    workdir = tmp_path / "work dir"
    request_file = tmp_path / "run;still-one-argument.json"

    argv = build_sbatch_argv(
        request,
        script_path=script,
        job_name="workflow-abc_123",
        output_path=output,
        work_directory=workdir,
        script_arguments=(request_file,),
    )

    assert argv[0] == "sbatch"
    assert "--parsable" in argv
    assert "--export=NONE" in argv
    assert "--nodes=1" in argv
    assert "--ntasks=1" in argv
    assert "--cpus-per-task=12" in argv
    assert "--mem=82G" in argv
    assert "--time=1-00:00:00" in argv
    assert "--gres=gpu:3" in argv
    assert "--gpus=3" not in argv
    assert argv[-2:] == (str(script), str(request_file))
    # Whitespace and shell metacharacters remain inside individual argv items.
    assert str(script) in argv
    assert str(request_file) in argv


def test_zero_gpu_request_has_no_gpu_allocation_flag(tmp_path: Path) -> None:
    request = _policy().resource_request(cpu_workers=1, gpu_workers=0)

    argv = build_sbatch_argv(
        request,
        script_path=tmp_path / "run.sh",
        job_name="workflow-cpu",
        output_path=tmp_path / "%j.log",
    )

    assert not any(argument.startswith("--gres=") for argument in argv)
    assert not any(argument.startswith("--gpus") for argument in argv)


def test_sbatch_argv_rejects_option_injection_and_relative_paths(tmp_path: Path) -> None:
    request = _policy().resource_request(cpu_workers=1, gpu_workers=0)
    with pytest.raises(ValueError, match="job_name"):
        build_sbatch_argv(
            request,
            script_path=tmp_path / "run.sh",
            job_name="ok\n--wrap=bad",
            output_path=tmp_path / "%j.log",
        )
    with pytest.raises(ValueError, match="script_path must be an absolute"):
        build_sbatch_argv(
            request,
            script_path="run.sh",
            job_name="workflow-test",
            output_path=tmp_path / "%j.log",
        )


def test_sbatch_argv_adds_only_valid_optional_comment(tmp_path: Path) -> None:
    request = _policy().resource_request(cpu_workers=1, gpu_workers=0)
    common = {
        "script_path": tmp_path / "run.sh",
        "output_path": tmp_path / "%j.log",
        "job_name": "workflow-test",
    }
    argv = build_sbatch_argv(
        request,
        **common,
        comment="wf:abcdef:0123456789",
    )
    assert "--comment=wf:abcdef:0123456789" in argv

    without_comment = build_sbatch_argv(request, **common)
    assert not any(item.startswith("--comment=") for item in without_comment)

    for unsafe in ("bad comment", "bad\ncomment", "--gres=gpu:8", "x" * 129):
        with pytest.raises(ValueError, match="comment"):
            build_sbatch_argv(request, **common, comment=unsafe)


@pytest.mark.parametrize(
    ("output", "job_id", "cluster"),
    [
        ("12345\n", "12345", None),
        ("98765;cluster_1\n", "98765", "cluster_1"),
        (b"42;alpha-beta\n", "42", "alpha-beta"),
    ],
)
def test_parse_sbatch_parsable_output(
    output: str | bytes,
    job_id: str,
    cluster: str | None,
) -> None:
    submission = parse_sbatch_submission(output)
    assert submission.job_id == job_id
    assert submission.cluster == cluster
    assert parse_sbatch_job_id(output) == job_id


@pytest.mark.parametrize(
    "output",
    ["", "0", "Submitted batch job 123", "123;bad/cluster", "123\n456", "abc"],
)
def test_parse_sbatch_rejects_ambiguous_output(output: str) -> None:
    with pytest.raises(ValueError, match="Invalid sbatch"):
        parse_sbatch_job_id(output)


@pytest.mark.parametrize(
    "execution_id",
    [
        "",
        ".",
        "..",
        ".hidden",
        "../escape",
        "nested/name",
        "nested\\name",
        "bad id",
        "CON",
        "a" * 129,
    ],
)
def test_execution_id_rejects_unsafe_path_values(execution_id: str) -> None:
    with pytest.raises(ValueError):
        validate_execution_id(execution_id)


def test_execution_directory_is_a_direct_child_of_fixed_root(tmp_path: Path) -> None:
    root = tmp_path / "executions"
    root.mkdir()
    expected = root / "48f04e27-47d2-4a52-b2a2-87383464ab2a"

    assert resolve_execution_directory(root, expected.name) == expected.resolve()
    with pytest.raises(ValueError, match="does not exist"):
        resolve_execution_directory(root, "missing", must_exist=True)

    expected.mkdir()
    assert resolve_execution_directory(root, expected.name, must_exist=True) == expected.resolve()


def test_execution_directory_rejects_file_and_symlink_targets(tmp_path: Path) -> None:
    root = tmp_path / "executions"
    root.mkdir()
    file_target = root / "ordinary-file"
    file_target.write_text("not a directory", encoding="utf-8")
    with pytest.raises(ValueError, match="not a directory"):
        resolve_execution_directory(root, file_target.name)

    outside = tmp_path / "outside"
    outside.mkdir()
    link = root / "linked-run"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"Creating directory symlinks is unavailable: {exc}")
    with pytest.raises(ValueError, match="symbolic link"):
        resolve_execution_directory(root, link.name)


def test_execution_directory_requires_an_absolute_real_root(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must be absolute"):
        resolve_execution_directory(Path("relative-root"), "run-1")

    outside = tmp_path / "outside"
    outside.mkdir()
    linked_root = tmp_path / "linked-root"
    try:
        linked_root.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"Creating directory symlinks is unavailable: {exc}")
    with pytest.raises(ValueError, match="symbolic link"):
        resolve_execution_directory(linked_root, "run-1")
