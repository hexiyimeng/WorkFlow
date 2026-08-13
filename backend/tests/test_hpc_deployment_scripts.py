from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
HPC_ROOT = PROJECT_ROOT / "deploy" / "hpc"
CONTROL_PLANE = HPC_ROOT / "start_control_plane.sh"
CONTROL_PLANE_MANAGER = HPC_ROOT / "control_plane.sh"
CLIENT_TUNNEL = HPC_ROOT / "open_workflow_tunnel.ps1"
EXECUTION_JOB = HPC_ROOT / "slurm" / "workflow_execution.sbatch"
MULTI_WORKER_JOB = HPC_ROOT / "slurm" / "multi_worker_smoke.sbatch"
MULTI_WORKER_SUBMIT = HPC_ROOT / "submit_multi_worker_smoke.sh"
INTEGRATION_SMOKE = HPC_ROOT / "integration_smoke.py"
INSTALL = HPC_ROOT / "install.sh"


def _text(path: Path) -> str:
    content = path.read_bytes()
    assert b"\r\n" not in content, f"Linux deployment script has CRLF: {path}"
    return content.decode("utf-8")


def test_control_plane_is_not_submitted_as_a_slurm_job() -> None:
    script = _text(CONTROL_PLANE)

    assert "WorkFlow_EXECUTION_BACKEND=\"slurm\"" in script
    assert "WorkFlow_CUDA_MODE=\"disabled\"" in script
    assert "exec \"$PYTHON\" -m uvicorn" in script
    assert "sbatch" not in script.split("exec \"$PYTHON\" -m uvicorn", 1)[1]
    assert "dask_service" not in script
    for command in (
        "SBATCH_COMMAND",
        "SQUEUE_COMMAND",
        "SACCT_COMMAND",
        "SCONTROL_COMMAND",
        "SCANCEL_COMMAND",
    ):
        assert command in script
    assert '[[ -v WorkFlow_SLURM_SACCT && -z "$WorkFlow_SLURM_SACCT" ]]' in script
    assert 'SACCT_COMMAND=""' in script


def test_control_plane_manager_persists_only_the_web_control_plane() -> None:
    script = _text(CONTROL_PLANE_MANAGER)

    assert "tmux new-session" in script
    assert "start_control_plane.sh" in script
    assert "sbatch" not in script
    assert "dask" not in script.lower()
    assert "kill-session" in script
    assert "WORKFLOW_RUNTIME_DIR/logs/control-plane.log" in script
    assert '"WORKFLOW_ROOT=$WORKFLOW_ROOT"' in script
    assert '"WORKFLOW_RUNTIME_DIR=$WORKFLOW_RUNTIME_DIR"' in script
    assert "WorkFlow_SLURM_PARTITION" in script
    assert "WorkFlow_SLURM_MAX_GPUS" in script
    assert "-u WORKFLOW_ROOT" in script
    assert "-u WORKFLOW_RUNTIME_DIR" in script
    assert 'launch+=(-u "$variable_name")' in script
    assert 'launch+=("$variable_name=${!variable_name}")' in script
    assert 'READY_URL="http://127.0.0.1:$WEB_PORT/plugin_status"' in script
    assert 'curl --silent --fail --max-time 2 --output /dev/null "$READY_URL"' in script
    assert 'did not become HTTP-ready within 60 seconds' in script
    assert script.index('launch+=(-u "$variable_name")') < script.index(
        'launch+=("$variable_name=${!variable_name}")'
    )


def test_client_tunnel_keeps_both_ends_on_loopback() -> None:
    script = CLIENT_TUNNEL.read_text(encoding="utf-8")

    assert "127.0.0.1:${LocalPort}:127.0.0.1:${RemotePort}" in script
    assert "ExitOnForwardFailure=yes" in script
    assert "ServerAliveInterval=30" in script
    assert "Start-Process $url" in script
    assert "password or private key is read or stored" in script


def test_execution_job_has_no_fixed_graph_resource_request() -> None:
    script = _text(EXECUTION_JOB)
    directives = tuple(
        line.strip()
        for line in script.splitlines()
        if line.lstrip().startswith("#SBATCH")
    )

    assert "#SBATCH --nodes=1" in directives
    assert "#SBATCH --ntasks=1" in directives
    assert not any("--cpus-per-task" in line for line in directives)
    assert not any("--mem" in line for line in directives)
    assert not any("--gres" in line for line in directives)
    assert not any("--time" in line for line in directives)
    assert "WorkFlow_EXECUTION_BACKEND=\"local\"" in script
    assert "if (( $# != 7 ))" in script
    assert "export WorkFlow_SLURM_SQUEUE=\"$SQUEUE_PATH\"" in script
    assert "export WorkFlow_SLURM_SACCT=\"$SACCT_PATH\"" in script
    assert "export WorkFlow_SLURM_SCONTROL=\"$SCONTROL_PATH\"" in script
    assert 'if [[ "$SACCT_PATH" == "-" ]]' in script
    assert '[[ ! -f "$scheduler_command" || ! -x "$scheduler_command" ]]' in script
    assert "exec \"$PYTHON\" -m services.slurm_execution_runner" in script


def test_legacy_fixed_server_submission_entrypoints_are_removed() -> None:
    assert not (HPC_ROOT / "submit.sh").exists()
    assert not (HPC_ROOT / "slurm" / "workflow_server.sbatch").exists()


def test_multi_worker_smoke_submits_dynamic_single_node_resources() -> None:
    job = _text(MULTI_WORKER_JOB)
    submit = _text(MULTI_WORKER_SUBMIT)
    directives = tuple(
        line.strip()
        for line in job.splitlines()
        if line.lstrip().startswith("#SBATCH")
    )

    assert "#SBATCH --nodes=1" in directives
    assert "#SBATCH --ntasks=1" in directives
    assert not any("--cpus-per-task" in line for line in directives)
    assert not any("--mem" in line for line in directives)
    assert not any("--gres" in line for line in directives)
    assert "CPU_WORKERS + GPU_WORKERS < 2" in job
    assert "multi_worker_smoke.py" in job
    assert "--cpus-per-task=\"$ALLOCATED_CPUS\"" in submit
    assert "--mem=\"${ALLOCATED_MEMORY_GIB}G\"" in submit
    assert 'SBATCH_ARGS+=(--gres="gpu:$GPU_WORKERS")' in submit
    assert "--export=NONE" in submit


def test_integration_smoke_uses_practical_windows() -> None:
    script = _text(INTEGRATION_SMOKE)

    assert '"windowShape": [1, 1]' in script
    assert "one token element per source Dask block" in script


def test_install_validates_the_prebuilt_frontend_without_node() -> None:
    script = _text(INSTALL)

    assert "validate_frontend_dist.py" in script
    assert '"$WORKFLOW_ROOT/backend/dist"' in script
    assert "npm " not in script
    assert "node " not in script
