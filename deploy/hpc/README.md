# WorkFlow on the Xili Slurm cluster

This deployment runs the existing WorkFlow-owned Dask `SpecCluster` inside one
Slurm allocation. It does not start compute on `mn02`, and it does not pretend
that the current backend is a multi-node Dask deployment.

Default locations:

- code: `$HOME/apps/WorkFlow`
- runtime data, models, outputs and recovery: `$HOME/workflow-runtime`
- uv binary and install logs: `$HOME/.workflow-deploy`
- Dask spill: the allocated node's `$SLURM_TMPDIR`, or a job-specific `/tmp`

Install or update from the login node:

```bash
bash "$HOME/apps/WorkFlow/deploy/hpc/install.sh"
```

Submit the server (defaults: `compute`, 1 GPU, 8 CPUs, 128 GiB, one day):

```bash
bash "$HOME/apps/WorkFlow/deploy/hpc/submit.sh"
```

Override requested resources without editing the script:

```bash
WORKFLOW_GPUS=1 WORKFLOW_CPUS=12 WORKFLOW_MEMORY=192G \
  bash "$HOME/apps/WorkFlow/deploy/hpc/submit.sh"
```

Read `$HOME/workflow-runtime/state/current-server.env` after the job starts.
It records the allocated node and dynamic web/dashboard ports. Use the SSH
tunnel printed in the Slurm log, then open the local WorkFlow URL.

GPU smoke test:

```bash
mkdir -p "$HOME/workflow-runtime/logs"
sbatch \
  --output="$HOME/workflow-runtime/logs/gpu-smoke-%j.log" \
  --chdir="$HOME/apps/WorkFlow" \
  "$HOME/apps/WorkFlow/deploy/hpc/slurm/gpu_smoke.sbatch"
```

End-to-end Window execution against a real microscopy image from the official
Cellpose demo archive:

```bash
bash "$HOME/apps/WorkFlow/deploy/hpc/prepare_test_data.sh"
sbatch \
  --output="$HOME/workflow-runtime/logs/integration-%j.log" \
  --chdir="$HOME/apps/WorkFlow" \
  "$HOME/apps/WorkFlow/deploy/hpc/slurm/integration_smoke.sbatch"
```

The integration test records the source archive hash/member, executes
OME-Zarr Reader -> Cellpose -> Zarr Writer + Parquet Writer in Window mode,
then verifies output bytes, Parquet rows, recovery manifest state, and the
one-byte-per-Window completion bitmap.

The server script intentionally does not set the legacy uniform
`WorkFlow_WORKER_MEMORY_LIMIT_GB=4`. CPU and GPU Dask Workers receive weighted
limits derived from the Slurm allocation, and the sum remains within the
application's 70% host-memory budget.
