# WorkFlow 的 Slurm 部署方式

生产部署分为控制面和计算作业两部分。Uvicorn/FastAPI 是常驻的轻量控制面；
Dask Scheduler、Nanny 和 Worker 只在 Slurm 分配的 compute node 上启动。

```text
浏览器
  │ SSH 隧道
  ▼
登录/服务节点：Uvicorn 控制面（127.0.0.1）
  │ Graph JSON → 资源计划 → 动态 sbatch
  ▼
单个 compute node：execution runner → Dask Scheduler + CPU/GPU Workers
```

控制面不会预先申请固定的 CPU、内存或 GPU。每次正式执行前，后端根据该次
Graph JSON 的资源计划构造独立的 `sbatch` 请求。普通 Full Graph 与 Window
执行使用同一套作业隔离方式；恢复执行仍从恢复目录中的不可变图和配置启动。

## 集群前提与管理员许可

开始部署前应由集群管理员确认：

- 允许在登录节点长期运行轻量 Web 服务；如果禁止，应将控制面部署在指定的
  management/service node，或由管理员提供 systemd/容器服务。
- 控制面账号可以执行 `sbatch`、查询和取消自己提交的作业。
- 代码目录和 runtime 目录是 compute node 可见的共享绝对路径。
- Slurm GRES 已正确配置 GPU，并在作业内设置 `CUDA_VISIBLE_DEVICES`。
- 允许用户通过 SSH 将控制面的 loopback 端口转发到客户端。

控制面只监听 `127.0.0.1`，不应直接暴露到集群网络。这里的脚本不会生成、
安装或修改任何 SSH 密钥，也不需要从登录节点 SSH 到 compute node。

当前正式执行仍是**单节点 Dask**：一次 execution 的 Scheduler 和全部 Workers
位于同一个 Slurm compute node。`workflow_execution.sbatch` 固定的是单节点
拓扑（`--nodes=1 --ntasks=1`），但没有写死 CPU、GPU、内存或时限。若一个图
超出单节点容量，作业会等待合适节点或被 Slurm 拒绝；跨节点 Dask 尚未实现。

## 安装或更新

默认目录：

- 代码：`$HOME/apps/WorkFlow`
- 运行数据、模型、输出和恢复记录：`$HOME/workflow-runtime`
- Python 环境：`$HOME/apps/WorkFlow/backend/.venv`

`workflow-runtime` 是代码仓库之外、compute node 也能访问的共享运行目录。常见
子目录用途如下：

- `data/demo_images.zip`：仅是 `prepare_test_data.sh` 下载的 Cellpose 官方示例
  压缩包缓存，供端到端 smoke test 使用；它不是生产输入，也不是 Zarr 输出。
  测试脚本从压缩包读取 PNG，并在本次 `test-runs/<UUID>/` 中建立真正的 Zarr。
- `models/`：共享模型缓存。HPC 作业通过 `WorkFlow_MODELS_DIR` 使用这里的模型，
  因而模型不会随代码更新被删除，也不必在每个 compute node 或 `backend/` 下
  重复保存。Cellpose 的默认文件是 `models/cellpose/cpsam`。
- `test-runs/`：仅保存集成 smoke test 的一次性输入、Zarr/Parquet 输出、恢复记录
  和结果摘要；正常页面执行不会把生产输出自动放到这里。不再需要某次测试时，
  可在确认对应 Slurm 作业已经结束后删除选定的 UUID 子目录。
- `output/` 与 `recovery/`：正式执行可使用的默认输出/恢复根目录；不要把它们与
  `test-runs/` 一并清理。
- `requests/`、`jobs/`、`state/`、`logs/`：控制面与 compute job 之间的请求、
  状态、事件和日志记录。

在 Slurm submit host 上运行：

```bash
bash "$HOME/apps/WorkFlow/deploy/hpc/install.sh"
```

代码和 runtime 必须使用 compute node 也能访问的共享文件系统路径。安装过程会
核验仓库已经包含 `backend/dist/index.html` 以及该页面引用的所有本地静态资源；
集群节点不需要安装 Node.js，也不会在生产安装时临时构建前端。

## 启动控制面

脚本以前台方式启动，不会自行转入后台：

```bash
cd "$HOME/apps/WorkFlow"
WorkFlow_SLURM_PARTITION=compute \
  bash deploy/hpc/start_control_plane.sh
```

默认监听 `127.0.0.1:8000`。可配置项示例：

```bash
WORKFLOW_RUNTIME_DIR=/shared/song/workflow-runtime \
WORKFLOW_WEB_PORT=8000 \
WorkFlow_SLURM_PARTITION=compute \
WorkFlow_SLURM_TIME_LIMIT=1-00:00:00 \
  bash /shared/song/apps/WorkFlow/deploy/hpc/start_control_plane.sh
```

`start_control_plane.sh` 设置：

- `WorkFlow_EXECUTION_BACKEND=slurm`
- `WorkFlow_SLURM_EXECUTION_SCRIPT=<repo>/deploy/hpc/slurm/workflow_execution.sbatch`
- `WorkFlow_SLURM_RUNTIME_DIR=<runtime>`

partition、时限以及 CPU/GPU/内存上限应通过环境或管理员配置提供，而不是重新在
生产 `.sbatch` 文件里写死资源。当前版本使用提交账号的默认 account/QoS；若站点
强制要求显式 account 或 QoS，应先在后端增加经过 allowlist 校验的显式策略参数，
不要通过 `SBATCH_*` 环境变量绕过资源策略。需要长期常驻时，可在
管理员许可下使用 tmux 或由管理员创建服务；脚本本身始终保持前台生命周期。

管理员允许用户级 `tmux` 常驻时，可以用只管理 Web 控制面的辅助脚本；它不会申请
Slurm 资源，也不会在登录节点启动 Dask：

```bash
bash "$HOME/apps/WorkFlow/deploy/hpc/control_plane.sh" start
bash "$HOME/apps/WorkFlow/deploy/hpc/control_plane.sh" status
bash "$HOME/apps/WorkFlow/deploy/hpc/control_plane.sh" logs
```

修改了 Slurm 策略环境变量后，应执行 `control_plane.sh restart`，使新的控制面进程读取
这些设置。若站点禁止登录节点常驻进程，必须改由管理员批准的 service node 或系统服务
托管，不能用 `tmux` 绕过站点规定。

## 从客户端访问

先在客户端按照集群指南连接 OpenVPN，再建立 SSH 本地端口转发。VPN 配置文件、
私钥和口令都属于敏感凭据，不要复制到仓库、日志或聊天中；不用页面时应关闭
VPN。以下是西丽集群管理/登录节点的示例（替换实际账号）：

```bash
ssh -N \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -L 127.0.0.1:8000:127.0.0.1:8000 \
  cluster-user@10.200.201.2
```

然后打开 `http://127.0.0.1:8000`。如果设置了其他 `WORKFLOW_WEB_PORT`，隧道
远端端口必须一致；本地端口可以换成空闲端口，例如使用
`-L 127.0.0.1:18000:127.0.0.1:8000` 后访问 `http://127.0.0.1:18000`。

不要把 `-L` 的本地监听地址或 Uvicorn 的监听地址改成 `0.0.0.0`。这套服务目前
不是可以直接公开到互联网的多用户认证网关。如果需要多人长期访问，应由集群
管理员在 service node 前部署带 TLS 和身份认证的反向代理，而不是开放 8000
端口。直接运行 `python backend/main.py` 时也默认监听 `127.0.0.1:8000`；只有
经过安全评审的部署才应显式设置 `WORKFLOW_WEB_HOST` 和 `WORKFLOW_WEB_PORT`。

## 每次执行时发生什么

1. 控制面读取当前 Graph JSON，并完成只读 preflight 和资源分析。
2. 后端在共享 runtime 中原子写入本次 execution request。
3. 后端用计算出的 CPU/GPU/内存参数调用 `sbatch --export=NONE`，并将 request
   文件和 runtime 目录作为绝对位置参数传给
   `slurm/workflow_execution.sbatch`。
4. Slurm 分配 compute node 后，脚本设置模型目录、作业级 Dask scratch 和
   Slurm 管理的 CUDA 可见性，并将执行后端明确切回 `local` 以防止嵌套提交，
   然后启动
   `python -m services.slurm_execution_runner`。
5. runner 在 compute node 内建立本地 Dask Scheduler/Workers，执行结束后退出，
   Slurm 随即释放资源。控制面不会在登录节点建立 Dask 集群。

`workflow_execution.sbatch` 应只由后端提交。手工调试时也必须由 `sbatch`
动态提供资源，并保证工作目录是仓库根目录，例如：

```bash
sbatch --export=NONE \
  --chdir="$HOME/apps/WorkFlow" \
  --cpus-per-task=8 --mem=128G --gres=gpu:1 --time=01:00:00 \
  "$HOME/apps/WorkFlow/deploy/hpc/slurm/workflow_execution.sbatch" \
  /shared/song/workflow-runtime/jobs/example-execution/request.json \
  /shared/song/workflow-runtime \
  8 64 \
  "$(command -v squeue)" \
  "$(command -v sacct || printf '%s' '-')" \
  "$(command -v scontrol)"
```

上面的资源和 Worker 内存数字只是一次手工调试请求，不是生产默认值。正常
运行时由 Graph 资源计划与管理员策略生成这些参数。CPU/GPU Worker 内存上限
会显式传入 compute job，避免 Dask 误把整台物理节点内存当成本次 allocation
可用内存。

`sacct` 是可选的终态历史来源。旧集群未运行 `slurmdbd` 时，控制面不会因
`sacct: Connection refused` 而停止：它优先读取 Slurm 19.05 已支持的
`scontrol show job -o` 根作业记录。若控制器已按 `MinJobAge` 清除了记录，
则只在两次精确 `squeue` 查询均成功且持续找不到同一根 job、同时没有 runner
的原子 `result.json` 后，才把作业判为丢失并失败结束。任何命令错误、错误 job
编号、歧义输出或明确的非终态记录都会继续保持 active，避免误回收正在运行的
Window recovery lock。

## Graph 如何决定 Worker 和 Slurm 资源

资源分析只遍历 terminal output 可达的节点。每个节点类型通过
`EXECUTION_RESOURCE` 声明 `cpu`、`gpu` 或 `any`，并通过
`EXECUTION_WORKERS` 向相应共享 Worker 池贡献数量：

```text
cpuWorkers = 所有可达 cpu/any 节点贡献数量之和
gpuWorkers = 所有可达 gpu 节点贡献数量之和
Slurm GPU  = gpuWorkers（每个 GPU Worker 只看一张获配 GPU）
Slurm CPU  = base + cpuWorkers × cpu配额 + gpuWorkers × gpu配额
Slurm 内存 = base + cpuWorkers × cpu内存 + gpuWorkers × gpu内存
```

因此，当前一个 Cellpose 节点声明 1 个 GPU Worker，它不会仅因为节点上物理
存在 8 张卡就自动申请 8 张卡；只有 Graph 资源计划实际得到 8 个 GPU Worker
时才会生成 `--gres=gpu:8`。`maxInFlightWindows` 是执行背压设置，不会偷偷改变
Slurm 资源申请。这样可以避免普通小图无条件占满整台 GPU 节点。

资源换算属于管理员策略，不由 Graph 注入任意 `sbatch` 参数。主要环境项为：

```text
WorkFlow_SLURM_PARTITION
WorkFlow_SLURM_TIME_LIMIT
WorkFlow_SLURM_BASE_CPUS
WorkFlow_SLURM_CPUS_PER_CPU_WORKER
WorkFlow_SLURM_CPUS_PER_GPU_WORKER
WorkFlow_SLURM_BASE_MEMORY_GIB
WorkFlow_SLURM_CPU_WORKER_MEMORY_GIB
WorkFlow_SLURM_GPU_WORKER_MEMORY_GIB
WorkFlow_SLURM_MAX_CPU_WORKERS
WorkFlow_SLURM_MAX_GPU_WORKERS
WorkFlow_SLURM_MAX_CPUS
WorkFlow_SLURM_MAX_GPUS
WorkFlow_SLURM_MAX_MEMORY_GIB
```

控制面会在提交前检查这些上限；超出单节点策略时明确拒绝，而不是把 Worker
放到登录节点，也不会伪装成已经支持多节点 Dask。

## 固定资源 smoke tests

以下脚本故意保留固定资源，仅用于验证集群 CUDA 和端到端测试；它们不是
生产服务器或正式 execution 的提交入口。

真实多 Worker 进程 smoke test（资源由 Worker 数量动态计算）：

```bash
# 两个 CPU Worker，不申请 GPU
bash "$HOME/apps/WorkFlow/deploy/hpc/submit_multi_worker_smoke.sh" 2 0

# 两个 CPU Worker + 两个 GPU Worker，动态申请两张 GPU
bash "$HOME/apps/WorkFlow/deploy/hpc/submit_multi_worker_smoke.sh" 2 2
```

这个提交器固定单 compute node，但不在 `.sbatch` 中写死 CPU、GPU 或内存；
它根据测试参数生成 `--cpus-per-task`、`--mem` 和（需要时）`--gres=gpu:N`。
作业会通过生产 `DaskService` 启动指定数量的真实 Nanny/Worker 进程，将一个
确定性计算任务精确绑定到每个 Worker，并验证地址、PID、主机和资源角色均
正确且唯一。只要 GPU Worker 数大于零，每个 GPU Worker 还必须实际执行一个
PyTorch CUDA kernel，并且只能看到一张获配 GPU；CPU-only 测试结果会明确记录
`cudaComputeValidated=false`，不能作为 CUDA 验证结果。日志位于
`workflow-runtime/logs/multi-worker-smoke-<jobId>.log`，机器可读结果位于
`workflow-runtime/test-runs/multi-worker-smoke-<jobId>.json`。

这组手工 smoke 参数只验证集群进程拓扑。页面正式执行仍然由 Graph 的
`resourcePlan` 自动决定 Worker 数量并通过同一 `DaskService` 启动，不会读取
smoke test 的 Worker 参数。

GPU smoke test：

```bash
mkdir -p "$HOME/workflow-runtime/logs"
sbatch \
  --output="$HOME/workflow-runtime/logs/gpu-smoke-%j.log" \
  --chdir="$HOME/apps/WorkFlow" \
  "$HOME/apps/WorkFlow/deploy/hpc/slurm/gpu_smoke.sbatch"
```

真实图像 Window 集成 smoke test：

```bash
bash "$HOME/apps/WorkFlow/deploy/hpc/prepare_test_data.sh"
sbatch \
  --output="$HOME/workflow-runtime/logs/integration-%j.log" \
  --chdir="$HOME/apps/WorkFlow" \
  "$HOME/apps/WorkFlow/deploy/hpc/slurm/integration_smoke.sbatch"
```

集成 smoke test 执行 OME-Zarr Reader → Cellpose → Zarr Writer + Parquet
Writer。Writer 的 terminal token 网格是一块输入 Dask block 对应一个元素；测试
使用 `windowShape=[1,1]`，因此默认 2×2 block 输入会形成 4 个可恢复 Window，
并检查输出、Parquet 行数、recovery manifest 和 completion bitmap。
