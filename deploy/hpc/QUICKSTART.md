# WorkFlow Slurm 快速部署

本文用于在新的 Slurm 集群上快速部署 WorkFlow。默认目录为：

```text
代码：$HOME/apps/WorkFlow
运行数据：$HOME/workflow-runtime
Python：$HOME/apps/WorkFlow/backend/.venv
模型：$HOME/workflow-runtime/models
```

代码目录和运行目录必须位于所有目标计算节点都能访问的共享文件系统。

## 1. 部署前检查

集群需要满足以下条件：

- 服务节点可以长期运行一个轻量 Web 服务。
- 当前用户可以执行 `sbatch`、`srun`、`squeue`、`scontrol` 和 `scancel`。
- Slurm 支持 heterogeneous job（新版本称为 hetjob）。
- 计算节点可以连接服务节点的 Scheduler、Worker 和 Nanny 端口。
- GPU 节点正确配置 GRES 和 `CUDA_VISIBLE_DEVICES`。
- PyTorch、CUDA 和 GPU 架构相互兼容。

## 2. 安装或更新

首次安装：

```bash
mkdir -p "$HOME/apps"
git clone --branch master https://github.com/hexiyimeng/WorkFlow.git \
  "$HOME/apps/WorkFlow"
bash "$HOME/apps/WorkFlow/deploy/hpc/install.sh"
```

后续更新：

```bash
bash "$HOME/apps/WorkFlow/deploy/hpc/install.sh"
```

安装脚本会完成以下工作：

- 安装项目固定使用的 Python 3.12。
- 通过 `uv sync` 创建或更新 `$HOME/apps/WorkFlow/backend/.venv`。
- 编译检查后端代码，并校验仓库中已有的前端构建文件；安装脚本不重新构建前端。
- 创建 `$HOME/workflow-runtime/models/cellpose` 等运行目录。
- CPSAM 不存在时，优先复用本机已有文件，否则下载并校验到：

```text
$HOME/workflow-runtime/models/cellpose/cpsam
```

项目只使用一个模型根目录 `WorkFlow_MODELS_DIR`。Slurm 部署会自动将它设置为
`$HOME/workflow-runtime/models`。其他 Cellpose 模型也放在 `models/cellpose/` 下。

## 3. 配置集群参数

首次部署时，在服务节点运行一次：

```bash
cd "$HOME/apps/WorkFlow"
WorkFlow_DASK_ALLOW_INSECURE_CLUSTER=1 \
  bash deploy/hpc/control_plane.sh configure
```

脚本会自动使用当前主机名、发现 Slurm 分区，并采用该用户的默认 Account 和 QoS。
配置保存在 `$HOME/workflow-runtime/config/control-plane.env`；已有配置就跳过此步。
上面的开关仅用于受限制的集群内网测试。其他集群需要 TLS、固定分区或特殊
Account/QoS 时，按 `deploy/hpc/README.md` 修改配置文件。

## 4. 启动控制平面

```bash
cd "$HOME/apps/WorkFlow"
bash deploy/hpc/control_plane.sh restart
```

检查服务：

```bash
bash deploy/hpc/control_plane.sh status
curl -fsS http://127.0.0.1:8000/plugin_status
```

正常情况下，接口返回 `ok: true`，并且节点插件没有加载失败。

## 5. 验证 Scheduler 网络

加载控制平面配置：

```bash
cd "$HOME/apps/WorkFlow"
set -a
. "$HOME/workflow-runtime/config/control-plane.env"
set +a
export WORKFLOW_RUNTIME_DIR="$HOME/workflow-runtime"
```

直接运行探针；它会选择默认的非排除分区：

```bash
bash deploy/hpc/probe_scheduler_connectivity.sh
```

也可以指定一个确定可用的分区：

```bash
WorkFlow_SLURM_PARTITION=compute \
  bash deploy/hpc/probe_scheduler_connectivity.sh
```

探针会在服务节点临时监听配置的 Scheduler 地址和端口，然后通过 `srun` 申请
`1 node + 1 CPU + 1 GiB`，由计算节点连接回来并交换一次随机 nonce。它只验证
“计算节点 → 服务节点 Scheduler TCP 端口”的路由和防火墙，不启动 Dask，也不验证 GPU。

## 6. 验证 Adaptive 扩缩容

先生成计划，不提交 Job：

```bash
backend/.venv/bin/python deploy/hpc/adaptive_smoke.py --gpu
```

确认计划正确后，提交真实 CPU/GPU Job：

```bash
backend/.venv/bin/python deploy/hpc/adaptive_smoke.py --gpu --run
```

通过标准：

- 最小 CPU/GPU 资源通过一次 HetJob 联合申请。
- CPU 负载只扩容 CPU worker。
- GPU 负载只扩容 GPU worker。
- 额外 worker 实际执行任务后能够缩容。
- 测试结束后 `cleanupConfirmed=true`，且没有残留 Job。

结果位于：

```text
$HOME/workflow-runtime/test-runs/smoke-*/result.json
```

可以同时观察 Slurm 队列：

```bash
watch -n 2 "squeue -u $USER -o '%.18i %.16P %.9T %.32R %.40k'"
```

## 7. 从本机访问页面

当前测试集群的 Windows 命令：

```powershell
powershell -ExecutionPolicy Bypass -File D:\Workspace\Python_Projects\WorkFlow\deploy\hpc\open_workflow_tunnel.ps1 -User songzh -ClusterHost 10.200.201.2
```

`10.200.201.2` 是从本机 SSH 登录集群的地址，不是 Dask Scheduler 地址。部署到
其他集群时，再替换脚本路径、用户名和 SSH 登录地址。脚本会打开 SSH 认证窗口；
输入密码后会自动打开 WorkFlow 页面。使用期间不要关闭该窗口。

认证成功后访问：

```text
WorkFlow：http://127.0.0.1:18000/
Dask Dashboard：http://127.0.0.1:18787/status
```

Dask Scheduler 和 Dashboard 只在工作流执行期间存在。没有正在运行的工作流时，
Dashboard 地址无法访问属于正常现象。

## 8. 页面中的资源配置

在 Execution Settings 中分别配置 CPU 和 GPU worker：

- `cores`、`memory` 和 `gpus`：每个 Slurm Job 的资源规格。
- `processes`：每个 Job 启动的 worker 进程数。
- `minimum_jobs`：启动工作流前必须获得的最低资源。
- `maximum_jobs`：Adaptive 可以扩展到的最大 Job 数，包含最低资源和排队中的 Job。

最低 CPU/GPU 资源会组合成一次 HetJob 申请；执行期间 Adaptive 根据对应类型的任务压力，
使用相同 Profile 的 Slurm 规格独立扩容或缩容。

## 9. 常用检查

```bash
# 控制平面状态
bash "$HOME/apps/WorkFlow/deploy/hpc/control_plane.sh" status

# 查看日志
bash "$HOME/apps/WorkFlow/deploy/hpc/control_plane.sh" logs

# 查看当前 Job 和排队原因
squeue -u "$USER" -o '%.18i %.16P %.9T %.32R %.40k'

# 停止控制平面
bash "$HOME/apps/WorkFlow/deploy/hpc/control_plane.sh" stop
```

Job 长时间处于 `PENDING` 时，应先检查 `NODELIST(REASON)`。常见原因包括资源不足、
优先级、Account/QoS 不匹配和 GPU GRES 不可用。
