# WorkFlow 的 Slurm 多节点部署

## 最终进程位置与生命周期

WorkFlow 在 HPC 上分成“服务进程”和“按工作流申请的计算资源”，两者不能混在一起：

```text
浏览器（SSH loopback 隧道）
  ↓
登录/管理/专用 service node
  ├─ Uvicorn/FastAPI：常驻
  ├─ workflow Driver：在一次执行期间运行
  └─ Dask Scheduler：有工作流时按需启动，执行结束立即关闭
       ↓ TCP/TLS（集群内网）
Slurm allocation（由当前 Graph 的 resourcePlan 动态决定）
  ├─ compute node 0：一个 Worker agent → CPU/GPU Nannies/Workers
  ├─ compute node 1：一个 Worker agent → CPU/GPU Nannies/Workers
  └─ ...
```

因此 Scheduler **不是常驻服务**。常驻的只有 Web 项目主进程；每次正式执行才在同一服务进程中建立一个 Scheduler，等待 Slurm 分配的 compute-node Workers 注册，Driver 再提交当前图。执行成功、失败或取消后，Worker allocation 被取消，Scheduler 被关闭。

只有 Nanny/Worker 进程运行在 compute node（CN）。Driver 不在 CN 执行，Uvicorn 也不通过 `sbatch` 启动。应用一次只允许一个正式 execution，所以可以使用一个固定 Scheduler 端口。

## Slurm 怎样申请一个或多个 CN

后端只分析当前 terminal outputs 可达的 Graph 节点。节点类通过 `EXECUTION_RESOURCE` 和 `EXECUTION_WORKERS` 声明 CPU/GPU Worker 需求。资源策略把全图 Worker 总数装箱到最少的可行节点数：

1. 先尝试一个 CN。
2. 如果该节点的 GPU、CPU 或内存容量装不下，就依次尝试 2、3……个 CN。
3. 节点数不能超过 `WorkFlow_SLURM_MAX_NODES`，每个节点不能超过配置的 per-node envelope。
4. 找到布局后，提交一个包含 N 个节点的 Slurm allocation。
5. `srun --ntasks-per-node=1` 在每个已分配 CN 上启动一个 Worker agent；agent 按持久化布局启动该节点应有的 CPU/GPU Workers。

Slurm 19.05 的 `--gres=gpu:N` 是同构的每节点申请。若最后一个节点只需较少 GPU，allocation 可能保守地多申请少量 GPU，但 Worker agent 不会把多余设备暴露给任何任务。例如 10 个 GPU Workers、每节点最多 4 张卡会形成逻辑布局 `4 + 3 + 3`，Slurm 同构申请为 3 个节点、每节点 4 张卡。

Graph 不能注入任意 `sbatch` 参数。partition、时限和容量均由管理员环境变量控制。若在 `MAX_NODES` 内仍装不下，preflight/提交会明确失败；不会把 Worker 回退到服务节点。

## 集群前提

部署前由管理员确认：

- 允许在指定登录/管理节点运行轻量 Web 服务和 workflow Driver；若登录节点不允许守护进程，必须改用管理员批准的 service node/systemd/容器服务。
- 服务账号可执行 `sbatch`、`srun`、`squeue`、`scontrol` 和 `scancel`。
- checkout 与 `$HOME/workflow-runtime` 是所有目标 CN 可见的共享绝对路径。
- CN 能解析并连接 `WorkFlow_DASK_SCHEDULER_HOST:WorkFlow_DASK_SCHEDULER_PORT`。
- 服务节点与 CN 之间允许配置的 Worker/Nanny TCP 端口范围；多节点之间也允许 Dask 所需流量。
- Slurm GRES 正确设置每个 job step 的 `CUDA_VISIBLE_DEVICES`。
- 站点的 partition/account/QoS 允许所配置的节点、CPU、内存和 GPU 数量。

Web 端口仍只监听 `127.0.0.1`，从外部通过 SSH loopback 隧道访问。Scheduler 地址则必须是 CN 可达的服务节点 IPv4 地址或 DNS 名，不能是 `127.0.0.1`、`localhost`、`0.0.0.0` 或 wildcard。

## 安全边界

Dask Scheduler 跨节点通信不应暴露到公网或非可信共享网络。生产环境应同时使用：

- 网络 ACL/防火墙，仅允许本服务节点与本账户 Slurm CN 访问 Scheduler、Worker、Nanny 端口；
- Dask mutual TLS，CA、证书和私钥由集群管理员提供并轮换；
- 共享 request/job 目录权限为用户私有。

TLS 文件配置必须三项同时存在：

```bash
export WorkFlow_DASK_TLS_CA=/absolute/private/dask-ca.pem
export WorkFlow_DASK_TLS_CERT=/absolute/private/dask-service.pem
export WorkFlow_DASK_TLS_KEY=/absolute/private/dask-service.key
```

启动脚本会拒绝缺项、相对路径、symlink 或不可读文件。证书文件“已配置”不等于 mTLS 已验收；必须在实际 deployment 中确认 Scheduler 地址为 `tls://`，并完成 Worker 注册和计算测试。

当前目标集群尚未提供并实测 mTLS 证书。只在管理员确认的可信隔离内网和严格 ACL 下进行临时验收时，才可显式使用：

```bash
export WorkFlow_DASK_ALLOW_INSECURE_CLUSTER=1
```

该开关不会让明文 TCP 变安全，生产环境不应依赖它。

## 安装与 runtime 目录

默认路径：

- checkout：`$HOME/apps/WorkFlow`
- Python：`$HOME/apps/WorkFlow/backend/.venv`
- 共享 runtime：`$HOME/workflow-runtime`

安装或更新：

```bash
bash "$HOME/apps/WorkFlow/deploy/hpc/install.sh"
```

`workflow-runtime` 与源码分开，原因是模型、输入、输出和恢复记录需要跨代码升级保留且被 CN 共享：

- `data/demo_images.zip`：`prepare_test_data.sh` 下载的 Cellpose 示例 PNG 压缩包，只是测试输入缓存，不是 Zarr 输出。
- `models/`：共享模型缓存，如 `models/cellpose/cpsam`。大模型不应随 backend 源码更新重复下载。
- `test-runs/`：smoke/probe 的一次性输入、Zarr/Parquet 输出、日志和结果；不是页面运行的默认生产输出。
- `jobs/`、`requests/`、`state/`、`logs/`：Driver 与 Slurm Worker allocation 的持久化控制记录。
- `output/`、`recovery/`：正式执行可使用的输出与 Window recovery 根目录。

## 必要配置

下面是一个站点配置示例，具体容量必须由管理员根据 partition 实际节点填写：

```bash
export WorkFlow_SLURM_PARTITION=compute
export WorkFlow_SLURM_MAX_NODES=8
export WorkFlow_SLURM_CPUS_PER_NODE=64
export WorkFlow_SLURM_GPUS_PER_NODE=8
export WorkFlow_SLURM_MEMORY_GIB_PER_NODE=512

# 必填：必须从 compute node 可解析、可达，不能写 localhost。
export WorkFlow_DASK_SCHEDULER_HOST=mn02.cluster.example
export WorkFlow_DASK_SCHEDULER_PORT=8786
export WorkFlow_DASK_WORKER_PORT_RANGE=20000:20999
export WorkFlow_DASK_NANNY_PORT_RANGE=21000:21999
```

默认值为 `MAX_NODES=8`、`CPUS_PER_NODE=64`、`GPUS_PER_NODE=8`、`MEMORY_GIB_PER_NODE=512`、Scheduler 端口 `8786`、Worker 端口 `20000:20999`、Nanny 端口 `21000:21999`。`WorkFlow_DASK_SCHEDULER_HOST` 没有默认值，必须显式设置。
每个端口范围的宽度至少要覆盖“单个 CN 上可能启动的最大 Worker 数”，不同 CN 可以复用同一范围。

全局上限和每 Worker 换算仍可配置：

```text
WorkFlow_SLURM_ALLOWED_PARTITIONS
WorkFlow_SLURM_TIME_LIMIT
WorkFlow_SLURM_BASE_CPUS
WorkFlow_SLURM_BASE_MEMORY_GIB
WorkFlow_SLURM_CPUS_PER_CPU_WORKER
WorkFlow_SLURM_CPUS_PER_GPU_WORKER
WorkFlow_SLURM_CPU_WORKER_MEMORY_GIB
WorkFlow_SLURM_GPU_WORKER_MEMORY_GIB
WorkFlow_SLURM_MAX_CPU_WORKERS
WorkFlow_SLURM_MAX_GPU_WORKERS
WorkFlow_SLURM_MAX_CPUS
WorkFlow_SLURM_MAX_GPUS
WorkFlow_SLURM_MAX_MEMORY_GIB
```

全局 `MAX_CPUS/MAX_GPUS/MAX_MEMORY_GIB` 限制整个 execution；per-node 变量定义单个 CN 装箱容量。生产 Worker launcher 是：

```text
WorkFlow_SLURM_EXECUTION_SCRIPT=<repo>/deploy/hpc/slurm/workflow_workers.sbatch
```

不要再将旧的 `workflow_execution.sbatch` 用作正式页面执行入口；它把 Driver/Scheduler 放入 CN，与本架构不符。

## 启动控制面

配置环境变量后启动：

```bash
cd "$HOME/apps/WorkFlow"
WorkFlow_SLURM_SACCT='' \
WorkFlow_SLURM_PARTITION=compute \
WorkFlow_DASK_SCHEDULER_HOST=mn02.cluster.example \
WorkFlow_DASK_ALLOW_INSECURE_CLUSTER=1 \
  bash deploy/hpc/control_plane.sh restart
```

上例 `ALLOW_INSECURE_CLUSTER=1` 只适合目标集群当前无证书的受控验收；生产应替换为 TLS 三文件。`control_plane.sh` 对 tmux 环境使用显式 allowlist，并会先 unset 所有受管变量，因此操作者取消某个变量后，旧 tmux server 不会偷偷恢复它。

检查：

```bash
bash deploy/hpc/control_plane.sh status
curl -fsS http://127.0.0.1:8000/plugin_status
bash deploy/hpc/control_plane.sh logs
```

启动日志应明确显示：Driver 位于 service process，Scheduler 是 `on-demand:<host>:<port>`，Workers 位于 `slurm-compute-nodes`。

## 先验证 Scheduler 网络

在没有活动 execution、固定 Scheduler 端口空闲时运行：

```bash
cd "$HOME/apps/WorkFlow"
WorkFlow_SLURM_PARTITION=compute \
WorkFlow_DASK_SCHEDULER_HOST=mn02.cluster.example \
WorkFlow_DASK_SCHEDULER_PORT=8786 \
  bash deploy/hpc/probe_scheduler_connectivity.sh
```

该 probe 在服务节点临时监听同一 TCP 地址，用 `srun` 申请一个 CN，让 CN 回连并使用随机 nonce 验证响应。结果保存在 `workflow-runtime/test-runs/scheduler-connectivity-*/result.json`。它只证明“CN → 服务节点固定端口”路由/防火墙可达，不证明 Dask、mTLS、GPU、多 Worker 或反向 Worker 端口已经通过。

## 页面执行过程

一次普通 Run 的顺序是：

1. 服务节点序列化当前 editor Graph，并进行只读 preflight/resource planning。
2. Driver 在服务进程内按需启动固定 host/port 的 Scheduler。
3. 后端持久化 execution/request/layout，再按 Graph 计划提交 `workflow_workers.sbatch`。
4. Slurm 选择满足同构请求的一个或多个 CN；`srun` 每节点启动一个 Worker agent。
5. 所有预期 CPU/GPU Workers 注册且身份、资源、设备拓扑验证通过后，Driver 才提交图。
6. terminal output Futures 成功后，Driver 更新 Window completion bitmap。
7. 成功、失败或取消都先停止计算并 `scancel` Worker allocation，再关闭 Scheduler。

Window resume/restart 仍由 Recovery 界面显式发起，并使用 recovery 目录中的不可变 Graph。服务进程重启时无法让已经消失的 Driver 继续计算；启动清理必须取消孤儿 Worker allocation，之后由用户显式 Resume。普通 New Run 不会静默变成 Resume。

## 外部访问页面

先连接管理员提供的 OpenVPN，然后在 Windows checkout 中运行：

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\hpc\open_workflow_tunnel.ps1 `
  -User YOUR_CLUSTER_USER `
  -ClusterHost 10.200.201.2
```

打开 `http://127.0.0.1:18000/`。脚本使用已有 SSH 认证，不生成、读取或保存密码/私钥。不要把 Uvicorn 改成监听公网 `0.0.0.0`；多人生产访问应由管理员在 service node 前部署带 TLS 和身份认证的反向代理。

## 测试边界

`submit_multi_worker_smoke.sh` 是旧的单 CN Dask 进程/GPU 基础设施检查，不等于最终跨节点 Driver/Scheduler 架构验收。最终验收至少应真实执行：

- Scheduler connectivity probe；
- 1 个 CN、多个 CPU Workers；
- 1 个 CN、至少 1 个 GPU Worker 并执行 CUDA kernel；
- 2 个或更多 CN，验证 Worker hostname 分布和跨节点任务；
- 页面 Full Graph 与 Window New Run；
- Window Resume/Restart；
- 取消和服务进程异常后的 orphan allocation 回收。

没有真实获得两个 CN 并运行上述任务时，不得声称“多节点已通过”。
