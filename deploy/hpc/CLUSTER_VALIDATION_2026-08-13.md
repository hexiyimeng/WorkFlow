# WorkFlow Slurm 集群部署与验收记录（2026-08-13）

## 结论

基于目标集群返回的真实日志，首轮提交 `839894bf0ab94b8567bee4d6ebce6f0c032e18d1`
以及兼容性更新 `f2c3d6f1bcf8d5738e885e6174d141639314a903` 已验证以下链路：

- Web 控制面常驻登录/服务节点 `mn02`，页面返回 HTTP 200；
- 控制面按 Worker 数量提交 Slurm 作业，Dask Scheduler、Nanny 和 Worker 仅在
  compute node `c001` 中运行；
- 两个 CPU Worker 的真实多进程任务通过；
- 一个 CPU Worker 加一个 GPU Worker 的真实多进程任务通过，GPU Worker 在
  NVIDIA A40 上完成了 PyTorch CUDA 计算；
- Windows 客户端经 OpenVPN/SSH loopback 隧道访问首页返回 HTTP 200，WebSocket
  收到 Slurm 控制面就绪消息；
- 通过同一外部 WebSocket 接口发起了一次正式 Window Recovery Restart，控制面使用
  recovery 目录中的不可变 Graph 提交 Slurm 作业，最终 4/4 Window 完成；
- 三次多 Worker smoke 都产生了持久结果文件，并完成 Dask 集群的正常关闭。

因此，当前的“登录/服务节点控制面 → Slurm 动态申请 → 单个 compute node 内建立
Dask 集群”架构已通过本次范围内的实机验收。此次验收**没有**证明多 GPU或跨多个
compute node 可用；这些边界见下文。

## 验收环境和证据

| 项目 | 结果 | 实机证据 |
| --- | --- | --- |
| 首轮部署版本 | 通过 | commit `839894bf0ab94b8567bee4d6ebce6f0c032e18d1` |
| 最终服务端版本 | 通过 | commit `f2c3d6f1bcf8d5738e885e6174d141639314a903` |
| Slurm 版本 | 通过 | `slurm 19.05.7`；`squeue=/usr/bin/squeue`；`scontrol=/usr/bin/scontrol` |
| 控制面 | 通过 | tmux 会话 `workflow-control-plane` 正在运行；`http://127.0.0.1:8000/` 返回 200 |
| 插件加载 | 通过 | `/plugin_status`：`ok=true`、loaded 7、failed 0、node-info error 0 |
| 作业 55052 | 通过 | `c001`；请求 CPU=2、GPU=0；验证 CPU Worker=2 |
| 作业 55053 | 通过 | `c001`；请求 CPU=1、GPU=1；验证 CPU Worker=1、GPU Worker=1 |
| 作业 55054 | 通过 | 最终服务端版本；`c001`；请求 CPU=1、GPU=1；验证 CPU Worker=1、GPU Worker=1 |
| CUDA 实算 | 通过（单卡） | NVIDIA A40；逻辑设备数 1；PyTorch 2.11.0+cu128；CUDA 12.8；计算校验通过 |
| Worker 隔离 | 通过 | 每个作业中的两个 Worker 地址与 PID 均不相同 |
| 关闭流程 | 通过 | 三个结果均记录 `clusterShutdown=graceful` |
| Windows 外部访问 | 通过 | 隧道端口 `127.0.0.1:18000` 返回 HTTP 200；插件 7/0；WebSocket 就绪消息正确 |
| 正式 WebSocket 执行 | 通过（Recovery Restart） | execution `cluster-page-restart-8c45f063883e4b4e84b622f4e806afa9`；不可变恢复 Graph；状态 `succeeded`；Window 4/4 |
| Slurm accounting | 不可用 | `sacct` 连接 `localhost:6819` 被拒绝，目标环境没有可用的 `slurmdbd` |

具体 Worker 证据：

- 55052：地址 `tcp://127.0.0.1:37687`、`tcp://127.0.0.1:37843`；PID
  3844288、3844290。
- 55053：地址 `tcp://127.0.0.1:38423`、`tcp://127.0.0.1:42297`；PID
  3844285、3844292。GPU Worker 物理 GPU ID 为 0，CUDA 结果 checksum 为
  750608527851520。
- 55054：地址 `tcp://127.0.0.1:40495`、`tcp://127.0.0.1:42139`；PID
  3844649、3844646。GPU Worker 只看到逻辑设备 0，NVIDIA A40 CUDA kernel
  checksum 为 750608527851520，`clusterShutdown=graceful`。

结果文件位于：

```text
/share/home/songzh/workflow-runtime/test-runs/multi-worker-smoke-55052.json
/share/home/songzh/workflow-runtime/test-runs/multi-worker-smoke-55053.json
/share/home/songzh/workflow-runtime/test-runs/multi-worker-smoke-55054.json
```

正式 WebSocket Recovery Restart 的恢复记录位于：

```text
/share/home/songzh/workflow-runtime/test-runs/server-smoke-7547c9a6f0404737b45a2d175c9ce0a4/recovery
```

再次通过恢复检查 API 读取该记录，得到 `valid=true`、`compatible=true`、
`status=succeeded`、`completedWindows=4`、`totalWindows=4`。输出合同指向测试目录中的
`output/masks.zarr` 和 `output/cells`。这项证据覆盖了浏览器所用 WebSocket 协议、
控制面 Slurm 分派、不可变恢复 Graph 和 Window checkpoint 收口；它是显式 Recovery
Restart，不等同于从当前编辑器 Graph 发起一次全新的普通 Run。

## 实际部署架构

```text
外部浏览器
  │ OpenVPN + SSH loopback 隧道
  ▼
登录/服务节点：Uvicorn/FastAPI 控制面（127.0.0.1:8000）
  │ 当前 Graph JSON
  ├─ 只读 preflight
  ├─ 生成 resourcePlan（CPU/GPU Worker、CPU、内存、时限）
  └─ sbatch --export=NONE
       ▼
Slurm 分配的一个 compute node
  └─ execution runner
       ├─ Dask Scheduler
       ├─ CPU Nanny/Workers
       └─ 每卡一个、受 Slurm CUDA 可见性约束的 GPU Nanny/Worker
            │
            ▼
共享文件系统：request、events、result、logs、output、recovery
```

控制面不会预占固定计算资源。每次正式运行前，后端只分析当前终端输出可达的节点，
根据各节点的 `EXECUTION_RESOURCE` 和 `EXECUTION_WORKERS` 生成资源计划，再用站点策略
换算 `--cpus-per-task`、`--mem` 和 `--gres=gpu:N`。Slurm 决定具体使用哪个 compute
node；作业进入分配后，runner 才在该节点建立本地 Dask 集群。控制面不会 SSH 到
compute node，也不会在登录节点启动 Dask Worker。

当前正式执行是**单 compute node Dask**：一次 execution 的 Scheduler 和全部 Worker
位于同一个 Slurm allocation。资源数量不是写死在生产 `.sbatch` 文件中，但拓扑固定为
`--nodes=1 --ntasks=1`。跨节点 Dask 尚未实现。

## `workflow-runtime` 目录说明

`$HOME/workflow-runtime` 与代码仓库分离，并必须位于控制面和 compute node 都能访问的
共享文件系统上。

- `data/demo_images.zip`：`prepare_test_data.sh` 下载的 Cellpose 示例 PNG 压缩包，
  只是集成测试输入缓存，不是生产输出，所以它是 ZIP 而不是 Zarr。测试执行时才会在
  对应测试目录中创建真实 Zarr。
- `models/`：共享模型缓存，例如 `models/cellpose/cpsam`。模型体积大且应跨代码更新、
  compute node 和作业复用，因此不放在 `backend/` 源码目录内。
- `test-runs/`：一次性 smoke/integration test 的结果、测试 Zarr/Parquet、恢复记录或
  摘要。页面发起的生产输出不会自动放到这里。
- `requests/`、`jobs/`、`state/`、`logs/`：控制面和 compute job 交换请求、状态、
  事件、结果及日志所需的运行记录。
- `output/`、`recovery/`：正式运行可以使用的输出和恢复根目录，不应随测试目录一起
  清理。

## 安装、启动和检查命令

在登录/服务节点执行安装或快进更新：

```bash
bash "$HOME/apps/WorkFlow/deploy/hpc/install.sh"
```

启动或重启常驻控制面：

```bash
cd "$HOME/apps/WorkFlow"
WorkFlow_SLURM_SACCT='' \
WorkFlow_SLURM_PARTITION=compute \
  bash deploy/hpc/control_plane.sh restart
```

这里显式将 `WorkFlow_SLURM_SACCT` 设为空，是因为本次验收已经证明站点安装了 `sacct`
客户端，但没有可连接的 `slurmdbd`；控制面将直接使用 `squeue + scontrol`。

检查状态、页面和插件：

```bash
bash "$HOME/apps/WorkFlow/deploy/hpc/control_plane.sh" status
curl -fsS -o /dev/null -w 'page_http=%{http_code}\n' \
  http://127.0.0.1:8000/
curl -fsS http://127.0.0.1:8000/plugin_status
```

查看控制面日志：

```bash
bash "$HOME/apps/WorkFlow/deploy/hpc/control_plane.sh" logs
```

提交与本次验收一致的真实多 Worker smoke tests：

```bash
cd "$HOME/apps/WorkFlow"
bash deploy/hpc/submit_multi_worker_smoke.sh 2 0
bash deploy/hpc/submit_multi_worker_smoke.sh 1 1
```

使用脚本输出的 job ID 查看队列、日志和机器可读结果：

```bash
squeue -h -j JOB_ID -o '%i|%T|%N|%R'
tail -n 200 "$HOME/workflow-runtime/logs/multi-worker-smoke-JOB_ID.log"
"$HOME/apps/WorkFlow/backend/.venv/bin/python" -m json.tool \
  "$HOME/workflow-runtime/test-runs/multi-worker-smoke-JOB_ID.json"
```

这些 smoke 脚本只用于验收；页面正式执行仍由 Graph 的资源计划自动决定 Worker 数量。

## 从外部网络访问页面

1. 先按集群管理员提供的说明连接 OpenVPN。
2. 在 Windows 上、包含本仓库脚本的 PowerShell 中执行：

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\hpc\open_workflow_tunnel.ps1 `
  -User YOUR_CLUSTER_USER `
  -ClusterHost 10.200.201.2
```

脚本使用已有 SSH 登录方式，在单独窗口请求认证，不生成、读取或保存密钥/密码；成功后
打开 `http://127.0.0.1:18000/`。使用页面期间需保持 VPN 和 SSH 窗口开启。

等价的手工命令为：

```bash
ssh -N -T \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -L 127.0.0.1:18000:127.0.0.1:8000 \
  YOUR_CLUSTER_USER@10.200.201.2
```

不要将控制面改为监听 `0.0.0.0`，也不要把 8000 端口直接暴露到公网。长期多人访问应由
集群管理员在 service node 前部署带 TLS 和身份认证的反向代理。

## `sacct/slurmdbd` 兼容性事项

目标集群上的 `sacct` 返回 `slurmdbd: Connection refused`。这不影响本次三个正常作业，
因为 runner 的原子 `result.json` 已提供最终结果；但在节点崩溃或作业被强制终止、尚未来得及
写结果时，仅依赖 `sacct` 会导致控制面无法确认终态，进而可能延迟释放 execution lease 或
Window recovery lock。

后续兼容性修改已由提交 `bb7c53c` 实现：以 runner 结果为最高优先级，以 `squeue` 判断 allocation
是否仍活跃，以 Slurm 19.05 可用的 `scontrol show job -o` 获取根作业终态；当历史记录已因
`MinJobAge` 被清理时，只有连续两次精确、成功的 `squeue` 查询都找不到同一 job，且仍没有
runner 结果，才将丢失作业判为失败。它不会把没有证据的作业判为成功。

上述无 `slurmdbd` 兼容性修改由 `bb7c53c` 实现，并已随服务端版本 `f2c3d6f`
部署到目标集群。真实作业 55054 证明更新后的控制面、Slurm 19.05.7 状态命令和
CPU/GPU Worker 主路径正常；尚未做目标集群“作业在写结果前被强杀”的故障注入验收。
已验证的状态命令包括：

```bash
scontrol --version
scontrol --local --oneliner --quiet show job 55054
```

若作业历史已被清理，`scontrol` 查不到旧 job 是允许的；仍需用新提交的作业验证运行态查询
和无结果异常退出的回收路径。

## 尚未覆盖的风险与边界

- **多 GPU 未实测**：本次只真实申请并计算了 1 张 NVIDIA A40；不能据此宣称 2/4/8 GPU
  Worker 已通过。
- **跨节点未实现、未实测**：当前是一个 Slurm job、一个 compute node 内的 Dask 集群。
- **普通 New Run 仍未单独做浏览器点击验收**：正式 WebSocket Recovery Restart 已完成，
  但本次没有用自动化视觉工具从编辑器逐项点击并提交一份全新的代表性 Graph。
- **浏览器视觉交互未自动化断言**：Windows 隧道的 HTTP、插件 API 和 WebSocket 已实测，
  但没有用自动化视觉工具逐项点击页面组件。
- **无 accounting 故障注入待复验**：`sacct` 不可用已确认，兼容修改已部署；仍需验证作业
  在写入 `result.json` 前异常结束时，UI 和 Window recovery lock 能在预期宽限期后收敛。
- **站点策略依赖管理员确认**：长期在登录节点运行 tmux 服务、partition/account/QoS、
  单节点 CPU/内存/GPU 上限及 SSH 转发权限应遵循集群管理规定。
