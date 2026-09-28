# WorkFlow Slurm 快速部署

默认目录：

```text
代码：$HOME/apps/WorkFlow
运行数据：$HOME/workflow-runtime
Python 环境：$HOME/apps/WorkFlow/backend/.venv
模型：$HOME/workflow-runtime/models
```

代码和运行目录必须位于计算节点可访问的共享文件系统。

## 1. 部署前确认

- 当前用户可以运行 `sbatch`、`srun`、`squeue`、`scontrol` 和 `scancel`。
- 集群支持 Slurm heterogeneous job（hetjob）。
- GPU 节点已配置 GRES，PyTorch 与 GPU/CUDA 兼容。
- 允许在当前服务节点运行 WorkFlow Web 服务。

## 2. 安装或更新

首次安装：

```bash
mkdir -p "$HOME/apps"
git clone --branch master https://github.com/hexiyimeng/WorkFlow.git \
  "$HOME/apps/WorkFlow"
WorkFlow_DASK_ALLOW_INSECURE_CLUSTER=1 \
  bash "$HOME/apps/WorkFlow/deploy/hpc/quick_deploy.sh"
```

以后更新只需执行：

```bash
bash "$HOME/apps/WorkFlow/deploy/hpc/quick_deploy.sh"
```

快速部署脚本会安装或更新项目、创建 Python 环境和运行目录，并启动 WorkFlow。首次
部署示例中的开关允许内部测试网络使用未加密的 Dask 通信；其他集群使用 TLS 配置，
详见 `deploy/hpc/README.md`。

CPSAM 最终保存在：

```text
$HOME/workflow-runtime/models/cellpose/cpsam
```

下载前会检查服务器上的以下旧位置：

```text
$HOME/.cellpose/models/cpsam
$HOME/apps/WorkFlow/backend/models/cellpose/cpsam
$HOME/apps/WorkFlow/models/cellpose/cpsam
```

找到有效文件时优先创建硬链接，失败后复制；都没有时才下载。这里不包含 Windows
本机文件。其他 Cellpose 模型放在 `$HOME/workflow-runtime/models/cellpose/`。

## 3. 可选检查

安装成功后可以直接访问页面。需要确认后台状态时再执行：

```bash
cd "$HOME/apps/WorkFlow"
bash deploy/hpc/control_plane.sh status
curl -fsS http://127.0.0.1:8000/plugin_status
```

首次运行时配置会自动保存到 `$HOME/workflow-runtime/config/control-plane.env`。

## 4. 可选验证 Scheduler 网络

怀疑计算节点无法连接 Scheduler 时再运行：

```bash
cd "$HOME/apps/WorkFlow"
bash deploy/hpc/probe_scheduler_connectivity.sh
```

看到下面内容说明计算节点可以连接 Scheduler：

```text
Compute-to-service Scheduler TCP connectivity passed.
```

探针会自动读取部署脚本生成的配置。

## 5. 从 Windows 访问

在本机仓库目录运行，并换成自己的 SSH 登录信息：

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\hpc\open_workflow_tunnel.ps1 `
  -User your-user `
  -ClusterHost login.cluster.example
```

输入 SSH 密码后访问：

```text
WorkFlow：http://127.0.0.1:18000/
Dask Dashboard：http://127.0.0.1:18787/status
```

保持 SSH 窗口开启。Dashboard 只在工作流运行期间存在。

## 6. 运行和观察

在页面的 Execution Settings 中配置 CPU/GPU Worker 的单 Job 规格、`minimum_jobs`
和 `maximum_jobs`，然后运行工作流。

在服务器观察 Slurm Job：

```bash
watch -n 2 "squeue -u $USER -o '%.18i %.16P %.9T %.32R %.40k'"
```

最低 CPU/GPU 资源通过一次 HetJob 联合申请；后续由 Adaptive 按 Worker 类型独立扩缩容。

## 7. 常用命令

```bash
# 状态
bash "$HOME/apps/WorkFlow/deploy/hpc/control_plane.sh" status

# 日志
bash "$HOME/apps/WorkFlow/deploy/hpc/control_plane.sh" logs

# 重启
bash "$HOME/apps/WorkFlow/deploy/hpc/control_plane.sh" restart

# 停止
bash "$HOME/apps/WorkFlow/deploy/hpc/control_plane.sh" stop
```

其他集群如果强制指定 Partition、Account、QoS、Reservation 或 TLS，再编辑
`$HOME/workflow-runtime/config/control-plane.env`。完整参数说明见 `deploy/hpc/README.md`。
