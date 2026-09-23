# Slurm Adaptive 现场验证（2026-09-23）

## 验证对象

- 执行 ID：`dc0d0230-0b33-48fc-bf30-32a0f6d1cf38`
- 分区：`tao`
- CPU 池：最少 1 Job、最多 4 Job，每 Job 2 个 Worker 进程
- GPU 池：最少 1 Job、最多 6 Job，每 Job 1 个 Worker 进程、1 张 GPU
- 基础资源由异构 Job `58012` 联合申请；CPU 组件运行在 `t000`，GPU 组件运行在 `t002`

## 现场结论

1. Adaptive 已按 GPU Profile 扩容。运行中的额外 GPU Job `58024` 已注册为
   `GPU-elastic-11`；没有提交 CPU 弹性 Job。
2. Dashboard 当时显示 4 个 Worker：2 个基础 CPU Worker、1 个基础 GPU Worker和
   1 个弹性 GPU Worker。PENDING Slurm Job 尚未启动进程，因此不会出现在 Dask Worker
   表中。
3. `CPU-1-het-1` 实际是基础 GPU Worker。它请求 `gpu:1`、声明 Dask 资源
   `GPU=1`，但旧实现用第一个异构组件 `CPU-1` 作为所有基础 Worker 的名称前缀，造成
   Dashboard 名称错误。资源类型和任务约束没有被改成 CPU。
4. `tao` 的 8 张 GPU 当时全部占用：本工作流使用 2 张，其他运行中 Job 使用 6 张。
   节点仍有空闲 CPU 和内存并不能满足额外 Job 的 `--gres=gpu:1` 请求。因此额外 Job
   显示 `PENDING (Resources)` 或 `PENDING (Priority)`，而 Worker 数不能继续增加。
5. 现场还发现排队抖动：allocation journal 已从 `GPU-elastic-1` 增长到
   `GPU-elastic-22`。`scontrol` 显示多批排队 Job 被 Adaptive 取消后重新提交。Dask 默认
   Adaptive 每 1 秒检查一次、连续 3 次低目标即可缩容，这不适合分钟或小时级 Slurm
   排队，会丢失原 Job 的排队时间。

## 修复

- 基础 Worker 名称改成 `baseline-<allocation-id>`。本例会显示为
  `baseline-CPU-1-0`、`baseline-CPU-1-1` 和 `baseline-GPU-1`。
- 已注册且空闲的弹性 Worker 仍使用 Dask 的普通缩容确认。
- 尚未注册的 PENDING Slurm Job 只有在目标数量连续 5 分钟保持下降后才取消，短暂的
  工作负载间隙不会重置排队位置。
- 必须减少多个 PENDING Job 时先取消最新申请，保留等待最久的 Job。
- 工作流结束或取消时仍立即停止 Adaptive，并由原有所有权清理路径回收全部 Job。

## 回归验证

- `backend/tests/test_worker_profiles.py` 与 `backend/tests/test_slurm_adaptive.py`：52 项通过。
- 完整后端测试：107 项通过。

## 跨分区修复与现场复测

原资源规划器虽然从 `sinfo` 发现了多个可用分区，却先选定一个模拟节点，再把该节点的
第一个分区写入 Worker Profile。结果是基础 Job 和 Adaptive 后续 Job 都可能被固定到
`tao`。这不是 Slurm 的限制，而是资源计划生成错误。

修复后，每类 Worker 按自身每 Job 的 CPU、内存和 GPU 规格筛选兼容分区，并把所有
兼容分区写入同一个提交参数。本集群当前站点策略排除 `mn,control`，CPU 和 GPU Profile
实际均生成 `--partition=gpu,compute,tao`。Slurm 再从候选分区中选择能够最早启动该 Job
的分区。Adaptive 扩容沿用对应 Profile 的候选分区列表，不会固定到基础 Job 最初落到的
分区。

现场复测目录：
`/share/home/songzh/workflow-runtime/test-runs/smoke-b95bc58774e94c70`

- 结果：`PASS`，Dask `2026.3.0`、distributed `2026.3.0`、dask-jobqueue `0.9.0`。
- 基础异构 Job `58084`：CPU 组件由 Slurm 放到 `gpu/aio`，GPU 组件放到
  `compute/c001`。
- CPU 弹性 Job `58086`：注册为 `CPU-elastic-1`，实际执行 CPU 任务，随后成功缩容。
- GPU 弹性 Job `58087`：注册为 `GPU-elastic-1`，实际执行 CUDA 运算，随后成功缩容。
- GPU 隔离有效：基础 GPU Worker 的 `CUDA_VISIBLE_DEVICES=0`，弹性 GPU Worker 为 `1`。
- 基础 Worker 在缩容后保留；现场名称为 `baseline-CPU-1` 和 `baseline-GPU-1`。
- Scheduler 最大往返时间约 `0.0074s`。
- 测试退出后确认 `squeue -u songzh -h` 为空，所有测试 Job 已清理。

对应实现提交：`3c9ec55 Schedule workers across compatible Slurm partitions`。

## GPU 架构兼容性复测

跨分区启用后，实际 CPSAM 工作流有两个弹性 GPU Job 落到 `gpu/aio`，并在加载
Transformer 权重时报告 `cudaErrorNoKernelImageForDevice`。日志确认该节点是 Tesla
V100（Compute Capability 7.0），而原 `torch 2.11.0+cu128` wheel 只包含 `sm_75`、
`sm_80`、`sm_86`、`sm_90`、`sm_100` 和 `sm_120`。权重 shape 一致，失败原因是
PyTorch 二进制没有 V100 对应的 `sm_70` kernel。

现场探针目录：
`/share/home/songzh/workflow-runtime/test-runs/gpu-compat-20260923-110643`

| 节点 | 分区 | GPU | CC | cu128 CUDA 运算 | cu128 CPSAM 加载 |
| --- | --- | --- | --- | --- | --- |
| `aio` | `gpu` | Tesla V100-SXM2-32GB | 7.0 | 失败 | 未进入加载 |
| `c001` | `compute` | NVIDIA A40 | 8.6 | 通过 | 通过 |
| `c002` | `compute` | NVIDIA A40 | 8.6 | 通过 | 通过 |
| `c003` | `compute` | NVIDIA A40 | 8.6 | 通过 | 通过 |
| `t000` | `tao` | NVIDIA A100-PCIE-40GB | 8.0 | 通过 | 通过 |
| `t002` | `tao` | NVIDIA GeForce RTX 3090 | 8.6 | 通过 | 通过 |

`t001` 的两张 GPU 当时均被其他用户占用，定向探针保持 `PENDING (Resources)`，没有
绕过 Slurm 登录节点直接访问设备。该节点探针随后取消并清理。

随后在独立临时环境中测试 `torch 2.11.0+cu126`。该 wheel 包含 `sm_50`、`sm_60`、
`sm_70`、`sm_75`、`sm_80`、`sm_86` 和 `sm_90`。V100、A40、A100 和 RTX 3090
均完成 CUDA 运算并成功加载同一个共享 CPSAM 模型。因此项目锁定改为
`torch 2.11.0+cu126` / `torchvision 0.26.0+cu126`，保留所有当前 GPU 分区的使用能力。
