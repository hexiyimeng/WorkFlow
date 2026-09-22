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

- `backend/tests/test_slurm_adaptive.py`：14 项通过。
- 完整后端测试：修复命名前 104 项通过；最终提交前重新执行完整测试。

