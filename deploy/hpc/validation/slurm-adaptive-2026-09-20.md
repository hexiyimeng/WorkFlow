# Slurm Adaptive 实测记录（2026-09-20）

> 2026-09-22 设计更新：当前本地代码已经收敛为 CPU、GPU 两类 Worker。下文三类 Worker 的 Slurm 实测结果是历史版本证据，不能作为本次两类 Worker 改动的集群验证结果。本次尚未更新远端部署。

## 2026-09-22 两类 Worker 的本地验证

- reader、writer、普通计算节点共用 CPU 池；GPU 节点共用 GPU 池。
- CPU Worker：`resources={"CPU": cores_per_worker}`；GPU Worker：`resources={"GPU": gpu_count}`。每个业务 task 要求对应资源 1，不再要求节点名称或专用 Profile token。
- GPU Worker 仍按一进程一张卡部署，其物理 CPU 仍向 Slurm 申请，但不声明 CPU task 额度。两池分别配置最低/最高 Job 数；基础资源联合申请，额外资源按类型扩缩容。
- Adaptive 按每 Worker 实际执行槽数与每 Job 进程数换算容量；Worker 尚未注册时也使用配置容量。
- 后端全量测试：102 passed。新增真实本地 Dask 并发测试确认：4 线程 CPU Worker 的峰值业务并发为 4；4 线程、GPU=1 的 Worker 峰值 GPU task 并发为 1；任务不会分配到另一类型 Worker。
- 前端测试、TypeScript 检查和生产构建通过，已更新本地 `backend/dist` 静态资源。
- smoke driver 的本地 Slurm 替身测试分别覆盖 CPU-only 和 CPU+GPU 模式，验证扩容、额外 Worker 执行、缩回最低数量和清理。未执行本次改动的真实 Slurm/CUDA 测试。

以下保留 2026-09-20 原始实测记录。

已直接更新 `/share/home/songzh/apps/WorkFlow`，保留原部署目录和站点配置。
更新前文件备份：`/share/home/songzh/.workflow-deploy/adaptive-update-20260919/previous-files.tar.gz`。

## 环境与修正

- Scheduler/Driver：`mn02`，Worker：真实 Slurm 计算节点。
- Slurm 19.05.7，`sched/backfill`，`select/cons_res` / `CR_CORE`。
- Dask / distributed 2026.3.0，dask-jobqueue 0.9.0，PyTorch 2.11.0+cu128。
- 修正旧 Slurm 命令兼容性：提交前检测版本；19.05 使用 `packjob` / `--pack-group`，20.02 以后使用 `hetjob` / `--het-group`。测试队列查询使用 `--user`，替代此集群不支持的 `--me`。

## 测试方式

运行 `deploy/hpc/adaptive_smoke.py --gpu --run`，使用生产资源规划、Worker 脚本生成和 ProfileAdaptive 代码，未模拟 sbatch 或 Worker。使用 Dask 默认 Adaptive 参数。

reader、writer、GPU 三类各最低 1 个 Job、最多 2 个 Job，每 Job 一个 Worker 进程。CPU Job 为 1 CPU / 4 GiB，GPU Job 为 1 CPU / 8 GiB / 1 GPU。每阶段是原生 delayed 依赖图：输入任务 → 32 个目标 Profile 任务 → 下游汇总。GPU 任务实际执行 CUDA 运算。

## 结果

| 验证项 | 第一轮：tao，2026-09-19 | 第二轮：compute，2026-09-20 |
|---|---|---|
| 最低资源联合申请 | 57875，三个组件全部注册 | 57883，三个组件全部注册 |
| reader 扩容 Job | 57878，执行 15 个任务 | 57886，执行 15 个任务 |
| writer 扩容 Job | 57879，执行 15 个任务 | 57887，执行 15 个任务 |
| GPU 扩容 Job | 57880，因 Resources 排队，未参与计算 | 57888，执行 15 个 CUDA 任务 |
| 扩容类型隔离 | 未扩容无关 Profile | 未扩容无关 Profile |
| 缩容 | CPU 返回最低值；撤回 GPU 排队申请 | 三类分别完成 1 → 2 → 1 |
| 基础资源 | 计算期间保留，GPU 基础 Worker 完成全部 32 个任务 | 缩容期间始终保留 |
| 最终清理 | 通过，无残留测试 Job | 通过，无残留测试 Job |
| 脚本判定 | INCONCLUSIVE：GPU 扩容执行证据不足 | PASS |

第二轮最低组件分别位于 c001、c002、c003；GPU 基础 Worker 在 c003，新增 GPU Worker 在 c001，分别完成 17 和 15 个 CUDA 任务。证明同一 Scheduler 下跨计算节点的依赖图执行与按类型扩缩容实际工作。

第一轮证明扩容 Job 排队时，基础 Worker 可以继续完成任务，未重复提交同类型扩容 Job；未将“提交成功但没有执行”标为通过。第二轮仅在测试进程中限定 compute 分区，未更改生产站点分区配置。

生产控制器的只读查询也对运行中的 57883 实测：`_query_queue_state` 聚合三个组件并返回 RUNNING，`_query_job_by_submission_token` 找回唯一根 Job 57883。

## Scheduler 观测和验证范围

| 指标 | 第一轮 | 第二轮 |
|---|---:|---:|
| Scheduler + 测试 Driver 进程 RSS 峰值 | 97.68 MiB | 99.03 MiB |
| 进程 CPU 采样峰值（100% 表示一个逻辑核） | 86.8% | 86.5% |
| 主线程到 Scheduler 循环查询最大耗时 | 1.326 ms | 1.543 ms |

这次轻量负载没有显示 Scheduler 资源瓶颈。观测包括测试 Driver，不能单独归因给 Scheduler；没有施加真实 Web/Dashboard 并发、大规模任务图或 Cellpose 模型负载，因此不能据此确定生产 Scheduler 的容量。

本地后端回归：94 项通过。最终 Web `/plugin_status` 返回 `ok=true`，7 个节点模块全部加载；测试 Scheduler 的 8786/8787 端口已释放，`squeue -u songzh` 无作业。

本次实测不覆盖完整 Cellpose 工作流吞吐、生产 Web 执行接口、运行中崩溃恢复或多进程 Worker Job；相关结论应另行验证，不能由本次 PASS 推导。

## 原始证据

两轮结果和逐次采样保存在集群：

- `/share/home/songzh/workflow-runtime/test-runs/smoke-7d5ab65e671b4175/{result.json,samples.jsonl}`
- `/share/home/songzh/workflow-runtime/test-runs/smoke-c48f0615ebe5465a/{result.json,samples.jsonl}`

本地证据副本位于 `.codex/slurm-session/evidence.json`；同目录保留命令记录、更新文件哈希和两轮结果 JSON。登录密码未写入这些文件。
