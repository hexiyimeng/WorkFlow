# Windows 多 GPU Worker 故障说明与验证

## 本次日志结论

8 GPU + 4 CPU 的失败不是 GPU 枚举失败。日志显示 12 个 Nanny/Worker 均为
`Status.running`，失败发生在 Driver 连接 Scheduler 公布的
`tcp://10.10.8.35:*` 地址时。对于同一台主机内的 `SpecCluster`，让 Driver、
Scheduler 和 Worker 绕物理网卡会受到 Windows 防火墙、VPN、RDP 及端点过滤策略
影响；连接数增加后尤其容易暴露问题。

4 GPU 那次执行在 `00:26:00` 明确进入 `succeeded`。约 16 小时后出现的
`forrtl error (200)` 是手动 Ctrl+C 关服务时，Windows 把控制台事件同时发送给
加载了 Intel native runtime 的 Worker 所致，并非 Cellpose 运行失败。

但是，运行期间的 `distributed.semaphore ... unknown lease ID` 是真实的数据安全
告警：默认 30 秒 Dask Lock 租约可能在长时间持有 GIL/本地代码写入期间过期，
从而让两个任务同时读改写同一个压缩 Zarr storage chunk。

## 已采用的修复

- 本机 `SpecCluster` 的 Scheduler、Nanny 和 Worker 全部绑定 `127.0.0.1`。
- 每个 GPU Worker 仍通过独立的 `CUDA_VISIBLE_DEVICES` 只看到一张物理 GPU。
- Worker 同时在 Windows 原生控制台层和 Python signal 层忽略 Driver Ctrl+C；
  Nanny 仍可通过进程句柄有序终止 Worker。
- Worker cache 清理改为每个 Worker 一个可取消、定向的普通 Dask task；不再使用
  超时后仍可在后台继续运行的 `Client.run` 广播。
- 查询 Worker 列表必须使用 `scheduler_info(n_workers=-1)`；Dask 默认只返回五个。
- Zarr partial-chunk correctness lock 使用不失效的租约，获取等待设为 300 秒；
  Worker 崩溃时选择失败而不是允许第二个写入者并发进入。失败/取消后重建本地
  Dask cluster，清除可能残留的无期限锁。
- 正常关闭失败时对 Nanny 子进程执行最终强制清理，避免旧 GPU 进程占用显存后又
  启动一套新集群。

## 验证方法

后端目录运行：

```powershell
.venv\Scripts\python.exe -m pytest tests\test_local_cluster_loopback.py -vv
```

该测试会实际启动 12 个 Windows 子进程：4 CPU Worker 和 8 个具有互异 GPU
可见性掩码的 GPU Worker。它验证 Scheduler/Worker 地址全部为
`tcp://127.0.0.1:*`、角色及掩码正确，然后有序关闭。

这个测试故意不导入 PyTorch/CUDA；它验证的是本次日志已证明发生在 CUDA 初始化
之前的进程和 TCP 拓扑故障。最终硬件验收必须在 8 GPU 目标机上执行一次真实
Cellpose workflow，确认：

1. 启动日志中的 Scheduler 和全部 Worker 地址均为 `127.0.0.1`。
2. Dashboard 显示 8 个 GPU Worker，且每个 Worker 的物理 GPU ID 不重复。
3. Cellpose 首批任务在八个 Worker 上运行并产生结果。
4. 日志中不再出现 `unknown lease ID`、`broadcast to 10.10.8.35` 或
   `forrtl error (200)`。
5. 对旧 4-GPU 运行生成的 Zarr 输出重新运行或做逐块校验。该旧运行发生过锁租约
   overbooking 告警，尽管执行状态为 succeeded，也不能仅凭状态保证没有丢写。

## 可调参数

`WorkFlow_ZARR_LOCK_ACQUIRE_TIMEOUT_SECONDS` 可调整 Zarr correctness lock 的等待
上限，默认 300 秒。不要恢复有限的 scheduler lock lease 来绕过超时；那会重新
引入压缩 Zarr chunk 的并发丢写风险。
