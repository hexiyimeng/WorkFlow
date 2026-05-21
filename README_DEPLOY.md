# BrainFlow 部署指南

## 环境要求

- Windows 10/11
- Python 3.10+
- Node.js 18+ (LTS recommended)
- Git

## 第一次安装

```powershell
# 1. 克隆代码后，在项目根目录运行：
.\scripts\setup.ps1

# 这将自动完成：
#   - 创建 .venv 虚拟环境
#   - 安装后端 Python 依赖
#   - 安装前端 Node 依赖
#   - 构建前端（生成 backend/dist/）
```

## 构建前端

```powershell
.\scripts\build_frontend.ps1
```

这会在 `backend/dist/` 生成前端资源。如果 `backend/dist/index.html` 不存在，浏览器将无法访问前端界面（后端 API 仍可正常使用）。

## 启动后端

```powershell
.\scripts\start.ps1
```

后端启动后访问：
- 本机: http://localhost:8000
- 局域网其他电脑: http://目标电脑IP:8000

**重要**：后端以单进程运行，不要使用 `uvicorn --workers` 多进程模式，否则 Dask 调度会出问题。

## GPU / Cellpose 可选安装

Cellpose 模型节点和 GPU 加速是可选的。没有安装时这些节点不可用，但不影响基础工作流。

```powershell
.\scripts\install_gpu.ps1
```

安装后重启后端：
```powershell
.\scripts\start.ps1
```

## 更新代码

```powershell
.\scripts\update.ps1
```

这会 `git pull`、更新依赖、并重新构建前端。

## 局域网访问

1. 确认目标电脑的 IP 地址（例如 `192.168.1.100`）
2. 防火墙放行 8000 端口：

```powershell
# 放行入站 8000（管理员权限）
New-NetFirewallRule -DisplayName "BrainFlow 8000" -Direction Inbound -Protocol TCP -LocalPort 8000 -Action Allow
```

3. 其他电脑浏览器打开 `http://192.168.1.100:8000`

## 开发模式

需要同时运行前端（Vite 热更新）和后端：

```powershell
# 方式 1：手动两个终端
# 终端 1:
.venv\Scripts\python.exe backend\main.py
# 终端 2:
cd frontend; npm run dev

# 方式 2：一键启动（会自动打开两个 PowerShell 窗口）
.\scripts\dev.ps1
```

Vite 开发模式默认连接 `http://localhost:8000`（API 代理已配置）。

## 常见问题

### 1. npm run build 失败

```powershell
# 先清空 node_modules 重装
Remove-Item -Recurse -Force frontend\node_modules
.\scripts\setup.ps1
```

### 2. backend/dist/index.html 不存在

```powershell
.\scripts\build_frontend.ps1
```

### 3. 前端页面打开但 API 连不上

- 确认后端在运行：`.\scripts\start.ps1`
- 确认没有其他进程占用 8000 端口：`netstat -ano | findstr 8000`
- 浏览器控制台查看 Network 是否有跨域错误

### 4. Cellpose / torch 没装

```powershell
.\scripts\install_gpu.ps1
.\scripts\start.ps1
```

### 5. 端口 8000 被占用

查找占用进程：
```powershell
netstat -ano | findstr 8000
```

结束进程（把 PID 换成实际值）：
```powershell
taskkill /PID <PID> /F
```

或改用其他端口（需要修改 backend/main.py 中的 uvicorn 启动参数）。
