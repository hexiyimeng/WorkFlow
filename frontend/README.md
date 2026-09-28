# WorkFlow 前端

需要 Node.js。项目在 `frontend/` 下运行：

```bash
npm ci
npm run dev
```

开发服务器默认连接 `http://localhost:8000`。如果后端地址不同，可设置
`VITE_API_BASE_URL` 和 `VITE_WS_URL`。

提交前检查：

```bash
npm run test
npm run lint
npm run build
```

`npm run build` 会把页面生成到 `backend/dist/`；Slurm 安装脚本只校验此目录，
不会在计算集群上重新构建前端。
