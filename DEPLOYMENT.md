# WorkFlow Docker GPU Deployment

This guide assumes the deployment machine has only these tools installed:

- Git
- Docker / Docker Desktop with Docker Compose
- NVIDIA driver, when running with GPU

You do not need to install Python, Node.js, uv, PyTorch, Cellpose, or npm
packages on the host machine. Docker builds and runs them inside a Linux
container.

## 1. Clone The Repository

```bash
git clone https://github.com/hexiyimeng/WorkFlow.git
cd WorkFlow
```

If the repository already exists on the machine:

```bash
cd WorkFlow
git pull
```

## 2. Check Docker

Check that Docker is using Linux containers:

```bash
docker info --format "{{.OSType}}"
```

Expected output:

```text
linux
```

Check Docker Compose:

```bash
docker compose version
```

## 3. Check GPU Access

Check the host GPU:

```bash
nvidia-smi
```

Check that Docker can access the GPU:

```bash
docker run --rm --gpus all nvidia/cuda:12.8.0-base-ubuntu24.04 nvidia-smi
```

Do not continue until this command works. If it fails, fix the NVIDIA driver,
Docker Desktop WSL2 backend, or NVIDIA Container Toolkit first.

## 4. Add Cellpose Models

Model files are not stored in GitHub. Create the model directory yourself:

Linux/macOS:

```bash
mkdir -p backend/models/cellpose
```

Windows PowerShell:

```powershell
New-Item -ItemType Directory -Force .\backend\models\cellpose
```

Put the Cellpose model file under:

```text
backend/models/cellpose/
```

Example:

```text
backend/models/cellpose/cpsam.pth
```

The Docker Compose file mounts this host directory into the container:

```text
./backend/models -> /app/backend/models
```

The backend code will find Cellpose models at:

```text
/app/backend/models/cellpose/
```

## 5. Start With GPU

From the project root:

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up --build -d
```

The first build can take a long time because Docker downloads and installs:

- Linux base images
- frontend npm dependencies
- backend Python dependencies
- CUDA-enabled PyTorch
- Cellpose dependencies

## 6. Open WorkFlow

Open the app:

```text
http://localhost:8000
```

Dask dashboard:

```text
http://localhost:8787
```

If deploying on a remote server, replace `localhost` with the server IP or
domain.

## 7. View Logs

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml logs -f workflow
```

Useful startup lines:

```text
[Dask] Startup plan ... cuda_mode=gpu
[Dask] Starting GPU mode
Dask Cluster Ready
```

If the logs show `cuda_mode=cpu`, the container did not detect the GPU.

## 8. Verify PyTorch GPU In The Container

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml exec workflow python -c "import torch; print(torch.__version__); print(torch.cuda.is_available()); print(torch.cuda.device_count()); print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'no cuda')"
```

Expected result:

```text
True
1
<your NVIDIA GPU name>
```

The exact PyTorch version and GPU name can vary.

## 9. Stop And Restart

Stop:

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml down
```

Start again without rebuilding:

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d
```

Rebuild after dependency or Dockerfile changes:

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up --build -d
```

## 10. Update From GitHub

```bash
git pull
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up --build -d
```

Your local model files under `backend/models/` are ignored by Git and will not
be overwritten by normal code updates.

## 11. Useful Commands

Show running containers:

```bash
docker compose ps
```

Enter the container:

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml exec workflow bash
```

Recreate from scratch:

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml down
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up --build -d
```

Remove the Dask spill volume:

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml down -v
```

## 12. CPU Mode

GPU mode is the intended deployment mode for Cellpose. CPU mode is available
for basic startup checks:

```bash
docker compose up --build -d
```

CPU mode sets:

```text
WorkFlow_CUDA_MODE=disabled
```

Cellpose segmentation can be very slow without GPU.

## 13. Troubleshooting

### Docker Cannot See GPU

Run:

```bash
docker run --rm --gpus all nvidia/cuda:12.8.0-base-ubuntu24.04 nvidia-smi
```

If this fails, the problem is outside WorkFlow. Fix Docker GPU support before
starting the app.

### Cellpose Model Not Found

Check that the model exists on the host:

```text
backend/models/cellpose/
```

Check that the container sees it:

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml exec workflow ls -la /app/backend/models/cellpose
```

### Port Already In Use

If port `8000` or `8787` is already used, change the left side of the port
mapping in `docker-compose.yml`.

Example:

```yaml
ports:
  - "18000:8000"
  - "18787:8787"
```

Then open:

```text
http://localhost:18000
```
