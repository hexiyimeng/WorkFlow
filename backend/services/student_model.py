#!/usr/bin/env python3
"""学生模型: CPDINO 最小改动轻量版 —— backbone 换 dinov3 ViT-S/16 (~22M 参数, vitb 86M 的 1/4)。

默认加载 dinov3 官方预训练权重 (pretrained=True); 加载已训练 checkpoint 时
(patch_cellpose_student 路径) 显式传 pretrained=False, 避免无谓下载。
其余结构 (ps=8 stride, out head, W2, forward, save_model) 与 CPDINO 完全一致,
保存的权重为 CP4 格式; 通过 patch_cellpose_student() 后可用 cellpose CellposeModel 直接加载推理。
"""
from pathlib import Path

import torch
import torch.nn as nn
from cellpose.vit import BaseModel, CPDINO
from dinov3.hub.backbones import dinov3_vits16

STUDENT_FEAT_DIM = 384  # dinov3 vits16 embed dim, 用于识别学生权重

# 官方 URL 被区域封锁时使用的本地预训练权重 (ModelScope 镜像转换而来)
PRETRAINED_VITS16 = Path(__file__).resolve().parent / "models" / "dinov3_vits16_pretrain_lvd1689m-08c60483.pth"


def _vits16_pretrained():
    """加载 dinov3 vits16 预训练权重: 优先项目 models/ 下本地文件, 否则走官方 URL 下载。"""
    if PRETRAINED_VITS16.exists():
        return dinov3_vits16(pretrained=True, weights=str(PRETRAINED_VITS16))
    return dinov3_vits16(pretrained=True)


class CPDINO_S(CPDINO):
    """dinov3 ViT-S/16 版 CPDINO, 接口/保存格式与官方 CP4 相同。"""

    def __init__(self, ps=8, nout=3, bsize=384, rdrop=0.4, dtype=torch.float32,
                 pretrained=True):
        BaseModel.__init__(self, dtype=dtype)
        self.rdrop = rdrop
        self.backbone = "dino_vits"
        self.model_name = "vits"
        self.encoder = (_vits16_pretrained() if pretrained
                        else dinov3_vits16(pretrained=False)).to(self._dtype)
        # 与 CPDINO 相同: 降低 stride 提高分辨率
        self.encoder.patch_embed.proj.stride = (ps, ps)
        self.encoder.patch_embed.proj.padding = (ps // 2, ps // 2) if ps < 16 else (0, 0)
        feat_dim = self.encoder.patch_embed.proj.weight.shape[0]
        assert feat_dim == STUDENT_FEAT_DIM
        self.out = nn.Linear(feat_dim, nout * ps**2, dtype=self._dtype)
        self.W2 = nn.Parameter(
            torch.eye(nout * ps**2, dtype=self._dtype).reshape(nout * ps**2, nout, ps, ps),
            requires_grad=False,
        )
        self.nout = nout
        self.ps = ps


def is_student_checkpoint(path):
    d = torch.load(path, map_location="cpu", mmap=True, weights_only=True)
    return (
        "encoder.cls_token" in d
        and d["encoder.patch_embed.proj.weight"].shape[0] == STUDENT_FEAT_DIM
    )


def patch_cellpose_student():
    """让 cellpose.models.CellposeModel 能识别并构建学生模型 (dino_vits backbone)。"""
    import cellpose.models as M

    if getattr(M, "_student_patched", False):
        return
    _orig_get_backbone = M.get_backbone

    def get_backbone(pretrained_model):
        if is_student_checkpoint(pretrained_model):
            return "dino_vits"
        return _orig_get_backbone(pretrained_model)

    _orig_CPDINO = M.CPDINO

    def CPDINO_factory(model_name="vitl", dtype=torch.float32):
        if model_name == "vits":
            return CPDINO_S(dtype=dtype, pretrained=False)
        return _orig_CPDINO(model_name=model_name, dtype=dtype)

    M.get_backbone = get_backbone
    M.CPDINO = CPDINO_factory
    M._student_patched = True


if __name__ == "__main__":
    # 自检: 前向 + 保存 + 加载
    torch.manual_seed(0)
    net = CPDINO_S(dtype=torch.bfloat16, pretrained=False)
    n = sum(p.numel() for p in net.parameters())
    print(f"学生模型参数: {n/1e6:.1f}M")
    net = net.to("cuda")
    x = torch.randn(2, 2, 384, 384, device="cuda", dtype=torch.bfloat16)
    with torch.autocast(device_type="cuda", dtype=torch.bfloat16):
        y, _ = net(x)
    print("前向输出:", y.shape, y.dtype)

    path = "/tmp/opencode/student_test.pth"
    net = net.to("cpu")
    net.dtype = torch.float32
    net.save_model(path)

    patch_cellpose_student()
    from cellpose import models
    m = models.CellposeModel(pretrained_model=path, gpu=True)
    print("CellposeModel 加载成功:", m.pretrained_model, "backbone:", m.net.backbone)
    import numpy as np
    img = np.random.rand(12, 96, 96, 2).astype(np.float32)
    masks, flows, _ = m.eval(img, z_axis=0, channel_axis=3, normalize=False, do_3D=True)
    print("3D eval 输出 masks:", masks.shape, "n=", masks.max())
