"""打包测试：验证非 editable 安装（pip wheel .）装出 flowlens 的全部子包。

背景：pyproject.toml 曾手工列举 packages（只写了 flowlens 与 flowlens.sketch），
漏了 flowlens.api，导致 `pip install .`（Docker、任何非 editable 安装）缺 api 子包、
容器起不来（ModuleNotFoundError: No module named 'flowlens.api'），见 B1 交接记录。
本测试在临时目录用 `pip wheel . --no-deps` 真实构建安装包，把其中包含的
flowlens/**/*.py 与源码目录的文件清单比对，确保没有遗漏——用清单比对而非写死
文件名，这样以后新增模块也能被自动覆盖。

构建需联网装 build backend（setuptools），耗时可能十几秒，故标 slow：日常
pytest -q 跳过，pytest -q -m slow 才跑。
"""

import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

# 项目根（tests/ 的上一级），pip wheel 的构建目录
ROOT = Path(__file__).resolve().parent.parent


def _source_py_files() -> set[str]:
    """源码 flowlens/ 下所有 .py 的路径集合（用 wheel 内的路径形式：flowlens/...）。"""
    return {str(p.relative_to(ROOT)).replace("\\", "/")
            for p in (ROOT / "flowlens").rglob("*.py")}


def _cleanup_build_dir() -> None:
    """pip wheel 会把源码拷进项目根的 build/（setuptools 构建副产物），测完清掉，
    避免留在工作区（.gitignore 未覆盖 build/）。只在目录存在时删。"""
    b = ROOT / "build"
    if b.is_dir():
        shutil.rmtree(b, ignore_errors=True)


def _wheel_py_files(wheel: Path) -> set[str]:
    """wheel 里 flowlens/ 下所有 .py 的路径集合。"""
    with zipfile.ZipFile(wheel) as z:
        return {n for n in z.namelist()
                if n.endswith(".py") and n.startswith("flowlens/")}


@pytest.mark.slow
def test_wheel_contains_every_source_py(tmp_path):
    out = tmp_path / "wheel"
    out.mkdir()
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "pip", "wheel", ".", "--no-deps", "-w", str(out)],
            cwd=ROOT, capture_output=True, text=True, timeout=300,
        )
        assert proc.returncode == 0, f"pip wheel 构建失败:\n{proc.stdout}\n{proc.stderr}"
        wheels = list(out.glob("*.whl"))
        assert len(wheels) == 1, f"应恰好构建出一个 wheel，实际 {wheels}"

        have = _wheel_py_files(wheels[0])
        want = _source_py_files()
        missing = want - have
        assert not missing, f"wheel 缺少源码中的 .py 文件（子包未打包）: {sorted(missing)}"
        extra = have - want
        assert not extra, f"wheel 含源码里不存在的 flowlens .py（异常打包）: {sorted(extra)}"
    finally:
        _cleanup_build_dir()
