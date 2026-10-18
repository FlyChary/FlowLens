"""e2e 测试的会话级清理（T8b）。

aiohttp 的全局中间件缓存 `_cached_build_middleware`（web_app.py 里
`lru_cache(maxsize=1024)(_build_middlewares)`）以 `(handler, match_info.apps)` 为键，
而 `apps` 是**含 Application 的元组**——每个 create_app 都把那个 Application（连带它
的 Engine：去重 Bloom 33.5 MB + 各维度 CMS 19.7 MB + HLL 1.1 MB ≈ 55 MB）用一个
**强引用**挂进这个进程级缓存。e2e 测试每个都 asyncio.run 起一个真实服务器（各自
create_app），测试结束 AppRunner 关掉，但 Application 仍被这个缓存引用着、不被回收。
全量 pytest 在同一进程逐个跑 e2e 测试时，进程内存每个测试累积约 55 MB（实测到第 4
个就超 256 MB × 0.9），之前因此把降级控制器打进第 4 级、所有写入 503（T8b 背景）。

这里在**每个 e2e 测试结束后**清掉该缓存，让 Application 可被回收。生产只建一个 app
（缓存就一条）、长驻运行，不受影响；这里只为"一个进程里反复建、反复拆 app"的测试
场景服务。
"""

import gc

import pytest

# 指向 aiohttp 的私有名；用 getattr 守卫，将来 aiohttp 改名时不会让 e2e 全挂
_MW_CACHE_ATTR = "_cached_build_middleware"


def _release_apps() -> None:
    """清掉 aiohttp 全局中间件 lru_cache，释放本测试创建的 Application（连带 Engine）。"""
    try:
        import aiohttp.web_app as wa
    except Exception:
        return
    cache = getattr(wa, _MW_CACHE_ATTR, None)
    clear = getattr(cache, "cache_clear", None)
    if clear is not None:
        clear()
    gc.collect()


@pytest.fixture(autouse=True)
def _drop_aiohttp_middleware_cache():
    """每个 e2e 测试结束后清 aiohttp 中间件缓存，避免 Application/Engine 被全局缓存
    长期引用导致进程内存逐测增长（见本文件 docstring）。"""
    yield
    _release_apps()
