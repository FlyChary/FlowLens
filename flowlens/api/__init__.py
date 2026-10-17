"""HTTP 接口层：路由、参数解析、统一错误处理。

各接口只抛 flowlens.errors 的异常，不自己写状态码；中间件统一转成
{"error": ...} 与对应 HTTP 状态码。
"""

from .app import create_app

__all__ = ["create_app"]
