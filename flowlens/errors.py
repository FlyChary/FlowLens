"""全项目共用的异常类。

各接口只抛这些异常、不自己写状态码，由 api 层统一转成 {"error": ...} 与对应
HTTP 状态码（T6）。model、engine 等模块也会抛这些异常，所以放在 flowlens 根层
而不是 api 子包下，api 层直接引用即可。
"""


class FlowLensError(Exception):
    """基类：带 HTTP 状态码（status）与默认错误信息（message）。"""

    status: int = 500
    message: str = "internal error"

    def __init__(self, message: str | None = None) -> None:
        if message is not None:
            self.message = message
        super().__init__(self.message)


class BadRequest(FlowLensError):
    """请求体或查询参数不合法。"""

    status = 400
    message = "bad request"


class TooOld(FlowLensError):
    """查询区间早于保留期。"""

    status = 422
    message = "range too old"


class PayloadTooLarge(FlowLensError):
    """请求体过大。"""

    status = 413
    message = "payload too large"


class UnsupportedEncoding(FlowLensError):
    """不支持的 Content-Encoding。"""

    status = 415
    message = "unsupported encoding"


class Overloaded(FlowLensError):
    """过载：整体拒绝写入，附 Retry-After 秒数。"""

    status = 503
    message = "overloaded"

    def __init__(self, message: str | None = None, retry_after: int = 1) -> None:
        super().__init__(message)
        self.retry_after = retry_after
