"""公开 HTTP 行情客户端: 新浪分钟 K 线 + 腾讯五档快照。

只依赖 httpx (宿主已内置); 不 import 宿主私有模块, 保证外部插件包可独立加载。
所有单位换算集中在 provider 层, 本层只做取数与结构化, 保留上游原值。
"""

from __future__ import annotations

import json
import re
from typing import Any

import httpx

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# 新浪 K 线返回 JSONP: var _data=([...]);
_SINA_JSONP = re.compile(r"=\((\[.*\])\)\s*;?\s*$", re.S)
# 腾讯快照每只一行: v_sh600519="1~名称~...";
_TENcent_LINE = re.compile(r'v_([a-z]{2}\d{6})="([^"]*)"')


class UpstreamError(RuntimeError):
    """上游接口明确失败 (网络/协议/限频); 调用方须显式上报, 不转成空数据。"""


class SinaClient:
    """新浪分钟 K 线。"""

    KLINE_URL = (
        "https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20_data=/"
        "CN_MarketDataService.getKLineData"
    )

    def __init__(self, timeout: float = 12.0) -> None:
        self._client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": _UA, "Referer": "https://finance.sina.com.cn"},
        )

    def kline(self, code: str, scale: int = 1, datalen: int = 1023) -> list[dict[str, Any]]:
        """取单只标的的分钟 K 线。

        返回原始字段: day/open/high/low/close/volume/amount (volume=股, amount=元)。
        """
        params = {"symbol": code, "scale": int(scale), "ma": "no", "datalen": int(datalen)}
        try:
            resp = self._client.get(self.KLINE_URL, params=params)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise UpstreamError(f"新浪 K 线请求失败: {exc}") from exc
        text = resp.text
        match = _SINA_JSONP.search(text.strip())
        if not match:
            # 上游对无效代码返回跳转脚本, 不是协议错误
            if "location.href" in text or "<html" in text.lower():
                return []
            raise UpstreamError("新浪 K 线响应格式无法解析")
        try:
            rows = json.loads(match.group(1))
        except json.JSONDecodeError as exc:
            raise UpstreamError("新浪 K 线响应 JSON 解析失败") from exc
        return [r for r in rows if isinstance(r, dict) and r.get("day")]

    def close(self) -> None:
        with_suppress = getattr(self._client, "close", None)
        if callable(with_suppress):
            with_suppress()


class TencentClient:
    """腾讯快照 (实时价 + 买卖五档)。"""

    QUOTE_URL = "https://qt.gtimg.cn/q="

    def __init__(self, timeout: float = 10.0) -> None:
        self._client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": _UA, "Referer": "https://gu.qq.com/"},
        )

    def quotes(self, codes: list[str]) -> dict[str, dict[str, Any]]:
        """批量取快照。codes 形如 ["sh600519", "sz000001"]。

        返回 {code: {"name","last","prev_close","open","high","low","volume","amount",
                     "pct","change","bid":[[price,vol]*5], "ask":[[price,vol]*5],
                     "time":"20261119161455"}}; 无效代码不出现。
        """
        if not codes:
            return {}
        url = self.QUOTE_URL + ",".join(codes)
        try:
            resp = self._client.get(url)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise UpstreamError(f"腾讯快照请求失败: {exc}") from exc
        try:
            text = resp.content.decode("gbk", errors="replace")
        except Exception as exc:  # noqa: BLE001
            raise UpstreamError("腾讯快照解码失败") from exc

        out: dict[str, dict[str, Any]] = {}
        for code, payload in _TENcent_LINE.findall(text):
            fields = payload.split("~")
            if len(fields) < 40:
                continue
            out[code] = {
                "name": fields[1],
                "last": fields[3],
                "prev_close": fields[4],
                "open": fields[5],
                "volume": fields[6],
                "bid": [[fields[9 + 2 * i], fields[10 + 2 * i]] for i in range(5)],
                "ask": [[fields[19 + 2 * i], fields[20 + 2 * i]] for i in range(5)],
                "time": fields[30],
                "change": fields[31],
                "pct": fields[32],
                "high": fields[33],
                "low": fields[34],
                # 字段35 形如 "价格/成交量(手)/成交额(元)"; 字段37 为成交额(万元)
                "amount": fields[35].split("/")[2] if "/" in fields[35] else None,
            }
        return out

    def close(self) -> None:
        closer = getattr(self._client, "close", None)
        if callable(closer):
            closer()
