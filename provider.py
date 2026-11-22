"""免费兜底数据源 provider: 分钟K / 全量分钟 / 五档盘口。

上游与单位 (2026-11-19 实测, 不按字段名推断):
  - 新浪 CN_MarketDataService.getKLineData (scale=1/5/15/30/60, datalen=1023)
      * volume 单位 = 股  → 内部契约要求手, 显式 floor(vol/100)
        (自证: 1 分钟 15300 股 × 1435.25 元 = 21951879 元 = 上游 amount)
      * amount 单位 = 元 (内部一致, 直接透传)
      * 深度: datalen=1023 根 ≈ 5 个交易日 (实测 2026-11-13 13:48 → 2026-11-19 15:00)
      * datetime 为北京时间墙钟 naive, 与内部契约一致
  - 腾讯 qt.gtimg.cn (买卖五档)
      * 买/卖档数量单位 = 手 (自证: 买一量 4 手 = 400 股; A 股最小单位 100 股)
      * 成交量单位 = 手; 成交额字段 "价/量/额" 第三段为元
      * 单请求可带数十只代码, 故按批解析

未声明 daily / adj_factor / realtime / financial → 这些能力自动回退 TickFlow 或 fuyao。
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from .client import SinaClient, TencentClient, UpstreamError

logger = logging.getLogger(__name__)

_DATASETS = ("minute", "depth5", "full_minute")

_TZ = ZoneInfo("Asia/Shanghai")
_FREQ_SCALE = {"1m": 1, "1min": 1, "5m": 5, "5min": 5, "15m": 15, "15min": 15,
               "30m": 30, "30min": 30, "60m": 60, "60min": 60}
_MAX_WORKERS = 6
_DEPTH_BATCH = 60
_MINUTE_CHUNK = 200

MINUTE_COLS = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]


@dataclass
class _CnfreeConfig:
    """轻量 config shim: 自定义加载器靠 config.datasets 识别能力声明。"""

    name: str = "cnfree"
    display_name: str = "免费兜底 (新浪+腾讯)"
    datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS))
    path: None = None
    builtin: bool = True


def availability() -> tuple[bool, str]:
    """可用性自检: 无 Key 依赖, 只检查依赖库是否可导入 (不做网络请求)。"""
    try:
        import httpx  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        return False, f"缺少依赖 httpx: {exc}"
    return True, "ok"


def _to_code(symbol: str) -> str | None:
    """内部 symbol (600519.SH) → 腾讯/新浪代码 (sh600519)。"""
    if not symbol:
        return None
    symbol = symbol.strip().upper()
    code, _, suffix = symbol.partition(".")
    if not code.isdigit():
        return None
    if suffix in ("SH", "SS"):
        return f"sh{code}"
    if suffix == "SZ":
        return f"sz{code}"
    if suffix == "BJ":
        return f"bj{code}"
    # 无后缀: 按代码段兜底
    if code.startswith(("60", "68", "9")):
        return f"sh{code}"
    if code.startswith(("00", "30", "2")):
        return f"sz{code}"
    if code.startswith(("4", "8")):
        return f"bj{code}"
    return None


def _f(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _wall(dt: datetime | None) -> datetime | None:
    """把带时区时间统一成北京墙钟 naive (宿主可能传 tz-aware 起止时间)。"""
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(_TZ).replace(tzinfo=None)
    return dt


def _epoch_ms(stamp: str) -> int | None:
    """腾讯时间戳 "20261119161455" → 毫秒 Unix 时间戳 (北京时间)。"""
    try:
        dt = datetime.strptime(stamp, "%Y%m%d%H%M%S").replace(tzinfo=_TZ)
    except (TypeError, ValueError):
        return None
    return int(dt.timestamp() * 1000)


class CnfreeProvider:
    """零成本兜底源: 分钟K/全量分钟 (新浪) + 五档盘口 (腾讯)。"""

    name = "cnfree"
    builtin = True
    # 1 分钟历史深度 (交易日): 新浪 datalen=1023 根 ≈ 5 个交易日
    minute_history_days = 5

    def __init__(self) -> None:
        self.config = _CnfreeConfig()
        self._sina = SinaClient()
        self._tencent = TencentClient()

    # ------------------------------------------------------------------ 分钟

    @staticmethod
    def _to_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
        if not rows:
            return pl.DataFrame()
        df = pl.DataFrame(rows)
        return df.select([
            pl.col("symbol").cast(pl.Utf8),
            pl.col("datetime").cast(pl.Datetime("us")),
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Float64),
            pl.col("amount").cast(pl.Float64),
        ]).sort(["symbol", "datetime"])

    def _symbol_frame(self, symbol: str, code: str, start: datetime | None,
                      end: datetime | None, scale: int) -> pl.DataFrame:
        """单只标的的分钟帧 (有界: 单标的最多 1023 根)。"""
        try:
            raw = self._sina.kline(code, scale=scale, datalen=1023)
        except UpstreamError as exc:
            logger.warning("cnfree 分钟取数失败 %s: %s", symbol, exc)
            return pl.DataFrame()
        start = _wall(start)
        end = _wall(end)
        rows: list[dict[str, Any]] = []
        for item_row in raw:
            day = str(item_row.get("day") or "")
            try:
                dt = datetime.strptime(day, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            if start is not None and dt < start:
                continue
            if end is not None and dt > end:
                continue
            volume = _f(item_row.get("volume"))
            rows.append({
                "symbol": symbol,
                "datetime": dt,
                "open": _f(item_row.get("open")),
                "high": _f(item_row.get("high")),
                "low": _f(item_row.get("low")),
                "close": _f(item_row.get("close")),
                # 新浪 volume 单位为股, 内部契约为手
                "volume": (volume // 100) if volume is not None else None,
                "amount": _f(item_row.get("amount")),
            })
        return self._to_frame(rows)

    def _chunk_frame(self, symbols: list[str], start: datetime | None, end: datetime | None,
                     scale: int) -> pl.DataFrame:
        """一批标的的分钟帧 (并发拉取, 逐只成帧后合并, 不堆全市场字典)。"""
        codes = {sym: code for sym in symbols if (code := _to_code(sym))}
        if not codes:
            return pl.DataFrame()
        frames: list[pl.DataFrame] = []
        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
            futures = [
                pool.submit(self._symbol_frame, symbol, code, start, end, scale)
                for symbol, code in codes.items()
            ]
            for future in futures:
                frame = future.result()
                if not frame.is_empty():
                    frames.append(frame)
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="vertical_relaxed")

    def _iter_chunks(self, symbols: list[str], start: datetime | None, end: datetime | None,
                     scale: int, on_chunk_done, total: int):
        """按标的分批取数并回报进度 (cur, total)。"""
        done = 0
        for start_idx in range(0, len(symbols), _MINUTE_CHUNK):
            chunk = symbols[start_idx:start_idx + _MINUTE_CHUNK]
            frame = self._chunk_frame(chunk, start, end, scale)
            done += len(chunk)
            if on_chunk_done is not None:
                on_chunk_done(done, total)
            yield chunk, frame

    def get_minute(self, symbols: list[str], start_time: datetime | None = None,
                   end_time: datetime | None = None, asset_type: str = "stock",
                   freq: str = "1m", on_chunk_done=None) -> pl.DataFrame:
        """分钟K: [symbol, datetime(北京墙钟), open, high, low, close, volume(手), amount(元)]。"""
        if asset_type not in ("stock", "etf"):
            # 股票 + ETF: 新浪对 ETF 同样有分钟线 (实测 sh513010 / sz159852 正常)。
            # 指数不走这里 —— 宿主明确拒绝指数分钟落库, 由 /api/index/minute 实时读取。
            return pl.DataFrame()
        scale = _FREQ_SCALE.get(str(freq).lower())
        if scale is None:
            return pl.DataFrame()
        symbols = list(symbols or [])
        total = len(symbols)
        frames: list[pl.DataFrame] = []
        for _chunk, frame in self._iter_chunks(symbols, start_time, end_time, scale,
                                               on_chunk_done, total):
            if not frame.is_empty():
                frames.append(frame)
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="vertical_relaxed")

    def iter_minute(self, symbols: list[str], start_time: datetime | None = None,
                    end_time: datetime | None = None, asset_type: str = "stock",
                    freq: str = "1m", on_chunk_done=None):
        """流式分钟帧: 逐批产出, 内存有界 (全市场历史同步用)。"""
        if asset_type not in ("stock", "etf"):
            return
        scale = _FREQ_SCALE.get(str(freq).lower())
        if scale is None:
            return
        symbols = list(symbols or [])
        total = len(symbols)
        for _chunk, frame in self._iter_chunks(symbols, start_time, end_time, scale,
                                               on_chunk_done, total):
            yield frame

    def get_intraday_batch(self, symbols: list[str], count: int = 300,
                           asset_type: str = "stock") -> pl.DataFrame:
        """全量分钟修复轮: 给定标的的当日 1 分钟K (canonical 8 列)。"""
        if asset_type not in ("stock", "etf"):
            return pl.DataFrame()
        today = datetime.now(_TZ).date()
        start = datetime.combine(today, datetime.min.time())
        end = datetime.combine(today, datetime.max.time())
        return self.get_minute(list(symbols or []), start, end, asset_type=asset_type, freq="1m")

    # ------------------------------------------------------------------ 五档

    def get_depth_batch(self, symbols: list[str]) -> dict[str, dict]:
        """五档盘口: {symbol: {bid_prices, bid_volumes, ask_prices, ask_volumes, timestamp}}。

        价格/数量数组均按一档到五档; 数量单位为手; timestamp 为毫秒 Unix 时间戳。
        """
        codes = {sym: code for sym in (symbols or []) if (code := _to_code(sym))}
        if not codes:
            return {}
        items = list(codes.items())
        out: dict[str, dict] = {}
        for start_idx in range(0, len(items), _DEPTH_BATCH):
            chunk = items[start_idx:start_idx + _DEPTH_BATCH]
            try:
                quotes = self._tencent.quotes([code for _, code in chunk])
            except UpstreamError as exc:
                logger.warning("cnfree 五档取数失败 (第 %d 批): %s",
                               start_idx // _DEPTH_BATCH + 1, exc)
                continue
            for symbol, code in chunk:
                quote = quotes.get(code)
                if not quote:
                    continue
                bid_prices: list[float] = []
                bid_volumes: list[float] = []
                ask_prices: list[float] = []
                ask_volumes: list[float] = []
                # **空档必须保留**: 腾讯对「封死涨停」的票给的是卖五档全 0.00/0
                # (卖档整体为空), 宿主 depth_service 正是靠
                #   ask_volumes[0] == 0  → 涨停真封
                #   bid_volumes[0] == 0  → 跌停真封
                # 来判定的 —— 一旦把空档丢掉, 真封板会退化成「待确认」(None),
                # 涨停梯队就全是「待确认」。价格 0.00 原样保留, 只丢格式非法的项。
                for price, volume in quote.get("bid") or []:
                    p, v = _f(price), _f(volume)
                    if p is None:
                        continue
                    bid_prices.append(p)
                    bid_volumes.append(v or 0.0)
                for price, volume in quote.get("ask") or []:
                    p, v = _f(price), _f(volume)
                    if p is None:
                        continue
                    ask_prices.append(p)
                    ask_volumes.append(v or 0.0)
                # 两侧全无有效报价(停牌/无效代码)才丢弃
                if not any(p > 0 for p in bid_prices) and not any(p > 0 for p in ask_prices):
                    continue
                out[symbol] = {
                    "bid_prices": bid_prices[:5],
                    "bid_volumes": bid_volumes[:5],
                    "ask_prices": ask_prices[:5],
                    "ask_volumes": ask_volumes[:5],
                    "timestamp": _epoch_ms(quote.get("time") or ""),
                }
        return out

    # ------------------------------------------------------------------ 辅助

    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        """设置页「试拉」: 有界样本请求, 不触发全量同步。"""
        probe = list(symbols or [])[:5] or ["600519.SH", "000001.SZ", "300750.SZ"]
        try:
            if dataset == "depth5":
                data = self.get_depth_batch(probe)
                rows = [{"symbol": k, **v} for k, v in data.items()]
                return {"provider": self.name, "dataset": dataset, "rows": len(rows),
                        "columns": ["symbol", "bid_prices", "bid_volumes", "ask_prices",
                                    "ask_volumes", "timestamp"],
                        "preview": rows[:5]}
            if dataset in ("minute", "full_minute"):
                today = datetime.now(_TZ).date()
                start = datetime.combine(today, datetime.min.time())
                end = datetime.combine(today, datetime.max.time())
                df = self.get_minute(probe, start, end, freq="1m")
                return {"provider": self.name, "dataset": dataset, "rows": df.height,
                        "columns": list(df.columns), "preview": df.head(5).to_dicts()}
            return {"provider": self.name, "dataset": dataset, "rows": 0, "columns": [],
                    "preview": [], "error": "本插件不提供该数据集"}
        except Exception as exc:  # noqa: BLE001
            return {"provider": self.name, "dataset": dataset, "rows": 0, "columns": [],
                    "preview": [], "error": f"{type(exc).__name__}: {exc}"}

    def close(self) -> None:
        for client in (self._sina, self._tencent):
            try:
                client.close()
            except Exception:  # noqa: BLE001
                logger.warning("cnfree 关闭客户端失败", exc_info=False)
