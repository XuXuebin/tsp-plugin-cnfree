"""cnfree 契约自测（不联网、不需要 Key）。

覆盖宿主插件开发指南要求的关键契约：
  - 五档: **空档必须保留**（封死涨停时腾讯卖档整体为空 → ask_volumes[0] == 0 → 真封板）
  - 分钟: volume 股 → 手（÷100），amount 元，datetime 北京墙钟 naive
  - 能力声明: 未声明的数据集不接（指数）
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl


def _load_plugin(ns: str):
    """按宿主的方式加载插件包(命名空间包 + 相对导入), 与外部插件 loader 一致。"""
    import importlib
    import sys
    import types
    root = Path(__file__).resolve().parent.parent
    package = types.ModuleType(ns)
    package.__path__ = [str(root)]
    package.__package__ = ns
    sys.modules[ns] = package
    return importlib.import_module(f"{ns}.provider")

_mod = _load_plugin("cnfree_plugin")
CnfreeProvider = _mod.CnfreeProvider

FAKE_QUOTES = {
    # 封死涨停: 买一 13.39 × 152499 手, 卖五档整体为空 (0.00/0)
    "sz002579": {
        "name": "中京电子", "last": "13.39", "prev_close": "12.17", "open": "13.40",
        "volume": "2109800", "bid": [["13.39", "152499"], ["13.38", "1857"]],
        "ask": [["0.00", "0"]] * 5, "time": "20261120100000",
        "change": "1.22", "pct": "10.02", "high": "13.40", "low": "13.30",
        "amount": "0",
    },
    # 正常票: 卖一有量
    "sh600879": {
        "name": "航天电子", "last": "11.06", "prev_close": "11.54", "open": "11.20",
        "volume": "132314", "bid": [["10.93", "510"]],
        "ask": [["10.94", "222"], ["10.95", "5304"]], "time": "20261120100000",
        "change": "-0.48", "pct": "-4.16", "high": "11.33", "low": "10.94",
        "amount": "148097807",
    },
}


def _fake_quotes(codes):
    return {c: FAKE_QUOTES[c] for c in codes if c in FAKE_QUOTES}


def test_depth5_keeps_empty_levels():
    """封死涨停 → 卖档为空 → ask_volumes 保留 5 个 0 → 宿主判定 ask_volumes[0]==0 → 真封板。"""
    p = CnfreeProvider()
    p._tencent.quotes = _fake_quotes
    p._tencent.close = lambda: None
    out = p.get_depth_batch(["002579.SZ", "600879.SH"])
    sealed = out["002579.SZ"]
    assert len(sealed["ask_volumes"]) == 5, sealed
    assert sealed["ask_volumes"][0] == 0.0, sealed
    assert sealed["ask_prices"][0] == 0.0, sealed
    assert sealed["bid_volumes"][0] == 152499.0, sealed
    normal = out["600879.SH"]
    assert normal["ask_volumes"][0] == 222.0, normal
    assert normal["ask_volumes"][0] != 0.0
    p.close()


def test_minute_volume_shares_to_lots():
    """分钟 volume 上游为股 → ÷100 转手；amount 为元；datetime 为北京墙钟 naive。"""
    p = CnfreeProvider()

    class _FakeSina:
        def close(self):
            pass

        def kline(self, code, scale=1, datalen=1023):
            return [{"day": "2026-11-20 09:31:00", "open": "12.20", "high": "12.20",
                     "low": "12.20", "close": "12.20", "volume": "15300", "amount": "21951879"}]

    p._sina = _FakeSina()
    df = p.get_minute(["000001.SZ"], None, None, asset_type="stock", freq="1m")
    assert df.height == 1, df
    row = df.to_dicts()[0]
    assert row["volume"] == 153.0, row          # 15300 股 → 153 手
    assert row["amount"] == 21951879.0, row
    assert list(df.columns) == ["symbol", "datetime", "open", "high", "low",
                                "close", "volume", "amount"], df.columns
    assert df.schema["datetime"] == pl.Datetime("us"), df.schema
    assert df["datetime"][0].hour == 9 and df["datetime"][0].minute == 31
    p.close()


def test_declares_only_supported_datasets():
    p = CnfreeProvider()
    assert set(p.config.datasets) == {"minute", "depth5", "full_minute"}, p.config.datasets
    assert p.name == "cnfree"
    p.close()


def _run_all():
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  PASS {name}")


if __name__ == "__main__":
    _run_all()
    print("cnfree 契约自测全部通过")
