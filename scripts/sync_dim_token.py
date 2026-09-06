#!/usr/bin/env python3
"""Sync dim_bsc_token and dim_bsc_token_price_hourly into ClickHouse.

Local usage:
  export CHAINLAKE_WRITER_USERNAME=chainlake_writer
  export CHAINLAKE_WRITER_PASSWORD=...
  export CLICKHOUSE_URL=http://localhost:8123     # or kubectl port-forward
  export BSC_RPC_URL=http://localhost:4000/main/evm/bsc
  python scripts/sync_dim_token.py

In production this runs as a Kubernetes CronJob (see
chainlake-infra/data/olap/clickhouse/single-node/dim-token-sync-cronjob.yaml).
The script in that CronJob is identical; keep the two in sync.

Data sources (priority order):
  1. PancakeSwap extended token list -- symbol/decimals/logo for main BSC tokens
  2. DeFiLlama coins API             -- gap-fill metadata + hourly price snapshots
  3. BSC RPC eth_call                -- last-resort for tokens DeFiLlama doesn't track
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

CH_URL = os.environ.get(
    "CLICKHOUSE_URL",
    "http://clickhouse-chainlake-ch.clickhouse.svc.cluster.local:8123",
)
CH_USER = os.environ["CHAINLAKE_WRITER_USERNAME"]
CH_PASS = os.environ["CHAINLAKE_WRITER_PASSWORD"]
BSC_RPC = os.environ.get(
    "BSC_RPC_URL",
    "http://erpc.ingestion.svc.cluster.local:4000/main/evm/bsc",
)

PANCAKE_LIST_URL = "https://tokens.pancakeswap.finance/pancakeswap-extended.json"
DEFILLAMA_BATCH = 100
RPC_FALLBACK_CAP = 50


def _get(url: str, timeout: int = 30, retries: int = 3) -> bytes:
    req = urllib.request.Request(
        url, headers={"User-Agent": "chainlake-dim-token-sync/1.0"}
    )
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = 5 * (attempt + 1)
            print(f"  GET {url[:60]}... failed ({e}), retry in {wait}s")
            time.sleep(wait)


def _ch_query(sql: str, retries: int = 5) -> list[dict]:
    params = urllib.parse.urlencode({"query": sql + " FORMAT JSON"})
    req = urllib.request.Request(f"{CH_URL}/?{params}")
    req.add_header("X-ClickHouse-User", CH_USER)
    req.add_header("X-ClickHouse-Key", CH_PASS)
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())["data"]
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = 10 * (attempt + 1)
            print(f"  CH query failed ({e}), retry in {wait}s")
            time.sleep(wait)


def _ch_insert(table: str, rows: list[dict], retries: int = 5) -> None:
    if not rows:
        return
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows).encode()
    params = urllib.parse.urlencode(
        {"query": f"INSERT INTO {table} FORMAT JSONEachRow"}
    )
    for attempt in range(retries):
        req = urllib.request.Request(
            f"{CH_URL}/?{params}", data=body, method="POST"
        )
        req.add_header("X-ClickHouse-User", CH_USER)
        req.add_header("X-ClickHouse-Key", CH_PASS)
        req.add_header("Content-Type", "application/x-ndjson")
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                resp = r.read()
            if resp and b"Exception" in resp:
                raise RuntimeError(f"ClickHouse INSERT error: {resp[:500]}")
            return
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = 10 * (attempt + 1)
            print(f"  CH insert {table} failed ({e}), retry in {wait}s")
            time.sleep(wait)


def _abi_str(hex_result: str) -> str:
    """Decode ABI-encoded string or bytes32 return value from eth_call."""
    data = bytes.fromhex(hex_result.removeprefix("0x"))
    if len(data) < 32:
        return ""
    offset = int.from_bytes(data[:32], "big")
    if offset == 32 and len(data) >= 64:
        # Standard ABI dynamic string
        length = int.from_bytes(data[32:64], "big")
        if 64 + length <= len(data):
            return data[64 : 64 + length].decode("utf-8", errors="replace").strip("\x00")
    # bytes32 fallback (older tokens: MKR, SAI use bytes32 for symbol/name)
    return data[:32].rstrip(b"\x00").decode("utf-8", errors="replace")


def _abi_uint8(hex_result: str) -> int:
    data = bytes.fromhex(hex_result.removeprefix("0x"))
    return data[-1] if data else 18


def _rpc(method: str, params: list, timeout: int = 15) -> object:
    body = json.dumps(
        {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    ).encode()
    req = urllib.request.Request(BSC_RPC, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read()).get("result")


def _token_from_rpc(addr: str) -> dict | None:
    try:
        sym = _rpc("eth_call", [{"to": addr, "data": "0x95d89b41"}, "latest"])
        dec = _rpc("eth_call", [{"to": addr, "data": "0x313ce567"}, "latest"])
        nam = _rpc("eth_call", [{"to": addr, "data": "0x06fdde03"}, "latest"])
        if not sym or sym == "0x":
            return None
        return {
            "address": addr,
            "symbol": _abi_str(sym)[:32],
            "name": _abi_str(nam)[:128] if nam and nam != "0x" else "",
            "decimals": _abi_uint8(dec) if dec and dec != "0x" else 18,
            "coingecko_id": "",
            "logo_uri": "",
        }
    except Exception as e:
        print(f"    rpc {addr}: {e}")
        return None


def _defillama_prices(addresses: list[str]) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for i in range(0, len(addresses), DEFILLAMA_BATCH):
        batch = addresses[i : i + DEFILLAMA_BATCH]
        coins = ",".join(f"bsc:{a}" for a in batch)
        url = f"https://coins.llama.fi/prices/current/{urllib.parse.quote(coins, safe=':,')}"
        try:
            data = json.loads(_get(url, timeout=20))
            for key, val in data.get("coins", {}).items():
                addr = key.split(":")[-1].lower()
                result[addr] = val
        except Exception as e:
            print(f"  defillama batch {i // DEFILLAMA_BATCH}: {e}")
        time.sleep(0.3)
    return result


def _pancake_tokens() -> list[dict]:
    data = json.loads(_get(PANCAKE_LIST_URL, timeout=30))
    return [
        {
            "address": t["address"].lower(),
            "symbol": t.get("symbol", "")[:32],
            "name": t.get("name", "")[:128],
            "decimals": int(t.get("decimals", 18)),
            "coingecko_id": "",
            "logo_uri": t.get("logoURI", "")[:512],
        }
        for t in data.get("tokens", [])
        if t.get("chainId") == 56
    ]


def main() -> None:
    print("[1/4] PancakeSwap token list...")
    try:
        rows = _pancake_tokens()
        _ch_insert("chainlake.dim_bsc_token", rows)
        print(f"  upserted {len(rows)} tokens")
    except Exception as e:
        print(f"  skipped: {e}")

    print("[2/4] Finding gap tokens (in bsc_token_transfer but missing from dim)...")
    unknown = _ch_query("""
        SELECT DISTINCT lower(token_address) AS a
        FROM chainlake.bsc_token_transfer
        WHERE token_address IS NOT NULL
          AND token_address != ''
          AND lower(token_address) NOT IN (
              SELECT address FROM chainlake.dim_bsc_token FINAL
          )
        LIMIT 500
    """)
    gap_addrs = [r["a"] for r in unknown]
    print(f"  {len(gap_addrs)} unknown tokens")

    if gap_addrs:
        print(f"  [2a] DeFiLlama gap-fill ({len(gap_addrs)} tokens)...")
        llama = _defillama_prices(gap_addrs)
        resolved, still_unknown = [], []
        for addr in gap_addrs:
            info = llama.get(addr)
            if info and info.get("symbol"):
                resolved.append({
                    "address": addr,
                    "symbol": info["symbol"][:32],
                    "name": info.get("name", info["symbol"])[:128],
                    "decimals": int(info.get("decimals", 18)),
                    "coingecko_id": "",
                    "logo_uri": "",
                })
            else:
                still_unknown.append(addr)
        if resolved:
            _ch_insert("chainlake.dim_bsc_token", resolved)
        print(f"  DeFiLlama: {len(resolved)} resolved, {len(still_unknown)} remaining")

        if still_unknown:
            cap = min(len(still_unknown), RPC_FALLBACK_CAP)
            print(f"  [2b] RPC fallback for {cap} tokens...")
            rpc_rows = []
            for addr in still_unknown[:cap]:
                row = _token_from_rpc(addr)
                if row:
                    rpc_rows.append(row)
                time.sleep(0.05)
            if rpc_rows:
                _ch_insert("chainlake.dim_bsc_token", rpc_rows)
            print(f"  RPC: {len(rpc_rows)} resolved")

    print("[3/4] Fetching hourly prices for all known tokens...")
    known = _ch_query(
        "SELECT address FROM chainlake.dim_bsc_token FINAL WHERE address != '' LIMIT 2000"
    )
    all_addrs = [r["address"] for r in known]
    if all_addrs:
        prices = _defillama_prices(all_addrs)
        hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        hour_str = hour.strftime("%Y-%m-%d %H:%M:%S")
        price_rows = [
            {
                "hour": hour_str,
                "token_address": addr,
                "price_usd": float(info["price"]),
                "source": "defillama",
            }
            for addr, info in prices.items()
            if info.get("price") is not None
        ]
        _ch_insert("chainlake.dim_bsc_token_price_hourly", price_rows)
        print(f"  {len(price_rows)} price rows for {hour_str}")

    print("[4/4] Done.")


if __name__ == "__main__":
    main()
