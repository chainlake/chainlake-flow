from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml


@dataclass(frozen=True)
class ChainProfile:
    chain_uid: str
    chain_name: str
    chain_type: str
    network: str
    interval_seconds: float


def default_chain_profiles_path() -> Path:
    return Path(__file__).resolve().parent / "chain_profiles.yaml"


def _normalize_key(chain_name: str, network: str) -> tuple[str, str]:
    return chain_name.strip().lower(), network.strip().lower()


def _load_profiles_from_yaml(path: Path) -> list[ChainProfile]:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    chains = raw.get("chains", [])
    if not isinstance(chains, list):
        raise ValueError(f"chain profiles file must contain a list at 'chains': {path}")

    profiles: list[ChainProfile] = []
    for entry in chains:
        meta = (entry or {}).get("meta", {})
        chain_uid = meta.get("chain_uid")
        chain_name = meta.get("chain_name")
        chain_type = meta.get("chain_type")
        network = meta.get("network")
        interval_seconds = (entry or {}).get("data", {}).get("interval_seconds")

        if not chain_uid or not chain_name or not chain_type or not network:
            raise ValueError(
                "chain profile entries must define meta.chain_uid, meta.chain_name, "
                "meta.chain_type and meta.network"
            )
        if interval_seconds is None:
            raise ValueError(
                "chain profile entries must define data.interval_seconds"
            )

        profiles.append(
            ChainProfile(
                chain_uid=str(chain_uid),
                chain_name=str(chain_name),
                chain_type=str(chain_type),
                network=str(network),
                interval_seconds=float(interval_seconds),
            )
        )

    return profiles


@lru_cache(maxsize=4)
def load_chain_profiles(path: str | None = None) -> dict[tuple[str, str], ChainProfile]:
    profile_path = Path(path) if path else default_chain_profiles_path()
    profiles = _load_profiles_from_yaml(profile_path)

    indexed: dict[tuple[str, str], ChainProfile] = {}
    for profile in profiles:
        key = _normalize_key(profile.chain_name, profile.network)
        if key in indexed:
            existing = indexed[key]
            raise ValueError(
                "duplicate chain profile for "
                f"{profile.chain_name}/{profile.network}: "
                f"{existing.chain_uid} and {profile.chain_uid}"
            )
        indexed[key] = profile

    return indexed


def get_chain_profile(
    chain_name: str,
    network: str,
    *,
    path: str | None = None,
) -> ChainProfile:
    profiles = load_chain_profiles(path)
    key = _normalize_key(chain_name, network)
    try:
        return profiles[key]
    except KeyError as exc:
        raise ValueError(
            f"unknown chain profile for name={chain_name!r}, network={network!r}"
        ) from exc


def resolve_chain_config(
    chain_cfg: dict,
    *,
    profiles_path: str | None = None,
) -> dict:
    chain_name = chain_cfg.get("name")
    network = chain_cfg.get("network")
    if not chain_name or not network:
        raise ValueError("chain.name and chain.network are required")

    profile = get_chain_profile(str(chain_name), str(network), path=profiles_path)

    resolved = dict(chain_cfg)
    explicit_uid = resolved.get("uid")
    explicit_type = resolved.get("type")

    if explicit_uid and str(explicit_uid) != profile.chain_uid:
        raise ValueError(
            f"chain.uid does not match profile for {chain_name}/{network}: "
            f"{explicit_uid!r} != {profile.chain_uid!r}"
        )
    if explicit_type and str(explicit_type) != profile.chain_type:
        raise ValueError(
            f"chain.type does not match profile for {chain_name}/{network}: "
            f"{explicit_type!r} != {profile.chain_type!r}"
        )

    resolved["uid"] = profile.chain_uid
    resolved["type"] = profile.chain_type
    return resolved
