"""
chainstate-anchor · contracts.py
=================================
Web3 provider, ABI loading, contract instances, and the calldata packing
helpers matching the on-chain packModal / packGasCostTime layouts.
"""
import json
import os
from pathlib import Path
from typing import Tuple

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware   # Base is an OP-stack rollup
from eth_account import Account
from eth_account.signers.local import LocalAccount

from config import Settings


ABI_DIR = Path(__file__).parent / "abi"


def load_abi(name: str) -> list:
    """Load an ABI JSON file from ./abi/."""
    path = ABI_DIR / f"{name}.json"
    with open(path, "r") as f:
        return json.load(f)


def make_web3(settings: Settings) -> Web3:
    """Construct a Web3 client for Base mainnet."""
    w3 = Web3(Web3.HTTPProvider(settings.base_rpc_url, request_kwargs={"timeout": 30}))
    # Base uses OP-stack extraData that includes a 65-byte signature field;
    # inject the POA middleware so block reads don't reject on that.
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    return w3


def make_account(settings: Settings) -> LocalAccount:
    """Load the AGI signer account. Private key stays in memory only."""
    return Account.from_key(settings.agi_private_key)


def make_anchor_contract(w3: Web3, settings: Settings):
    return w3.eth.contract(
        address=Web3.to_checksum_address(settings.anchor_contract_addr),
        abi=load_abi("CHAINSTATEAnchor"),
    )


def make_cardiac_contract(w3: Web3, settings: Settings):
    return w3.eth.contract(
        address=Web3.to_checksum_address(settings.cardiac_contract_addr),
        abi=load_abi("NWOCardiacExtensions"),
    )


# ─── Calldata packing helpers ──────────────────────────────────────────
# These mirror the on-chain packModal / packGasCostTime layouts exactly.
# Anyone can call the on-chain versions instead (they're free views), but
# doing it locally saves an RPC round-trip per receipt.

def pack_modal(
    verdict: int,
    dominant_subspace: int,
    substrate_target: int,
    confidence_bps: int,
    rounds_run: int,
    participating_nodes: int,
) -> int:
    """
    Pack the six small-int modal fields into a single uint256, matching
    CHAINSTATEAnchor.packModal().

    Layout:
        bits [ 0,  7]:  verdict            (uint8)
        bits [ 8, 15]:  dominantSubspace   (uint8)
        bits [16, 23]:  substrateTarget    (uint8)
        bits [24, 39]:  confidenceBps      (uint16)
        bits [40, 55]:  roundsRun          (uint16)
        bits [56, 71]:  participatingNodes (uint16)
    """
    if not (0 <= verdict <= 4):
        raise ValueError(f"verdict out of range: {verdict}")
    if not (0 <= dominant_subspace <= 5):
        raise ValueError(f"dominant_subspace out of range: {dominant_subspace}")
    if not (0 <= substrate_target <= 3):
        raise ValueError(f"substrate_target out of range: {substrate_target}")
    if not (0 <= confidence_bps <= 10000):
        raise ValueError(f"confidence_bps out of range: {confidence_bps}")
    if not (0 <= rounds_run <= 0xFFFF):
        raise ValueError(f"rounds_run out of range: {rounds_run}")
    if not (0 <= participating_nodes <= 0xFFFF):
        raise ValueError(f"participating_nodes out of range: {participating_nodes}")

    return (
        (verdict & 0xFF)
        | ((dominant_subspace & 0xFF) << 8)
        | ((substrate_target & 0xFF) << 16)
        | ((confidence_bps & 0xFFFF) << 24)
        | ((rounds_run & 0xFFFF) << 40)
        | ((participating_nodes & 0xFFFF) << 56)
    )


def pack_gas_cost_time(
    gas_used_micro: int,
    substrate_cost_usdc_micro: int,
    received_at: int,
) -> int:
    """
    Pack gas/cost/time into a single uint256, matching
    CHAINSTATEAnchor.packGasCostTime().

    Layout:
        bits [  0,  63]:  gasUsedMicro           (uint64)
        bits [ 64, 127]:  substrateCostUsdcMicro (uint64)
        bits [128, 191]:  receivedAt             (uint64)
    """
    for name, val in [
        ("gas_used_micro", gas_used_micro),
        ("substrate_cost_usdc_micro", substrate_cost_usdc_micro),
        ("received_at", received_at),
    ]:
        if not (0 <= val <= 0xFFFFFFFFFFFFFFFF):
            raise ValueError(f"{name} does not fit in uint64: {val}")

    return (
        (gas_used_micro & 0xFFFFFFFFFFFFFFFF)
        | ((substrate_cost_usdc_micro & 0xFFFFFFFFFFFFFFFF) << 64)
        | ((received_at & 0xFFFFFFFFFFFFFFFF) << 128)
    )


# ─── Value converters ──────────────────────────────────────────────────

VERDICT_MAP = {
    "ACCEPTED": 0, "REFUSED": 1, "UNCERTAIN": 2, "LOW_TRUST": 3, "INFEASIBLE": 4,
}

SUBSPACE_MAP = {
    "math": 0, "sci": 1, "lang": 2, "occ": 3, "emo": 4, "ctrl": 5,
}

TARGET_MAP = {
    "edge": 0, "gpu": 1, "qpu": 2, "npu": 3,
}


def verdict_to_int(v) -> int:
    if isinstance(v, int):
        return v
    return VERDICT_MAP.get((v or "").upper(), 0)


def subspace_to_int(s) -> int:
    if isinstance(s, int):
        return s
    return SUBSPACE_MAP.get((s or "").lower(), 0)


def target_to_int(t) -> int:
    if isinstance(t, int):
        return t
    return TARGET_MAP.get((t or "").lower(), 0)


def truth_lattice_to_bytes8(lattice: str) -> bytes:
    """Encode a truth lattice string like 'MMMM' as bytes8, left-aligned."""
    b = (lattice or "").encode("ascii")[:8]
    return b.ljust(8, b"\x00")


def qhash_to_bytes32(qhash) -> bytes:
    """Accept either a hex string (with or without 0x) or raw bytes."""
    if isinstance(qhash, bytes):
        if len(qhash) == 32:
            return qhash
        return qhash.ljust(32, b"\x00")[:32]
    s = str(qhash)
    if s.startswith("0x") or s.startswith("0X"):
        s = s[2:]
    # Pad or truncate to 64 hex chars = 32 bytes
    s = s.rjust(64, "0")[:64]
    return bytes.fromhex(s)


def hex_to_bytes32(h) -> bytes:
    """Convert a hex string to bytes32 (right-padded with zeros if short)."""
    if h is None:
        return b"\x00" * 32
    if isinstance(h, bytes):
        return h.ljust(32, b"\x00")[:32]
    s = str(h)
    if s.startswith("0x") or s.startswith("0X"):
        s = s[2:]
    s = s.rjust(64, "0")[:64]
    return bytes.fromhex(s)
