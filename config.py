"""
chainstate-anchor · config.py
=============================
Loads environment variables and validates them at startup.
"""
import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Settings:
    """Immutable runtime configuration."""

    # ─── AGI writer wallet ─────────────────────────────────────────────
    # The private key of the wallet authorized to call anchor* functions
    # on both contracts. NEVER commit; set via Render Secret Files or
    # environment variables marked as secret.
    agi_private_key: str

    # ─── Bearer authentication (shared with the CHAINSTATE worker) ──────
    # The worker passes this in the Authorization header on every anchor
    # request. Generate with: openssl rand -hex 32
    anchor_queue_token: str

    # ─── Deployed contract addresses ───────────────────────────────────
    anchor_contract_addr: str            # CHAINSTATEAnchor.sol
    cardiac_contract_addr: str           # NWOCardiacExtensions.sol

    # ─── Base mainnet RPC ──────────────────────────────────────────────
    base_rpc_url: str = "https://mainnet.base.org"
    chain_id: int = 8453

    # ─── Worker settings ───────────────────────────────────────────────
    max_retries: int = 3
    retry_backoff_seconds: float = 2.0
    gas_price_multiplier: float = 1.1     # 10% cushion over base fee
    tx_wait_timeout_seconds: int = 30
    nonce_refresh_interval_seconds: int = 300

    # ─── Service settings ──────────────────────────────────────────────
    port: int = 8000
    log_level: str = "INFO"

    @classmethod
    def load(cls) -> "Settings":
        """Load from environment; raise if any required var is missing."""
        required = {
            "AGI_PRIVATE_KEY":         "AGI writer wallet private key (0x-prefixed hex)",
            "ANCHOR_QUEUE_TOKEN":      "Bearer token shared with the CHAINSTATE worker",
            "ANCHOR_CONTRACT_ADDR":    "Deployed CHAINSTATEAnchor address",
            "CARDIAC_CONTRACT_ADDR":   "Deployed NWOCardiacExtensions address",
        }
        missing = [k for k in required if not os.environ.get(k)]
        if missing:
            details = "\n".join(f"  {k} - {required[k]}" for k in missing)
            raise RuntimeError(f"Missing required env vars:\n{details}")

        pk = os.environ["AGI_PRIVATE_KEY"].strip()
        if not pk.startswith("0x"):
            pk = "0x" + pk
        if len(pk) != 66:
            raise RuntimeError(f"AGI_PRIVATE_KEY must be 32 bytes hex (got {len(pk)-2} hex chars)")

        return cls(
            agi_private_key=pk,
            anchor_queue_token=os.environ["ANCHOR_QUEUE_TOKEN"].strip(),
            anchor_contract_addr=os.environ["ANCHOR_CONTRACT_ADDR"].strip(),
            cardiac_contract_addr=os.environ["CARDIAC_CONTRACT_ADDR"].strip(),
            base_rpc_url=os.environ.get("BASE_RPC_URL", cls.base_rpc_url).strip(),
            chain_id=int(os.environ.get("CHAIN_ID", str(cls.chain_id))),
            max_retries=int(os.environ.get("MAX_RETRIES", str(cls.max_retries))),
            retry_backoff_seconds=float(os.environ.get("RETRY_BACKOFF_SECONDS", str(cls.retry_backoff_seconds))),
            gas_price_multiplier=float(os.environ.get("GAS_PRICE_MULTIPLIER", str(cls.gas_price_multiplier))),
            tx_wait_timeout_seconds=int(os.environ.get("TX_WAIT_TIMEOUT_SECONDS", str(cls.tx_wait_timeout_seconds))),
            nonce_refresh_interval_seconds=int(os.environ.get("NONCE_REFRESH_INTERVAL_SECONDS", str(cls.nonce_refresh_interval_seconds))),
            port=int(os.environ.get("PORT", str(cls.port))),
            log_level=os.environ.get("LOG_LEVEL", cls.log_level).upper(),
        )
