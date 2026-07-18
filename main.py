"""
chainstate-anchor · main.py
============================
FastAPI service that receives receipts (and other attestations) from the
CHAINSTATE worker and streams them onto the Base mainnet CHAINSTATEAnchor
+ NWOCardiacExtensions contracts.

Endpoints:
  GET  /                         · HTML status page (browsers)
  GET  /health                   · Render health probe
  GET  /status                   · JSON status: queue depth, balance, recent txs
  POST /anchor/receipt           · Anchor a /query receipt (bearer auth)
  POST /anchor/refusal           · Anchor a REFUSED-verdict entry
  POST /anchor/seed-run          · Anchor a seed cron run summary
  POST /anchor/guardrail-state   · Anchor a Deontic guardrail state
  POST /anchor/identity-refresh  · Anchor a /identity/refresh event
  POST /anchor/eml-expression    · Anchor an EML world-model expression
  POST /anchor/credential        · Issue a Cardiac credential
  POST /anchor/credential/revoke · Revoke a Cardiac credential

All POST endpoints require:
  Authorization: Bearer <ANCHOR_QUEUE_TOKEN>

Every POST enqueues a job and returns 202 Accepted immediately. The
background writer processes the queue serially to keep nonce ordering
trivial. Recent tx hashes are inspectable via /status.
"""
import asyncio
import contextlib
import hashlib
import logging
import time
from typing import Any, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from web3 import Web3

from config import Settings
from contracts import (
    make_web3, make_account,
    make_anchor_contract, make_cardiac_contract,
    pack_modal, pack_gas_cost_time,
    verdict_to_int, subspace_to_int, target_to_int,
    truth_lattice_to_bytes8, qhash_to_bytes32, hex_to_bytes32,
)
from writer import AnchorWriter, AnchorJob


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s · %(levelname)s · %(name)s · %(message)s",
)
logger = logging.getLogger("chainstate-anchor")


settings = Settings.load()
w3 = make_web3(settings)
account = make_account(settings)
anchor = make_anchor_contract(w3, settings)
cardiac = make_cardiac_contract(w3, settings)

writer = AnchorWriter(w3, account, settings)


# ─── Lifecycle ─────────────────────────────────────────────────────────

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    await writer.start()
    logger.info(
        "chainstate-anchor ready · anchor=%s · cardiac=%s · chain=%d",
        settings.anchor_contract_addr, settings.cardiac_contract_addr, settings.chain_id,
    )
    yield
    await writer.stop()


app = FastAPI(
    title="CHAINSTATE Anchor microservice",
    version="1.0.0",
    lifespan=lifespan,
)


# ─── Bearer auth ───────────────────────────────────────────────────────

def _check_auth(authorization: Optional[str]):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization[7:].strip()
    if token != settings.anchor_queue_token:
        raise HTTPException(status_code=403, detail="invalid bearer token")


# ─── Pydantic request models ───────────────────────────────────────────

class ReceiptBody(BaseModel):
    qHash: str
    semantic_hash: Optional[str] = None
    identity_hash: Optional[str] = None
    truth_lattice: str = "MMMM"
    verdict: Any = "ACCEPTED"           # str or int
    dominant_subspace: Any = "math"     # str or int
    target: Any = "edge"                # str or int
    confidence: float = 0.0             # 0..1 (multiplied to bps)
    rounds_run: int = 0
    participating_nodes: int = 0
    gas_used: float = 0.0               # $STATE units (multiplied to micro)
    substrate_cost_usdc: float = 0.0    # USDC (multiplied to micro)
    received_at: Optional[int] = None   # unix seconds; defaults to now
    requester_root_token_id: Optional[Any] = None   # str or int; 0 = anonymous


class RefusalBody(BaseModel):
    qHash: str
    category: str
    marker: Optional[str] = None
    refused_at: Optional[int] = None


class SeedRunBody(BaseModel):
    run_id: str                          # e.g. "seed:2026-07-18T14"
    results_hash: Optional[str] = None   # SHA-256 of results JSON
    seed_count: int = 0
    ok_count: int = 0
    error_count: int = 0
    ran_at: Optional[int] = None


class GuardrailStateBody(BaseModel):
    ruleset_hash: str
    categories_active_hash: Optional[str] = None
    categories_disabled_hash: Optional[str] = None
    genomic_integrity_active: bool = True
    active_count: int = 0
    disabled_count: int = 0


class IdentityRefreshBody(BaseModel):
    worker_version_hash: str
    contracts_hash: Optional[str] = None
    endpoints_hash: Optional[str] = None
    allowlist_hash: Optional[str] = None
    deontic_hash: Optional[str] = None


class EmlExpressionBody(BaseModel):
    expression_hash: str
    feature_hash: str
    sample_n: int = 0
    residual_ppm: int = 0
    fitted_at: Optional[int] = None


class CredentialBody(BaseModel):
    subject_root_token_id: Any            # str or int (uint256)
    credential_type: str                  # keccak-hashed if not already 0x-prefixed 32 bytes
    scope_hash: Optional[str] = None
    expires_at: int                       # unix seconds
    issuer_root_token_id: Optional[Any] = None   # 0 = defaults to substrate


class CredentialRevokeBody(BaseModel):
    index: int


# ─── Helper: hash a symbolic string to bytes32 ─────────────────────────

def _tag_to_bytes32(tag: str) -> bytes:
    """
    If `tag` is a 0x-prefixed 32-byte hex string, use it as-is.
    Otherwise keccak256 the utf-8 bytes to produce the bytes32.
    Matches the convention Solidity uses for constant type labels.
    """
    if tag.startswith("0x") and len(tag) == 66:
        return bytes.fromhex(tag[2:])
    return Web3.keccak(text=tag)


def _root_token_id_to_int(v) -> int:
    if v is None:
        return 0
    if isinstance(v, int):
        return v
    s = str(v).strip()
    if not s:
        return 0
    return int(s)


# ─── Endpoints ─────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """Render probe. Cheap; does not touch the chain."""
    return {"ok": True, "service": "chainstate-anchor", "version": app.version}


@app.get("/status")
async def status():
    snap = writer.snapshot()
    try:
        balance_wei = await writer.get_wallet_balance_wei()
        snap["wallet_balance_eth"] = balance_wei / 1e18
    except Exception as e:
        snap["wallet_balance_eth"] = None
        snap["wallet_balance_error"] = str(e)[:120]
    snap["contracts"] = {
        "anchor": settings.anchor_contract_addr,
        "cardiac": settings.cardiac_contract_addr,
        "chain_id": settings.chain_id,
    }
    return snap


@app.get("/", response_class=HTMLResponse)
async def root():
    snap = writer.snapshot()
    try:
        balance_wei = await writer.get_wallet_balance_wei()
        balance_eth = f"{balance_wei / 1e18:.6f}"
    except Exception:
        balance_eth = "unknown"
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>chainstate-anchor</title>
<style>
  body{{font-family:'IBM Plex Mono',Menlo,monospace;background:#0a0a0a;color:#e5e5e5;padding:2rem;max-width:900px;margin:0 auto;}}
  h1{{font-family:Orbitron,sans-serif;letter-spacing:.1em;font-weight:500;font-size:1.4rem;color:#fff;}}
  .row{{padding:.3rem 0;border-bottom:1px solid #222;}}
  .k{{color:#888;}}
  .v{{color:#e5e5e5;}}
  a{{color:#7aa;}}
  code{{background:#1a1a1a;padding:.1rem .3rem;border-radius:2px;}}
</style></head>
<body>
<h1>Φ  chainstate-anchor · v{app.version}</h1>
<div class="row"><span class="k">writer wallet:</span> <span class="v"><code>{snap["wallet"]}</code></span></div>
<div class="row"><span class="k">wallet balance:</span> <span class="v">{balance_eth} ETH</span></div>
<div class="row"><span class="k">current nonce:</span> <span class="v">{snap["current_nonce"]}</span></div>
<div class="row"><span class="k">queue depth:</span> <span class="v">{snap["queue_depth"]}</span></div>
<div class="row"><span class="k">anchor contract:</span> <span class="v"><a href="https://basescan.org/address/{settings.anchor_contract_addr}"><code>{settings.anchor_contract_addr}</code></a></span></div>
<div class="row"><span class="k">cardiac contract:</span> <span class="v"><a href="https://basescan.org/address/{settings.cardiac_contract_addr}"><code>{settings.cardiac_contract_addr}</code></a></span></div>
<div class="row"><span class="k">chain:</span> <span class="v">Base mainnet ({settings.chain_id})</span></div>
<p>Machine-readable status: <a href="/status">/status</a> · Health: <a href="/health">/health</a></p>
</body></html>"""


# ─── /anchor/receipt ───────────────────────────────────────────────────

@app.post("/anchor/receipt", status_code=202)
async def anchor_receipt(body: ReceiptBody, authorization: str = Header(None)):
    _check_auth(authorization)

    q = qhash_to_bytes32(body.qHash)
    sem = hex_to_bytes32(body.semantic_hash)
    ident = hex_to_bytes32(body.identity_hash)
    lattice = truth_lattice_to_bytes8(body.truth_lattice)

    verdict_int = verdict_to_int(body.verdict)
    subspace_int = subspace_to_int(body.dominant_subspace)
    target_int = target_to_int(body.target)
    confidence_bps = int(round(max(0.0, min(1.0, float(body.confidence))) * 10000))
    rounds = max(0, min(0xFFFF, int(body.rounds_run)))
    peers = max(0, min(0xFFFF, int(body.participating_nodes)))

    modal_packed = pack_modal(verdict_int, subspace_int, target_int, confidence_bps, rounds, peers)

    # gas and cost passed as floats; convert to micro-units
    gas_micro = int(max(0.0, float(body.gas_used)) * 1_000_000)
    cost_micro = int(max(0.0, float(body.substrate_cost_usdc)) * 1_000_000)
    received_at = body.received_at if body.received_at is not None else int(time.time())
    # Clamp to uint64
    gas_micro = min(gas_micro, 0xFFFFFFFFFFFFFFFF)
    cost_micro = min(cost_micro, 0xFFFFFFFFFFFFFFFF)
    received_at = min(received_at, 0xFFFFFFFFFFFFFFFF)

    gas_cost_time_packed = pack_gas_cost_time(gas_micro, cost_micro, received_at)

    requester_id = _root_token_id_to_int(body.requester_root_token_id)

    tuple_arg = (q, sem, ident, lattice, modal_packed, gas_cost_time_packed, requester_id)

    def build_tx(base):
        return anchor.functions.anchorReceipt(tuple_arg).build_transaction(base)

    depth = writer.enqueue(AnchorJob(
        kind="receipt",
        build_tx=build_tx,
        memo=f"qHash={body.qHash[:10]}...",
    ))
    return {"ok": True, "queued": True, "queue_depth": depth}


# ─── /anchor/refusal ───────────────────────────────────────────────────

@app.post("/anchor/refusal", status_code=202)
async def anchor_refusal(body: RefusalBody, authorization: str = Header(None)):
    _check_auth(authorization)

    q = qhash_to_bytes32(body.qHash)
    cat = _tag_to_bytes32(body.category)
    marker = _tag_to_bytes32(body.marker or "")
    refused_at = body.refused_at if body.refused_at is not None else int(time.time())
    refused_at = min(refused_at, 0xFFFFFFFFFFFFFFFF)

    def build_tx(base):
        return anchor.functions.anchorRefusal(q, cat, marker, refused_at).build_transaction(base)

    depth = writer.enqueue(AnchorJob(
        kind="refusal", build_tx=build_tx,
        memo=f"cat={body.category}",
    ))
    return {"ok": True, "queued": True, "queue_depth": depth}


# ─── /anchor/seed-run ──────────────────────────────────────────────────

@app.post("/anchor/seed-run", status_code=202)
async def anchor_seed_run(body: SeedRunBody, authorization: str = Header(None)):
    _check_auth(authorization)

    run_id_hash = Web3.keccak(text=body.run_id)
    results_hash = hex_to_bytes32(body.results_hash)
    ran_at = body.ran_at if body.ran_at is not None else int(time.time())
    ran_at = min(ran_at, 0xFFFFFFFFFFFFFFFF)

    seed_count = max(0, min(0xFFFF, int(body.seed_count)))
    ok_count = max(0, min(0xFFFF, int(body.ok_count)))
    error_count = max(0, min(0xFFFF, int(body.error_count)))

    def build_tx(base):
        return anchor.functions.anchorSeedRun(
            run_id_hash, results_hash, seed_count, ok_count, error_count, ran_at
        ).build_transaction(base)

    depth = writer.enqueue(AnchorJob(
        kind="seed-run", build_tx=build_tx,
        memo=body.run_id,
    ))
    return {"ok": True, "queued": True, "queue_depth": depth}


# ─── /anchor/guardrail-state ───────────────────────────────────────────

@app.post("/anchor/guardrail-state", status_code=202)
async def anchor_guardrail_state(body: GuardrailStateBody, authorization: str = Header(None)):
    _check_auth(authorization)

    ruleset_hash = hex_to_bytes32(body.ruleset_hash)
    active_hash = hex_to_bytes32(body.categories_active_hash)
    disabled_hash = hex_to_bytes32(body.categories_disabled_hash)
    active_count = max(0, min(0xFFFF, int(body.active_count)))
    disabled_count = max(0, min(0xFFFF, int(body.disabled_count)))

    def build_tx(base):
        return anchor.functions.anchorGuardrailState(
            ruleset_hash, active_hash, disabled_hash,
            bool(body.genomic_integrity_active),
            active_count, disabled_count,
        ).build_transaction(base)

    depth = writer.enqueue(AnchorJob(
        kind="guardrail-state", build_tx=build_tx,
        memo=f"genomic={body.genomic_integrity_active}",
    ))
    return {"ok": True, "queued": True, "queue_depth": depth}


# ─── /anchor/identity-refresh ──────────────────────────────────────────

@app.post("/anchor/identity-refresh", status_code=202)
async def anchor_identity_refresh(body: IdentityRefreshBody, authorization: str = Header(None)):
    _check_auth(authorization)

    def build_tx(base):
        return anchor.functions.anchorIdentityRefresh(
            hex_to_bytes32(body.worker_version_hash),
            hex_to_bytes32(body.contracts_hash),
            hex_to_bytes32(body.endpoints_hash),
            hex_to_bytes32(body.allowlist_hash),
            hex_to_bytes32(body.deontic_hash),
        ).build_transaction(base)

    depth = writer.enqueue(AnchorJob(
        kind="identity-refresh", build_tx=build_tx,
        memo=body.worker_version_hash[:10],
    ))
    return {"ok": True, "queued": True, "queue_depth": depth}


# ─── /anchor/eml-expression ────────────────────────────────────────────

@app.post("/anchor/eml-expression", status_code=202)
async def anchor_eml_expression(body: EmlExpressionBody, authorization: str = Header(None)):
    _check_auth(authorization)

    fitted_at = body.fitted_at if body.fitted_at is not None else int(time.time())
    fitted_at = min(fitted_at, 0xFFFFFFFFFFFFFFFF)
    sample_n = max(0, min(0xFFFFFFFF, int(body.sample_n)))
    residual_ppm = max(0, min(0xFFFFFFFF, int(body.residual_ppm)))

    def build_tx(base):
        return anchor.functions.anchorEmlExpression(
            hex_to_bytes32(body.expression_hash),
            hex_to_bytes32(body.feature_hash),
            sample_n, residual_ppm, fitted_at,
        ).build_transaction(base)

    depth = writer.enqueue(AnchorJob(
        kind="eml-expression", build_tx=build_tx,
        memo=body.feature_hash[:10],
    ))
    return {"ok": True, "queued": True, "queue_depth": depth}


# ─── /anchor/credential ────────────────────────────────────────────────

@app.post("/anchor/credential", status_code=202)
async def anchor_credential(body: CredentialBody, authorization: str = Header(None)):
    _check_auth(authorization)

    subject = _root_token_id_to_int(body.subject_root_token_id)
    if subject == 0:
        raise HTTPException(status_code=400, detail="subject_root_token_id must be non-zero")
    cred_type = _tag_to_bytes32(body.credential_type)
    scope = hex_to_bytes32(body.scope_hash)
    issuer = _root_token_id_to_int(body.issuer_root_token_id)
    expires_at = min(int(body.expires_at), 0xFFFFFFFFFFFFFFFF)
    if expires_at <= int(time.time()):
        raise HTTPException(status_code=400, detail="expires_at must be in the future")

    def build_tx(base):
        return cardiac.functions.anchorCredential(
            subject, cred_type, scope, expires_at, issuer,
        ).build_transaction(base)

    depth = writer.enqueue(AnchorJob(
        kind="credential", build_tx=build_tx,
        memo=f"type={body.credential_type[:20]} subject={subject}",
    ))
    return {"ok": True, "queued": True, "queue_depth": depth}


# ─── /anchor/credential/revoke ─────────────────────────────────────────

@app.post("/anchor/credential/revoke", status_code=202)
async def anchor_credential_revoke(body: CredentialRevokeBody, authorization: str = Header(None)):
    _check_auth(authorization)

    def build_tx(base):
        return cardiac.functions.revokeCredential(int(body.index)).build_transaction(base)

    depth = writer.enqueue(AnchorJob(
        kind="credential-revoke", build_tx=build_tx,
        memo=f"index={body.index}",
    ))
    return {"ok": True, "queued": True, "queue_depth": depth}


# ─── Error handler for cleaner logs ────────────────────────────────────

@app.exception_handler(HTTPException)
async def http_exc_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})
