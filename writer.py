"""
chainstate-anchor · writer.py
==============================
The transaction worker. One background task processes queued anchor
requests sequentially so nonce ordering is trivial. Each request builds
its own transaction against the appropriate contract (Anchor or Cardiac),
signs with the AGI wallet, submits to Base RPC, and records the outcome.

Design notes:
  • Single-flight submitter — no concurrent nonce guessing. If a burst of
    100 receipts arrives we still submit them one at a time, but the queue
    fills instantly and the caller gets 202 Accepted without waiting.
  • Nonce is fetched from chain at startup and after each successful tx.
    On mismatch we refetch from chain and reset.
  • EIP-1559 gas pricing with a small cushion via GAS_PRICE_MULTIPLIER.
  • Retries on transient network errors, drops on contract reverts (with
    the revert reason logged).
  • Records the last N tx hashes for the /status endpoint.
"""
import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, Optional

from web3 import Web3
from web3.exceptions import ContractLogicError, TimeExhausted, TransactionNotFound
from eth_account.signers.local import LocalAccount

from config import Settings

logger = logging.getLogger(__name__)


@dataclass
class AnchorJob:
    """One unit of work: a contract function call to sign and submit."""
    kind: str                                  # "receipt" | "refusal" | "seed-run" | ...
    build_tx: Callable[[dict], Any]            # web3 contract call → tx dict
    memo: str = ""                             # human-readable label for logging
    submitted_at: float = field(default_factory=time.monotonic)
    attempts: int = 0


@dataclass
class TxRecord:
    """History entry for the /status endpoint."""
    kind: str
    memo: str
    tx_hash: str
    block_number: Optional[int]
    status: str                                # "success" | "reverted" | "dropped"
    at_ts: float
    gas_used: Optional[int] = None


class AnchorWriter:
    """Owns the tx queue, the signer, and the background submit loop."""

    def __init__(
        self,
        w3: Web3,
        account: LocalAccount,
        settings: Settings,
        history_size: int = 100,
    ):
        self.w3 = w3
        self.account = account
        self.settings = settings
        self.queue: asyncio.Queue[AnchorJob] = asyncio.Queue()
        self.history: Deque[TxRecord] = deque(maxlen=history_size)
        self._nonce: Optional[int] = None
        self._nonce_lock = asyncio.Lock()
        self._last_success_at: Optional[float] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

    # ─── Public API ─────────────────────────────────────────────────────

    async def start(self):
        """Fetch initial nonce, start background submitter."""
        await self._refresh_nonce()
        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info(
            "AnchorWriter started · wallet=%s · nonce=%d · queue_depth=%d",
            self.account.address, self._nonce or 0, self.queue.qsize(),
        )

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("AnchorWriter stopped")

    def enqueue(self, job: AnchorJob):
        """Non-blocking submission. Returns queue depth."""
        self.queue.put_nowait(job)
        return self.queue.qsize()

    def snapshot(self) -> Dict[str, Any]:
        """Read-only status for the /status endpoint."""
        return {
            "wallet": self.account.address,
            "current_nonce": self._nonce,
            "queue_depth": self.queue.qsize(),
            "last_success_at": self._last_success_at,
            "recent_txs": [
                {
                    "kind": r.kind, "memo": r.memo,
                    "tx_hash": r.tx_hash, "block_number": r.block_number,
                    "status": r.status, "at_ts": r.at_ts, "gas_used": r.gas_used,
                }
                for r in list(self.history)[-20:]
            ],
        }

    async def get_wallet_balance_wei(self) -> int:
        return await asyncio.get_event_loop().run_in_executor(
            None, lambda: self.w3.eth.get_balance(self.account.address)
        )

    # ─── Background submitter ───────────────────────────────────────────

    async def _run(self):
        last_refresh = time.monotonic()
        while self._running:
            # Periodically resync nonce against chain to catch external
            # transactions from the same wallet (should be zero in production
            # but defends against operator-side surprises).
            if time.monotonic() - last_refresh > self.settings.nonce_refresh_interval_seconds:
                try:
                    await self._refresh_nonce()
                except Exception as e:
                    logger.warning("nonce refresh failed: %s", e)
                last_refresh = time.monotonic()

            try:
                job = await asyncio.wait_for(self.queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue

            try:
                await self._process(job)
            except asyncio.CancelledError:
                # Requeue the job for next process lifecycle, then re-raise
                self.queue.put_nowait(job)
                raise
            except Exception as e:
                logger.exception("unexpected error processing %s: %s", job.kind, e)

    async def _refresh_nonce(self):
        async with self._nonce_lock:
            nonce = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.w3.eth.get_transaction_count(self.account.address, "pending"),
            )
            if self._nonce is None or nonce > self._nonce:
                logger.info("nonce refreshed: %s → %s", self._nonce, nonce)
                self._nonce = nonce

    async def _process(self, job: AnchorJob):
        """Sign and submit one job, with retry on transient errors."""
        for attempt in range(1, self.settings.max_retries + 1):
            job.attempts = attempt
            try:
                tx_hash, receipt = await self._submit(job)
                status = "success" if receipt.status == 1 else "reverted"
                self.history.append(TxRecord(
                    kind=job.kind, memo=job.memo,
                    tx_hash=tx_hash.hex() if isinstance(tx_hash, bytes) else str(tx_hash),
                    block_number=receipt.blockNumber,
                    status=status, at_ts=time.time(),
                    gas_used=receipt.gasUsed,
                ))
                if status == "success":
                    self._last_success_at = time.time()
                    logger.info(
                        "anchored %s (%s) · tx=%s · block=%d · gas=%d",
                        job.kind, job.memo, tx_hash.hex() if isinstance(tx_hash, bytes) else tx_hash,
                        receipt.blockNumber, receipt.gasUsed,
                    )
                else:
                    logger.warning(
                        "reverted %s (%s) · tx=%s", job.kind, job.memo,
                        tx_hash.hex() if isinstance(tx_hash, bytes) else tx_hash,
                    )
                return
            except ContractLogicError as e:
                # Revert during build — this is a contract-level rejection,
                # not a transient error. Drop.
                logger.warning("drop %s (%s) · contract revert: %s", job.kind, job.memo, e)
                self.history.append(TxRecord(
                    kind=job.kind, memo=job.memo, tx_hash="",
                    block_number=None, status="dropped", at_ts=time.time(),
                ))
                return
            except (TimeExhausted, TransactionNotFound) as e:
                logger.warning(
                    "tx wait timeout on %s (%s) attempt %d: %s",
                    job.kind, job.memo, attempt, e,
                )
                # Refresh nonce and retry
                await self._refresh_nonce()
            except Exception as e:
                logger.warning(
                    "transient error on %s (%s) attempt %d: %s",
                    job.kind, job.memo, attempt, e,
                )
                await asyncio.sleep(self.settings.retry_backoff_seconds * attempt)

        # All retries exhausted
        logger.error("giving up on %s (%s) after %d attempts", job.kind, job.memo, job.attempts)
        self.history.append(TxRecord(
            kind=job.kind, memo=job.memo, tx_hash="",
            block_number=None, status="dropped", at_ts=time.time(),
        ))

    async def _submit(self, job: AnchorJob):
        """Build tx dict, sign, send, wait for receipt. Runs blocking ops
        in the default executor so we don't block the event loop."""
        loop = asyncio.get_event_loop()

        async with self._nonce_lock:
            nonce = self._nonce
            self._nonce = nonce + 1 if nonce is not None else 0

        def _do():
            # EIP-1559 gas pricing with cushion
            latest = self.w3.eth.get_block("latest")
            base_fee = latest.get("baseFeePerGas") or self.w3.eth.gas_price
            try:
                priority_fee = self.w3.eth.max_priority_fee
            except Exception:
                priority_fee = self.w3.to_wei(1, "gwei")
            max_fee = int(base_fee * self.settings.gas_price_multiplier) + priority_fee

            tx_params = {
                "from": self.account.address,
                "nonce": nonce,
                "chainId": self.settings.chain_id,
                "maxFeePerGas": max_fee,
                "maxPriorityFeePerGas": priority_fee,
            }
            # build_tx receives the base tx params and returns a fully-populated tx
            tx = job.build_tx(tx_params)
            signed = self.account.sign_transaction(tx)
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(
                tx_hash, timeout=self.settings.tx_wait_timeout_seconds,
            )
            return tx_hash, receipt

        return await loop.run_in_executor(None, _do)
