# AGENT.md · chainstate-anchor

Machine-readable manifest for the NWO ecosystem. The CHAINSTATE priors
ingester (nightly) pulls this file so the substrate's own reasoning about
its capabilities includes accurate metadata for this service.

## Identity

- **name**: chainstate-anchor
- **role**: on-chain attestation writer
- **layer**: L4 (settlement)
- **owner**: Ciprian Florin Pater
- **status**: LIVE

## Purpose

Autonomous Base mainnet transaction submitter for the CHAINSTATE AGI.
Holds the AGI writer wallet's private key. Receives attestation events
from the CHAINSTATE Cloudflare Worker via HTTPS bearer-authenticated
POSTs and pushes them to the CHAINSTATEAnchor and NWOCardiacExtensions
contracts on Base mainnet 8453.

This service is the ONLY component in the ecosystem with signing
authority over the AGI's on-chain identity. Deployer authority
(`0x2E964e1c...`) can rotate this service's writer wallet at any time
via `setAgiWallet()` on both contracts.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/` | HTML status snapshot |
| GET | `/health` | Render health probe (no auth) |
| GET | `/status` | JSON: queue depth, wallet balance, recent tx hashes |
| POST | `/anchor/receipt` | Anchor a /query receipt |
| POST | `/anchor/refusal` | Anchor a REFUSED verdict record |
| POST | `/anchor/seed-run` | Anchor a seed cron run summary |
| POST | `/anchor/guardrail-state` | Anchor a Deontic ruleset state |
| POST | `/anchor/identity-refresh` | Anchor a /identity/refresh event |
| POST | `/anchor/eml-expression` | Anchor an EML world-model expression |
| POST | `/anchor/credential` | Issue a Cardiac credential |
| POST | `/anchor/credential/revoke` | Revoke a Cardiac credential |

All POST endpoints require `Authorization: Bearer <ANCHOR_QUEUE_TOKEN>`.

## Chain integration

- **Chain**: Base mainnet
- **Chain ID**: 8453
- **Anchor contract**: `0x12441662740836e9c72a4b758fe1c60c17ddd2d8`
- **Cardiac contract**: `0x5438854ead35dc6c873414f222725732f862dabe`
- **Deployer / owner**: `0x2E964e1c0e3Fa2C0dfD484B2E6D2189dfCF20958`
- **RPC**: https://mainnet.base.org (public; can be swapped)
- **Gas**: EIP-1559 with 10% cushion on max fee; typical cost ~$0.0002 per anchor
- **Nonce strategy**: single-flight, chain-refetched every 5 minutes

## Consumers

- **CHAINSTATE Worker** (Cloudflare) — the sole authorized caller,
  authenticated via `ANCHOR_QUEUE_TOKEN` shared secret

## Producers

- **CHAINSTATEAnchor contract** — receives all six anchor streams
- **NWOCardiacExtensions contract** — receives credential attestations

## Governance

- The deployer wallet can rotate the writer wallet at any time
- The deployer wallet can pause the Anchor contract, freezing all writes
- Neither the deployer nor this service can edit already-anchored data
- This service holds NO admin authority on either contract

## Failure modes

- **Service down** → worker's `waitUntil()` calls fail silently; receipts
  stay in KV/Supabase only; no chain writes until service returns
- **Wallet depleted** → transactions fail with insufficient funds; logged
  in `/status.recent_txs` with `status: dropped`; refill wallet
- **Contract paused by owner** → transactions revert with `IsPaused`;
  logged and dropped; no retry
- **Bad ANCHOR_QUEUE_TOKEN** → 401/403 to worker; worker's `waitUntil`
  logs the failure but the /query response still succeeds

## Related components

- `chainstate-worker.ciprianpater.workers.dev` — the caller
- `chainstate-priors.onrender.com` — nightly priors ingester
- `chainstate-encoder.onrender.com` — MiniLM embedding service
- `cpater-nwo-cardiac.static.hf.space` — Cardiac SDK / documentation

## Repository

- **GitHub**: `RedCiprianPater/chainstate-anchor`
- **Deployment**: Render, Frankfurt region, Starter plan ($7/mo)
