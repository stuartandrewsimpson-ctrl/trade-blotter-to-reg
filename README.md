Select all the existing text, delete it, and paste this full README:

# Trade Blotter → Regulatory & Ledger Views

*A small, opinionated demo of how the same economic reality is described in
different grammars: Front Office trades, regulatory views, and accounting
double-entry. Written as a worked example rather than a framework.*

---

## Why this exists

In a bank, the **same trade** is spoken in at least three different dialects:

- **Front Office (FO)** talks in *trades, positions, MtM, risk*.
- **Regulatory reporting** talks in *exposures, product types, buckets,
  counterparties*.
- **Accounts / GL** talk in *accounts, debits/credits, balances over time*.

Most reconciliation pain comes from treating this as a “data mapping problem”
rather than a **grammar translation problem**.

This repo is a tiny, self-contained example of that translation:

> A few simple securities trades →  
> FO positions & MtM →  
> Trade-level GL postings →  
> MTM revaluation postings →  
> Thin general ledger that matches FO MTM and can feed regulatory views.

It’s not a production system; it’s a **teaching and interview artefact**.

---

## What the example does

The main script is:

```bash
examples/trading_subledger_report.py


It:

Loads toy data from data/

sec_trades.csv – FO trade blotter

sec_positions.csv – official positions (as-of date)

fo_sec_positions.csv – FO MtM at position level (snapshot)

fo_mtm_timeseries.csv – FO MtM by position over time

Computes open trades (reg view)

Applies a FIFO engine per (customer_id, isin, ccy)

Derives remaining_quantity per trade

Builds v_reg_sec_open_trades: the set of open deals and their remaining
quantities

Control: compares FIFO-derived position vs the official positions table

Allocates FO MtM down to deal level

For each position:

Compute open_notional = remaining_quantity * price per trade

Allocate position MtM to deals ∝ open_notional

Control:

Σ(deal_
𝑚
𝑡
𝑚
𝑎
𝑙
𝑙
𝑜
𝑐
𝑎
𝑡
𝑒
𝑑
mtm
a
	​

llocated) = FO MtM at position level (within rounding)

Builds trade-level GL postings

Implements a simple average-cost model:

BUY → Dr Securities, Cr Cash

SELL → Dr Cash, Cr Securities, Dr/Cr Realised P&L

Double-entry per trade; balances always net to zero.

Builds MtM revaluation postings over time

For each position across dates:

Reverse previous day’s MtM

Book today’s MtM level

Uses two accounts:

400100 Unrealised P&L

400200 Revaluation reserve

Checks FO vs GL revaluation

Per position & date: FO MtM vs cumulative GL revaluation balance

Portfolio-level control: Σ(FO MtM) vs Σ(GL revaluation) per date

Builds a thin general ledger

Spine of (posting_date, account_code, ccy, balance)

Suitable as a feed into a proper GL / reporting mart.

Generates a PDF report

Trading_Subledger_Report_Full.pdf in the repo root

Includes:

Open trades view

FIFO vs positions control

GL postings for trades

MtM allocation & control

MtM postings & FO vs GL controls

Thin GL balances (per account & date)

Quickstart
1. Set up environment
git clone https://github.com/stuartandrewsimpson-ctrl/trade-blotter-to-reg.git
cd trade-blotter-to-reg

python -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate

pip install -r requirements.txt

2. Run the example

From the repo root:

python examples/trading_subledger_report.py


This will create:

Trading_Subledger_Report_Full.pdf


Open it and scroll through the sections – each shows one “grammar” and the
controls that link them together.

Data model & grammars

The pipeline is deliberately shaped like a small warehouse:

Staging

Raw FO tables (sec_trades, sec_positions, FO MtM snapshots)

Enrichment

Derived fields: FIFO remaining quantities, open notionals, allocated MtM,
GL postings

Transformation

Reg view of open deals

GL view of journal entries

Normalised / reporting

Thin GL spine (date, account, ccy, balance)

Reg-style open positions with MtM and customer identifiers

This mirrors a real bank:

FO systems care about deals and risk.

Regulatory tools care about exposures by product, counterparty, and bucket.

Finance / GL care about accounts and balances over time.

The code is intentionally explicit rather than “clever”: the goal is to make the
semantic moves visible.

How this relates to real systems

This repo is deliberately system-agnostic:

No reference to specific FO platforms

No assumptions about GL product architecture

No regulatory engine baked in

But each step maps to something recognisable in a real bank:

Trade blotter → FO or treasury system trade tables

Positions + MtM → FO risk / valuation engine

GL postings → subledger or accounting rules engine

Thin GL → general ledger / data mart feeding financial & regulatory reports

The interesting part is not the code; it’s the shape of the controls:

FIFO position vs official positions table

Σ(deal MtM) vs FO position MtM

FO MtM timeseries vs GL revaluation balances

Double-entry and thin ledger balances

These are the “bridges” between different grammars.

Extending this

Some natural extensions:

Add repos, FX, swaps with their own grammars (legs vs deals vs cashflows)

Add regulatory buckets (tenor bands, product types, counterparty classes)

Replace the toy CSVs with real-ish anonymised data for internal demos

Generate lineage diagrams from the steps (e.g. using Graphviz)

Licence

MIT – use freely, adapt, and extend. No warranty.
