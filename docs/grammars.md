```markdown
# Grammars of FO, Regulatory Reporting, and Accounting

The same trade tells three different stories depending on who is listening.

This repo uses a tiny securities book to illustrate those stories.

---

## 1. Front Office (FO) grammar

FO systems care about **trades, positions, and risk**:

- A **trade** has: trade date, direction, price, quantity, counterparty.
- A **position** is the net of trades for a `(customer, instrument, ccy)`.
- **MTM** is the current value of that position.
- Time is often implicit: the system holds the current state plus a blotter.

FO asks questions like:

- “What is my open position in ISIN X for client Y?”  
- “What is the MtM and P&L today?”  
- “What is my sensitivity if rates move 10bp?”

The natural unit is **a deal or a position**.

---

## 2. Regulatory grammar

Regulatory tools care about **exposures and limits**:

- Deal IDs and trade tickets are less important than:
  - product type
  - maturity bucket
  - counterparty class
  - collateral and netting sets

Reg reports ask:

- “How big is my exposure to this counterparty class?”  
- “What is the large exposure measure across all entities?”  
- “Which trades sit in this capital or liquidity bucket?”

In that grammar:

- A single FO trade can split across multiple regulatory buckets.
- Several FO trades can collapse into one exposure record.

The natural unit is an **exposure line in a report**, not a trade.

---

## 3. Accounting grammar

Accounting systems speak **double-entry**:

- Everything is a **journal**: `(date, account, DR/CR, amount)`.
- Accounts have types (asset, liability, income, expense).
- The core object is the **trial balance** – the sum over time.

Accounting asks:

- “What is the balance of account 200100 on 31 Jan?”  
- “Does the revaluation reserve reconcile to the MtM engine?”  
- “Is the trial balance in equilibrium?”

The natural unit is an **account balance by date**.

---

## 4. Why translation is hard

Most “reconciliation problems” come from assuming there is:

> one true table of trades that everyone should share.

But each grammar is compressing and re-labeling reality in its own way.

In this demo:

1. Trade blotter → FIFO open trades (one grammar of “what is open”)  
2. FIFO open trades → position-level controls (consistency within FO)  
3. Open trades + FO MtM → deal-level MtM (regulatory-friendly grammar)  
4. Deals → GL journals (accounting grammar)  
5. FO MtM timeseries → GL revaluation postings (bridge FO ↔ GL)  
6. Thin GL → candidate feed into regulatory engines and finance reporting

The point isn’t that this is “the correct way” to do it. It’s that **you have
to be explicit about the grammar you’re translating from and into**.

Once you do that, most mysterious reconciliation breaks become legible.
