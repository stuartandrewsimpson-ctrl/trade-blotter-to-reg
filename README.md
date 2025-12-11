# Trade Blotter → Regulatory & Ledger Views

This repo is a small, self-contained example of how the same trades are
expressed in different “grammars”:

- Front Office: trades, positions, MtM
- Regulatory: exposures and buckets
- Accounting: double-entry journals and balances

The main demo script is:

    examples/trading_subledger_report.py

It loads toy CSV data from `data/`, builds:

- FIFO open-trade view
- FO MtM allocated down to deals
- Trade-level GL postings
- MtM revaluation postings over time
- A thin general ledger (date, account, ccy, balance)

and generates a PDF report `Trading_Subledger_Report_Full.pdf` in the repo
root that shows each step and the controls that link them together.

To run:

    pip install -r requirements.txt
    python examples/trading_subledger_report.py
