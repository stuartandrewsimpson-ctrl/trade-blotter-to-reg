"""
Trading Subledger Report
========================

Generate a single PDF showing the full chain:

Trade Blotter -> Positions -> FO MTM ->
GL (Trade + MTM + MTM reversals) -> Thin General Ledger

Assumptions: CSVs are in ../data relative to this script:
    - sec_trades.csv
    - sec_positions.csv
    - fo_sec_positions.csv
    - fo_mtm_timeseries.csv

Run from repo root:
    cd trade-blotter-to-reg
    python examples/trading_subledger_report.py
"""

import os
from collections import deque

import pandas as pd
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
)
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib import colors


# ----------------------------------------------------------------------
# 1. Parameters & account constants
# ----------------------------------------------------------------------

SECURITY_ASSET_ACCOUNT = 200100
CASH_ACCOUNT = 100000
REALIZED_PNL_ACCOUNT = 300100
UNREAL_PNL_ACCOUNT = 400100
REVAL_ACCOUNT = 400200


# ----------------------------------------------------------------------
# 2. Data loading
# ----------------------------------------------------------------------

def load_staging(base_path: str = "."):
    data_path = os.path.join(base_path, "data")

    stg_sec_trd = pd.read_csv(
        os.path.join(data_path, "sec_trades.csv"),
        parse_dates=["trade_date"],
    )

    stg_sec_pos = pd.read_csv(
        os.path.join(data_path, "sec_positions.csv"),
        parse_dates=["as_of_date"],
    )

    stg_fo_pos = pd.read_csv(
        os.path.join(data_path, "fo_sec_positions.csv"),
        parse_dates=["as_of_date"],
    )

    return stg_sec_trd, stg_sec_pos, stg_fo_pos


def load_fo_mtm_timeseries(base_path: str = "."):
    data_path = os.path.join(base_path, "data")

    fo_ts = pd.read_csv(
        os.path.join(data_path, "fo_mtm_timeseries.csv"),
        parse_dates=["as_of_date"],
    )
    return fo_ts


# ----------------------------------------------------------------------
# 3. Core transformations (FIFO, allocation, GL, controls)
# ----------------------------------------------------------------------

def fifo_remaining_qty_for_group(df_group: pd.DataFrame) -> pd.Series:
    """
    Per (customer_id, isin, ccy):
      BUY  → adds quantity to FIFO queue
      SELL → consumes from FIFO queue
    Returns remaining quantity per trade_id.
    """
    df = df_group.sort_values(["trade_date", "trade_id"]).copy()
    remaining = {tid: 0 for tid in df["trade_id"]}

    buys: deque[list] = deque()

    # Enqueue BUY trades
    for _, r in df.iterrows():
        if r["side"].upper() == "BUY":
            buys.append([r["trade_id"], r["quantity"]])
            remaining[r["trade_id"]] = r["quantity"]

    # Apply SELL trades against FIFO queue
    for _, r in df.iterrows():
        if r["side"].upper() == "SELL":
            qty_to_match = r["quantity"]

            while qty_to_match > 0 and buys:
                buy_trade_id, buy_qty = buys[0]
                used = min(buy_qty, qty_to_match)

                # reduce remaining on that buy trade
                remaining[buy_trade_id] -= used

                # update queue
                buy_qty -= used
                qty_to_match -= used

                if buy_qty == 0:
                    buys.popleft()
                else:
                    buys[0][1] = buy_qty

            # sells themselves have no remaining qty
            remaining[r["trade_id"]] = 0

    return df["trade_id"].map(remaining)


def build_v_reg_sec_open_trades(
    stg_sec_trd: pd.DataFrame,
    stg_sec_pos: pd.DataFrame,
):
    """
    Returns:
      - v_reg_open   : open trades with remaining_quantity > 0
      - ctrl_trd_pos : FIFO position vs official position table
    """
    trades = stg_sec_trd.copy()

    trades["remaining_quantity"] = (
        trades
        .groupby(["customer_id", "isin", "ccy"], group_keys=False)
        .apply(fifo_remaining_qty_for_group)
    )

    fifo_pos = (
        trades
        .groupby(["customer_id", "isin", "ccy"], as_index=False)
        .agg(fifo_position_qty=("remaining_quantity", "sum"))
    )

    ctrl_trd_pos = fifo_pos.merge(
        stg_sec_pos[["customer_id", "isin", "ccy", "position_quantity"]],
        on=["customer_id", "isin", "ccy"],
        how="left",
    ).fillna(0.0)

    ctrl_trd_pos["difference"] = (
        ctrl_trd_pos["fifo_position_qty"] - ctrl_trd_pos["position_quantity"]
    )

    v_reg_open = trades[trades["remaining_quantity"] > 0].copy()
    v_reg_open["open_flag"] = True

    return v_reg_open, ctrl_trd_pos


def allocate_mtm_to_deals(
    v_reg_open: pd.DataFrame,
    stg_fo_pos: pd.DataFrame,
):
    """
    Allocate FO position-level MTM to open deals, pro-rata on open_notional.
    Also produces a control table Σ(deal MTM) vs FO MTM per position.
    """
    trades = v_reg_open.copy()
    trades["open_notional"] = trades["remaining_quantity"] * trades["price"]

    fo_pos = stg_fo_pos.copy()

    # merge in FO MTM and FO snapshot date
    trades = trades.merge(
        fo_pos[["customer_id", "isin", "ccy", "fo_mtm", "as_of_date"]],
        on=["customer_id", "isin", "ccy"],
        how="left",
        suffixes=("", "_snap"),
    )
    trades.rename(columns={"as_of_date": "snapshot_date"}, inplace=True)

    def alloc_group(g: pd.DataFrame) -> pd.DataFrame:
        total_notional = g["open_notional"].sum()
        if total_notional == 0 or pd.isna(total_notional):
            g["mtm_allocated"] = 0.0
        else:
            g["mtm_allocated"] = g["fo_mtm"] * (g["open_notional"] / total_notional)
        return g

    trades = (
        trades
        .groupby(["customer_id", "isin", "ccy"], group_keys=False)
        .apply(alloc_group)
    )

    ctrl_pos_mtm = (
        trades
        .groupby(["customer_id", "isin", "ccy", "snapshot_date"], as_index=False)
        .agg(
            allocated_mtm=("mtm_allocated", "sum"),
            fo_mtm=("fo_mtm", "max"),
        )
    )
    ctrl_pos_mtm["difference"] = (
        ctrl_pos_mtm["allocated_mtm"] - ctrl_pos_mtm["fo_mtm"]
    )

    return trades, ctrl_pos_mtm


def generate_gl_trade_postings(trades: pd.DataFrame) -> pd.DataFrame:
    """
    Double-entry trade GL postings using average cost:
      - BUY  : Dr Securities, Cr Cash
      - SELL : Dr Cash, Cr Securities, Dr/Cr Realised P&L
    """
    rows = []

    trades_sorted = trades.sort_values(
        ["customer_id", "isin", "trade_date", "trade_id"]
    )

    pos_qty = {}
    pos_cost = {}

    for _, r in trades_sorted.iterrows():
        key = (r["customer_id"], r["isin"], r["ccy"])
        qty = r["quantity"]
        price = r["price"]
        notional = qty * price
        side = r["side"].upper()

        if key not in pos_qty:
            pos_qty[key] = 0.0
            pos_cost[key] = 0.0

        if side == "BUY":
            pos_qty[key] += qty
            pos_cost[key] += notional

            rows.extend([
                {
                    "posting_date": r["trade_date"],
                    "deal_id": r["trade_id"],
                    "customer_id": r["customer_id"],
                    "isin": r["isin"],
                    "ccy": r["ccy"],
                    "account_code": SECURITY_ASSET_ACCOUNT,
                    "dr_cr": "DR",
                    "amount": notional,
                    "posting_type": "PURCHASE",
                },
                {
                    "posting_date": r["trade_date"],
                    "deal_id": r["trade_id"],
                    "customer_id": r["customer_id"],
                    "isin": r["isin"],
                    "ccy": r["ccy"],
                    "account_code": CASH_ACCOUNT,
                    "dr_cr": "CR",
                    "amount": notional,
                    "posting_type": "PURCHASE",
                },
            ])

        elif side == "SELL":
            avg_cost = (pos_cost[key] / pos_qty[key]) if pos_qty[key] != 0 else price
            cost_of_sold = avg_cost * qty
            proceeds = notional
            pnl = proceeds - cost_of_sold

            pos_qty[key] -= qty
            pos_cost[key] -= cost_of_sold

            rows.extend([
                {
                    "posting_date": r["trade_date"],
                    "deal_id": r["trade_id"],
                    "customer_id": r["customer_id"],
                    "isin": r["isin"],
                    "ccy": r["ccy"],
                    "account_code": CASH_ACCOUNT,
                    "dr_cr": "DR",
                    "amount": proceeds,
                    "posting_type": "SALE",
                },
                {
                    "posting_date": r["trade_date"],
                    "deal_id": r["trade_id"],
                    "customer_id": r["customer_id"],
                    "isin": r["isin"],
                    "ccy": r["ccy"],
                    "account_code": SECURITY_ASSET_ACCOUNT,
                    "dr_cr": "CR",
                    "amount": cost_of_sold,
                    "posting_type": "SALE",
                },
            ])

            if pnl != 0:
                rows.append({
                    "posting_date": r["trade_date"],
                    "deal_id": r["trade_id"],
                    "customer_id": r["customer_id"],
                    "isin": r["isin"],
                    "ccy": r["ccy"],
                    "account_code": REALIZED_PNL_ACCOUNT,
                    "dr_cr": "CR" if pnl > 0 else "DR",
                    "amount": abs(pnl),
                    "posting_type": "SALE_PNL",
                })

    return pd.DataFrame(rows)


def _double_entry_mtm(
    posting_date,
    customer_id,
    isin,
    ccy,
    amount,
    posting_type,
):
    """
    Create a DR/CR pair for an MTM movement.
    Positive amount: Dr Reval, Cr Unrealised P&L
    Negative amount: Dr Unrealised P&L, Cr Reval
    """
    rows = []
    amt = abs(amount)

    if amount > 0:
        rows.append({
            "posting_date": posting_date,
            "deal_id": None,
            "customer_id": customer_id,
            "isin": isin,
            "ccy": ccy,
            "account_code": REVAL_ACCOUNT,
            "dr_cr": "DR",
            "amount": amt,
            "posting_type": posting_type,
        })
        rows.append({
            "posting_date": posting_date,
            "deal_id": None,
            "customer_id": customer_id,
            "isin": isin,
            "ccy": ccy,
            "account_code": UNREAL_PNL_ACCOUNT,
            "dr_cr": "CR",
            "amount": amt,
            "posting_type": posting_type,
        })
    elif amount < 0:
        rows.append({
            "posting_date": posting_date,
            "deal_id": None,
            "customer_id": customer_id,
            "isin": isin,
            "ccy": ccy,
            "account_code": UNREAL_PNL_ACCOUNT,
            "dr_cr": "DR",
            "amount": amt,
            "posting_type": posting_type,
        })
        rows.append({
            "posting_date": posting_date,
            "deal_id": None,
            "customer_id": customer_id,
            "isin": isin,
            "ccy": ccy,
            "account_code": REVAL_ACCOUNT,
            "dr_cr": "CR",
            "amount": amt,
            "posting_type": posting_type,
        })

    return rows


def generate_gl_mtm_postings(fo_ts: pd.DataFrame) -> pd.DataFrame:
    """
    For each (customer, isin, ccy) over time:
      - Reverse prior day's MTM
      - Book today's MTM level
    """
    rows = []

    for (cust, isin, ccy), g in fo_ts.groupby(["customer_id", "isin", "ccy"]):
        g_sorted = g.sort_values("as_of_date")
        prev_mtm = 0.0

        for _, r in g_sorted.iterrows():
            date = r["as_of_date"]
            mtm = float(r["fo_mtm"])

            if prev_mtm != 0:
                rows.extend(
                    _double_entry_mtm(
                        posting_date=date,
                        customer_id=cust,
                        isin=isin,
                        ccy=ccy,
                        amount=-prev_mtm,
                        posting_type="MTM_REVERSAL",
                    )
                )

            if mtm != 0:
                rows.extend(
                    _double_entry_mtm(
                        posting_date=date,
                        customer_id=cust,
                        isin=isin,
                        ccy=ccy,
                        amount=mtm,
                        posting_type="MTM",
                    )
                )

            prev_mtm = mtm

    return pd.DataFrame(rows)


def build_mtm_gl_control(
    fo_ts: pd.DataFrame,
    gl_mtm: pd.DataFrame,
) -> pd.DataFrame:
    """
    Per position & per date:
      FO MTM vs cumulative GL revaluation (account 400200).
    """
    reval = gl_mtm[gl_mtm["account_code"] == REVAL_ACCOUNT].copy()
    reval["signed_amount"] = reval.apply(
        lambda r: r["amount"] if r["dr_cr"] == "DR" else -r["amount"],
        axis=1,
    )

    reval_grouped = (
        reval
        .groupby(["customer_id", "isin", "ccy", "posting_date"], as_index=False)
        .agg(day_change=("signed_amount", "sum"))
        .sort_values(["customer_id", "isin", "ccy", "posting_date"])
    )

    reval_grouped["gl_mtm_balance"] = (
        reval_grouped
        .groupby(["customer_id", "isin", "ccy"])["day_change"]
        .cumsum()
    )

    fo = fo_ts.rename(columns={"as_of_date": "posting_date"}).copy()

    ctrl = fo.merge(
        reval_grouped,
        on=["customer_id", "isin", "ccy", "posting_date"],
        how="left",
    ).fillna({"day_change": 0.0, "gl_mtm_balance": 0.0})

    ctrl["difference"] = ctrl["gl_mtm_balance"] - ctrl["fo_mtm"]

    return ctrl


# ----------------------------------------------------------------------
# 4. PDF utilities
# ----------------------------------------------------------------------

_styles = getSampleStyleSheet()
_title_style = _styles["Title"]
_heading = _styles["Heading2"]
_body = _styles["BodyText"]


def df_to_table(df: pd.DataFrame, max_rows: int = 20) -> Table:
    df_show = df.head(max_rows).copy()
    data = [list(df_show.columns)] + df_show.astype(str).values.tolist()

    tbl = Table(data, hAlign="LEFT")
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightblue),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 7),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
    ]))
    return tbl


# ----------------------------------------------------------------------
# 5. Main report builder
# ----------------------------------------------------------------------

def build_report(
    base_path: str = ".",
    output_pdf: str = "Trading_Subledger_Report_Full.pdf",
):
    # Load data
    stg_sec_trd, stg_sec_pos, stg_fo_pos = load_staging(base_path)
    fo_ts = load_fo_mtm_timeseries(base_path)

    # Pipeline
    v_reg_open, ctrl_trd_pos = build_v_reg_sec_open_trades(stg_sec_trd, stg_sec_pos)
    v_reg_with_mtm, ctrl_pos_mtm = allocate_mtm_to_deals(v_reg_open, stg_fo_pos)
    gl_trade = generate_gl_trade_postings(stg_sec_trd)
    gl_mtm = generate_gl_mtm_postings(fo_ts)
    ctrl_mtm_gl = build_mtm_gl_control(fo_ts, gl_mtm)

    # Thin GL
    gl_all = pd.concat([gl_trade, gl_mtm], ignore_index=True)
    gl_all["signed_amount"] = gl_all.apply(
        lambda r: r["amount"] if r["dr_cr"] == "DR" else -r["amount"],
        axis=1,
    )

    daily_moves = (
        gl_all
        .groupby(["posting_date", "account_code", "ccy"], as_index=False)
        .agg(day_change=("signed_amount", "sum"))
        .sort_values(["account_code", "ccy", "posting_date"])
    )
    daily_moves["balance"] = (
        daily_moves
        .groupby(["account_code", "ccy"])["day_change"]
        .cumsum()
    )

    # Portfolio-level MTM control (date-only)
    portfolio_mtm = (
        ctrl_mtm_gl
        .groupby("posting_date", as_index=False)
        .agg(
            sum_fo_mtm=("fo_mtm", "sum"),
            gl_reval_balance=("gl_mtm_balance", "sum"),
        )
    )
    portfolio_mtm["difference"] = (
        portfolio_mtm["gl_reval_balance"] - portfolio_mtm["sum_fo_mtm"]
    )

    # ------------------------------------------------------------------
    # Build PDF
    # ------------------------------------------------------------------
    doc = SimpleDocTemplate(
        output_pdf,
        pagesize=landscape(letter),
    )
    story = []

    # Cover
    story.append(Paragraph(
        "Trading Subledger – End-to-End Accounting & Reconciliation Report",
        _title_style,
    ))
    story.append(Spacer(1, 12))
    story.append(Paragraph(
        "Trade Blotter → Positions → FO MTM → GL (Trade + MTM) → Thin Ledger",
        _body,
    ))
    story.append(PageBreak())

    # 1. Open trades
    story.append(Paragraph("1. Open Trades (Reg View)", _heading))
    story.append(Spacer(1, 6))
    story.append(df_to_table(v_reg_open[[
        "trade_date", "trade_id", "customer_id", "isin", "ccy",
        "side", "quantity", "remaining_quantity", "open_flag",
    ]]))
    story.append(PageBreak())

    # 2. FIFO vs positions
    story.append(Paragraph("2. FIFO Position Control", _heading))
    story.append(Spacer(1, 6))
    story.append(df_to_table(ctrl_trd_pos))
    story.append(PageBreak())

    # 3. Trade GL postings
    story.append(Paragraph("3. Trade-Level GL Postings", _heading))
    story.append(Spacer(1, 6))
    story.append(df_to_table(gl_trade.sort_values([
        "posting_date", "deal_id", "posting_type", "account_code",
    ])))
    story.append(PageBreak())

    # 4. MTM allocation
    story.append(Paragraph("4. FO MTM Allocation to Open Deals", _heading))
    story.append(Paragraph(
        "Position-level FO MTM allocated to open trades, pro-rata on open_notional. "
        "Snapshot date is shown explicitly.",
        _body,
    ))
    story.append(Spacer(1, 6))
    story.append(df_to_table(v_reg_with_mtm[[
        "trade_date", "snapshot_date",
        "trade_id", "customer_id", "isin", "ccy",
        "remaining_quantity", "price",
        "open_notional", "fo_mtm", "mtm_allocated",
    ]]))
    story.append(Spacer(1, 12))
    story.append(Paragraph("Σ(deal MTM) vs FO MTM per position.", _body))
    story.append(df_to_table(ctrl_pos_mtm))
    story.append(PageBreak())

    # 5. MTM GL postings
    story.append(Paragraph("5. Multi-Day MTM & MTM Reversal Postings", _heading))
    story.append(Spacer(1, 6))
    story.append(df_to_table(gl_mtm.sort_values([
        "customer_id", "isin", "ccy",
        "posting_date", "posting_type", "account_code",
    ]), max_rows=40))
    story.append(PageBreak())

    # 6. MTM control per position
    story.append(Paragraph("6. MTM Control: FO vs GL (Per Position, Per Date)", _heading))
    story.append(Spacer(1, 6))
    story.append(df_to_table(ctrl_mtm_gl.sort_values([
        "customer_id", "isin", "ccy", "posting_date",
    ])))
    story.append(PageBreak())

    # 6b. Portfolio-level MTM control
    story.append(Paragraph("6b. Portfolio MTM Control: FO vs GL (Per Date)", _heading))
    story.append(Spacer(1, 6))
    story.append(df_to_table(portfolio_mtm[[
        "posting_date", "sum_fo_mtm", "gl_reval_balance", "difference",
    ]], max_rows=40))
    story.append(PageBreak())

    # 7. Thin GL
    story.append(Paragraph("7. Thin General Ledger (Date, Account, CCY, Balance)", _heading))
    story.append(Spacer(1, 6))
    story.append(df_to_table(daily_moves[[
        "posting_date", "account_code", "ccy", "balance",
    ]], max_rows=50))

    doc.build(story)


# ----------------------------------------------------------------------
# 6. Entry point
# ----------------------------------------------------------------------

if __name__ == "__main__":
    # script lives in ./examples; project root is parent
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_file = os.path.join(base_dir, "Trading_Subledger_Report_Full.pdf")
    build_report(base_path=base_dir, output_pdf=out_file)
    print(f"Report written to: {out_file}")
