import pandas as pd
from collections import deque

# ---------------------------------------
# Account constants
# ---------------------------------------

# Trade-level accounts
SECURITY_ASSET_ACCOUNT = 200100   # Balance sheet: securities at cost
CASH_ACCOUNT = 100000             # Balance sheet: cash
REALIZED_PNL_ACCOUNT = 300100     # P&L: realised gains/losses

# MtM accounts
REVAL_ACCOUNT = 400200            # Balance sheet: revaluation reserve
UNREAL_PNL_ACCOUNT = 400100       # P&L: unrealised MtM


# ---------------------------------------
# 1. Load staging tables
# ---------------------------------------

def load_staging(base_path: str = "."):
    """
    Load:
      - stg_sec_trd: securities trade blotter
      - stg_sec_pos: positions table
      - stg_fo_pos : FO MTM at single as-of date (for deal-level allocation)
    """
    stg_sec_trd = pd.read_csv(f"{base_path}/data/sec_trades.csv", parse_dates=["trade_date"])
    stg_sec_pos = pd.read_csv(f"{base_path}/data/sec_positions.csv", parse_dates=["as_of_date"])
    stg_fo_pos  = pd.read_csv(f"{base_path}/data/fo_sec_positions.csv", parse_dates=["as_of_date"])
    return stg_sec_trd, stg_sec_pos, stg_fo_pos


def load_fo_mtm_timeseries(base_path: str = "."):
    """
    Load FO MTM at position level across multiple days.
    One row per (customer_id, isin, ccy, as_of_date).
    """
    fo_ts = pd.read_csv(f"{base_path}/data/fo_mtm_timeseries.csv", parse_dates=["as_of_date"])
    return fo_ts


# ---------------------------------------
# 2. FIFO: trades → remaining quantity per deal
# ---------------------------------------

def fifo_remaining_qty_for_group(df_group: pd.DataFrame) -> pd.Series:
    """
    Within a single (customer_id, isin, ccy):
      - BUY  adds quantity to the buy queue
      - SELL consumes from the earliest buys (FIFO)
    Returns remaining quantity per trade_id (0 for fully closed buys and for sells).
    """
    df = df_group.sort_values(["trade_date", "trade_id"]).copy()
    remaining = {tid: 0 for tid in df["trade_id"]}
    buys = deque()

    # enqueue buys
    for _, r in df.iterrows():
        if r["side"].upper() == "BUY":
            buys.append([r["trade_id"], r["quantity"]])
            remaining[r["trade_id"]] = r["quantity"]

    # allocate sells against the buy queue
    for _, r in df.iterrows():
        if r["side"].upper() == "SELL":
            qty_to_match = r["quantity"]
            while qty_to_match > 0 and buys:
                buy_trade_id, buy_qty = buys[0]
                used = min(buy_qty, qty_to_match)
                buy_qty -= used
                qty_to_match -= used
                remaining[buy_trade_id] -= used
                if buy_qty == 0:
                    buys.popleft()
                else:
                    buys[0][1] = buy_qty
            # sells themselves end with no remaining quantity
            remaining[r["trade_id"]] = 0

    return df["trade_id"].map(remaining)


def build_v_reg_sec_open_trades(stg_sec_trd: pd.DataFrame,
                                stg_sec_pos: pd.DataFrame,
                                as_of_date=None):
    """
    Build:
      - v_reg_sec_open_trades: deal-level open trades with remaining_quantity > 0
      - ctrl_trd_pos: control comparing FIFO-derived position vs positions table
    """
    trades = stg_sec_trd.copy()
    if as_of_date is not None:
        trades = trades[trades["trade_date"] <= pd.to_datetime(as_of_date)]

    # Remaining quantity per trade via FIFO
    trades["remaining_quantity"] = (
        trades
        .groupby(["customer_id", "isin", "ccy"], group_keys=False)
        .apply(fifo_remaining_qty_for_group)
    )

    # Aggregate to position to compare with stg_sec_positions
    fifo_pos = (
        trades
        .groupby(["customer_id", "isin", "ccy"], as_index=False)
        .agg(fifo_position_qty=("remaining_quantity", "sum"))
    )

    pos = stg_sec_pos.copy()
    if as_of_date is not None:
        pos = pos[pos["as_of_date"] == pd.to_datetime(as_of_date)]

    ctrl_trd_pos = (
        fifo_pos
        .merge(
            pos[["customer_id", "isin", "ccy", "position_quantity"]],
            on=["customer_id", "isin", "ccy"],
            how="outer"
        )
        .fillna(0.0)
    )
    ctrl_trd_pos["difference"] = ctrl_trd_pos["fifo_position_qty"] - ctrl_trd_pos["position_quantity"]

    # Reg view: only trades with remaining quantity > 0
    v_reg = trades[trades["remaining_quantity"] > 0].copy()
    v_reg["open_flag"] = True

    return v_reg, ctrl_trd_pos


# ---------------------------------------
# 3. Allocate FO MTM (single date) → deals (reg)
# ---------------------------------------

def allocate_mtm_to_deals(v_reg_open: pd.DataFrame,
                          stg_fo_pos: pd.DataFrame,
                          as_of_date=None):
    """
    Take FO MTM at position level (single as-of date) and allocate it to open deals per ISIN,
    pro-rata by open_notional (remaining_quantity * price).
    Returns:
      - v_reg_with_mtm: open trades with allocated MTM
      - ctrl_pos_mtm: control comparing Σ(deal MTM) vs FO MTM per position
    """
    trades = v_reg_open.copy()

    # Simple notional proxy: remaining_qty * trade price
    trades["open_notional"] = trades["remaining_quantity"] * trades["price"]

    if as_of_date is not None:
        fo_pos = stg_fo_pos[stg_fo_pos["as_of_date"] == pd.to_datetime(as_of_date)].copy()
    else:
        fo_pos = stg_fo_pos.copy()

    # Join FO MTM at position level
    trades = trades.merge(
        fo_pos[["customer_id", "isin", "ccy", "fo_mtm"]],
        on=["customer_id", "isin", "ccy"],
        how="left"
    )

    # Allocate within each (customer, isin, ccy)
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

    # Control: check allocation sums back to FO MTM
    ctrl_pos_mtm = (
        trades
        .groupby(["customer_id", "isin", "ccy"], as_index=False)
        .agg(
            allocated_mtm=("mtm_allocated", "sum"),
            fo_mtm=("fo_mtm", "max")
        )
    )
    ctrl_pos_mtm["difference"] = ctrl_pos_mtm["allocated_mtm"] - ctrl_pos_mtm["fo_mtm"]

    return trades, ctrl_pos_mtm


# ---------------------------------------
# 4. GL PURCHASE / SALE postings (Phase 2)
# ---------------------------------------

def generate_gl_trade_postings(trades: pd.DataFrame) -> pd.DataFrame:
    """
    Generate GL journals for trade events (purchase/sale) using an average-cost method:

      BUY:
        Dr Securities Asset (at cost)
        Cr Cash

      SELL:
        Dr Cash                 (proceeds)
        Cr Securities Asset     (cost of sold units, avg cost)
        Dr/Cr Realised P&L      (difference)

    Ensures double-entry per deal.
    """
    rows = []

    trades_sorted = trades.sort_values(["customer_id", "isin", "trade_date", "trade_id"]).copy()

    # Track running position quantity and cost per (customer, isin, ccy)
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
            # Update position at cost
            pos_qty[key] += qty
            pos_cost[key] += notional

            # GL: Dr Securities Asset / Cr Cash
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
            # Average cost per unit based on current position
            if pos_qty[key] <= 0:
                avg_cost = price  # degenerate fallback
            else:
                avg_cost = pos_cost[key] / pos_qty[key]

            cost_of_sold = avg_cost * qty
            proceeds = notional
            pnl = proceeds - cost_of_sold

            # Reduce position
            pos_qty[key] -= qty
            pos_cost[key] -= cost_of_sold

            # GL:
            # Dr Cash (proceeds)
            rows.append({
                "posting_date": r["trade_date"],
                "deal_id": r["trade_id"],
                "customer_id": r["customer_id"],
                "isin": r["isin"],
                "ccy": r["ccy"],
                "account_code": CASH_ACCOUNT,
                "dr_cr": "DR",
                "amount": proceeds,
                "posting_type": "SALE",
            })
            # Cr Securities Asset (cost)
            rows.append({
                "posting_date": r["trade_date"],
                "deal_id": r["trade_id"],
                "customer_id": r["customer_id"],
                "isin": r["isin"],
                "ccy": r["ccy"],
                "account_code": SECURITY_ASSET_ACCOUNT,
                "dr_cr": "CR",
                "amount": cost_of_sold,
                "posting_type": "SALE",
            })
            # Realised P&L
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

    gl = pd.DataFrame(rows)
    return gl


def build_trade_gl_control(trades: pd.DataFrame,
                           gl_trade: pd.DataFrame):
    """
    Controls:
      - For BUY trades:
          trade_notional == GL asset DR == GL cash CR
      - For SELL trades:
          trade_notional == GL cash DR
          GL cash DR == GL asset CR + GL realised P&L  (balance check)
    """
    trades2 = trades.copy()
    trades2["trade_notional"] = trades2["quantity"] * trades2["price"]

    # ---- BUY control ----
    buys = trades2[trades2["side"].str.upper() == "BUY"].copy()
    gl_buys = gl_trade[gl_trade["posting_type"] == "PURCHASE"]
    gb = gl_buys.groupby("deal_id")

    def get_from_group(tid, account, dr_cr):
        if tid not in gb.groups:
            return 0.0
        g = gb.get_group(tid)
        return g[(g["account_code"] == account) & (g["dr_cr"] == dr_cr)]["amount"].sum()

    buys["gl_asset"] = buys["trade_id"].map(
        lambda tid: get_from_group(tid, SECURITY_ASSET_ACCOUNT, "DR")
    )
    buys["gl_cash"] = buys["trade_id"].map(
        lambda tid: get_from_group(tid, CASH_ACCOUNT, "CR")
    )
    buys["diff_asset"] = buys["gl_asset"] - buys["trade_notional"]
    buys["diff_cash"] = buys["gl_cash"] - buys["trade_notional"]

    # ---- SELL control ----
    sells = trades2[trades2["side"].str.upper() == "SELL"].copy()
    gl_sales = gl_trade[gl_trade["posting_type"].isin(["SALE", "SALE_PNL"])]
    gs = gl_sales.groupby("deal_id")

    def summarize_sale(tid):
        if tid not in gs.groups:
            return pd.Series({"gl_cash": 0.0, "gl_asset": 0.0, "gl_pnl": 0.0})
        g = gs.get_group(tid)
        return pd.Series({
            "gl_cash": g[(g["account_code"] == CASH_ACCOUNT) & (g["dr_cr"] == "DR")]["amount"].sum(),
            "gl_asset": g[(g["account_code"] == SECURITY_ASSET_ACCOUNT) & (g["dr_cr"] == "CR")]["amount"].sum(),
            "gl_pnl": g[(g["account_code"] == REALIZED_PNL_ACCOUNT)]["amount"].sum(),
        })

    sale_summary = sells["trade_id"].apply(summarize_sale)
    sells = pd.concat([sells.reset_index(drop=True), sale_summary.reset_index(drop=True)], axis=1)
    sells["diff_cash"] = sells["gl_cash"] - sells["trade_notional"]
    sells["balance_check"] = sells["gl_cash"] - (sells["gl_asset"] + sells["gl_pnl"])

    return buys, sells


# ---------------------------------------
# 5. GL MTM + MTM reversals over multiple days
# ---------------------------------------

def _double_entry_rows(posting_date, customer_id, isin, ccy,
                       amount, posting_type):
    """
    Create two GL rows (DR/CR) for a given MtM amount.
    Convention:
      - positive amount  => Dr Revaluation, Cr Unrealised P&L
      - negative amount  => Dr Unrealised P&L, Cr Revaluation
    """
    rows = []
    amt = abs(amount)

    if amount > 0:
        # Dr Revaluation, Cr Unrealised P&L
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
        # Dr Unrealised P&L, Cr Revaluation
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

    # if amount == 0 => no postings
    return rows


def generate_gl_mtm_postings(fo_ts: pd.DataFrame) -> pd.DataFrame:
    """
    For each (customer_id, isin, ccy) over time:
      - On first date: book MtM fo_mtm(d0)
      - On later dates: reverse prior day's MtM, then book today's MtM
    Returns a GL postings DataFrame with MtM and MtM_REVERSAL entries.
    """
    rows = []

    # Work per position (customer, isin, ccy)
    for (cust, isin, ccy), g in fo_ts.groupby(["customer_id", "isin", "ccy"]):
        g_sorted = g.sort_values("as_of_date").reset_index(drop=True)
        prev_mtm = 0.0

        for _, r in g_sorted.iterrows():
            date = r["as_of_date"]
            mtm = float(r["fo_mtm"])

            # 1) reversal of prior day's MtM
            if prev_mtm != 0:
                rows.extend(
                    _double_entry_rows(
                        posting_date=date,
                        customer_id=cust,
                        isin=isin,
                        ccy=ccy,
                        amount=-prev_mtm,
                        posting_type="MTM_REVERSAL"
                    )
                )

            # 2) booking of today's MtM
            if mtm != 0:
                rows.extend(
                    _double_entry_rows(
                        posting_date=date,
                        customer_id=cust,
                        isin=isin,
                        ccy=ccy,
                        amount=mtm,
                        posting_type="MTM"
                    )
                )

            prev_mtm = mtm

    gl = pd.DataFrame(rows)
    return gl


def build_mtm_gl_control(fo_ts: pd.DataFrame, gl: pd.DataFrame) -> pd.DataFrame:
    """
    Control: for each day and position, check that GL revaluation balance
    matches FO MtM level.
    We derive GL balance by cumulatively summing signed amounts on the
    revaluation account (REVAL_ACCOUNT).
    """
    # Keep only revaluation account rows
    reval = gl[gl["account_code"] == REVAL_ACCOUNT].copy()

    # Signed amount: DR = +, CR = -
    reval["signed_amount"] = reval.apply(
        lambda r: r["amount"] if r["dr_cr"] == "DR" else -r["amount"],
        axis=1
    )

    # Aggregate by date
    reval_grouped = (
        reval
        .groupby(["customer_id", "isin", "ccy", "posting_date"], as_index=False)
        .agg(day_change=("signed_amount", "sum"))
    )

    # Cumulative sum over time per position
    reval_grouped = reval_grouped.sort_values(["customer_id", "isin", "ccy", "posting_date"])
    reval_grouped["gl_mtm_balance"] = (
        reval_grouped
        .groupby(["customer_id", "isin", "ccy"])["day_change"]
        .cumsum()
    )

    # Prepare FO side
    fo = fo_ts.rename(columns={"as_of_date": "posting_date"}).copy()

    ctrl = fo.merge(
        reval_grouped,
        on=["customer_id", "isin", "ccy", "posting_date"],
        how="left"
    ).fillna({"day_change": 0.0, "gl_mtm_balance": 0.0})

    ctrl["difference"] = ctrl["gl_mtm_balance"] - ctrl["fo_mtm"]

    return ctrl


# ---------------------------------------
# 6. Main: run the example
# ---------------------------------------

def main():
    base_path = "."

    # 1) Trades → positions → allocation of FO MTM to deals
    stg_sec_trd, stg_sec_pos, stg_fo_pos = load_staging(base_path)

    v_reg_open, ctrl_trd_pos = build_v_reg_sec_open_trades(stg_sec_trd, stg_sec_pos)
    v_reg_with_mtm, ctrl_pos_mtm = allocate_mtm_to_deals(v_reg_open, stg_fo_pos)

    print("=== v_reg_sec_open_trades (open deals) ===")
    print(v_reg_open[["trade_id", "customer_id", "isin", "ccy", "quantity",
                      "remaining_quantity", "open_flag"]])

    print("\n=== Control: FIFO position vs positions table ===")
    print(ctrl_trd_pos)

    print("\n=== v_reg_sec_open_trades with allocated MTM ===")
    print(v_reg_with_mtm[["trade_id", "customer_id", "isin", "ccy",
                          "remaining_quantity", "price",
                          "open_notional", "fo_mtm", "mtm_allocated"]])

    print("\n=== Control: Σ(deal MTM) vs FO MTM (position) ===")
    print(ctrl_pos_mtm)

    # 2) GL trade postings (purchase / sale) + controls
    gl_trade = generate_gl_trade_postings(stg_sec_trd)
    buys_ctrl, sells_ctrl = build_trade_gl_control(stg_sec_trd, gl_trade)

    print("\n=== GL trade postings (purchase & sale) ===")
    print(gl_trade.sort_values(["posting_date", "deal_id", "posting_type", "account_code"]))

    print("\n=== Control: BUY trades – trade notional vs GL asset/cash ===")
    print(buys_ctrl[["trade_id", "customer_id", "isin", "trade_date",
                     "quantity", "price", "trade_notional",
                     "gl_asset", "gl_cash", "diff_asset", "diff_cash"]])

    print("\n=== Control: SELL trades – cash vs asset + realised P&L ===")
    print(sells_ctrl[["trade_id", "customer_id", "isin", "trade_date",
                      "quantity", "price", "trade_notional",
                      "gl_cash", "gl_asset", "gl_pnl",
                      "diff_cash", "balance_check"]])

    # 3) FO MTM timeseries → GL MtM + reversals + control
    fo_ts = load_fo_mtm_timeseries(base_path)
    gl_mtm = generate_gl_mtm_postings(fo_ts)
    ctrl_mtm_gl = build_mtm_gl_control(fo_ts, gl_mtm)

    print("\n=== GL MtM postings (with reversals) ===")
    print(gl_mtm.sort_values(["customer_id", "isin", "ccy",
                              "posting_date", "posting_type", "account_code"]))

    print("\n=== Control: FO MtM level vs GL revaluation balance over time ===")
    print(ctrl_mtm_gl.sort_values(["customer_id", "isin", "ccy", "posting_date"]))

    # 4) Combined GL and thin ledger
    gl_all = pd.concat([gl_trade, gl_mtm], ignore_index=True)

    print("\n=== Combined GL (trades + MtM) – sample ===")
    print(gl_all.sort_values(["posting_date", "customer_id", "isin",
                              "deal_id", "account_code", "posting_type"]).head(20))

    # ---------- Thin general ledger ----------
    # DR = +, CR = - , then cumulative balance per account/ccy over time
    gl_all = gl_all.copy()
    gl_all["signed_amount"] = gl_all.apply(
        lambda r: r["amount"] if r["dr_cr"] == "DR" else -r["amount"],
        axis=1
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

    print("\n=== Thin general ledger (date, account, ccy, balance) ===")
    print(daily_moves[["posting_date", "account_code", "ccy", "balance"]])


if __name__ == "__main__":
    main()
