# import argparse
import json
import logging
from pathlib import Path
from typing import Tuple

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import re


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CURRENCY_RE = re.compile(r"[^\d\.\-\(\)]+")
ORDER_RE = re.compile(r"\b([0-9A-F]{8}-[0-9A-F]{6}-[0-9A-F]{5})\b", re.I)


def load_orders_jsons(orders_dir: str | Path) -> list[dict]:
    orders_dir = Path(orders_dir)
    files = sorted(orders_dir.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No .json files found in {orders_dir}")

    orders = []
    for fp in files:
        with fp.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        # keep filename for traceability
        obj["_source_file"] = fp.name
        orders.append(obj)
    return orders


def normalize_orders(orders: list[dict]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      - orders_df: one row per order
      - lines_df: one row per line item (quantity not exploded)
    """
    # Order-level fields
    orders_df = pd.json_normalize(
        orders,
        sep=".",
        meta=["orderNumber", "_source_file", "createdAt", "status", "orderChannel", "orderFulfillment"],
        errors="ignore",
    )

    # The above creates many columns; select what we care about (add more as needed)
    keep_order_cols = [
        "orderNumber",
        "_source_file",
        "createdAt",
        "status",
        "orderChannel",
        "orderFulfillment",
        "transaction.grossAmount",
        "transaction.netAmount",
        "transaction.feeAmount",
        "transaction.directFeeAmount",
        "transaction.productAmount",
        "transaction.shippingAmount",
    ]
    for c in keep_order_cols:
        if c not in orders_df.columns:
            orders_df[c] = np.nan
    orders_df = orders_df[keep_order_cols].copy()

    # Parse order date
    orders_df["order_date"] = pd.to_datetime(orders_df["createdAt"], errors="coerce", utc=True).dt.date

    # Lines: explode products
    # Each order dict has a "products" list (per your sample) :contentReference[oaicite:1]{index=1}
    lines_rows = []
    for o in orders:
        order_id = o.get("orderNumber")
        created_at = o.get("createdAt")
        products = o.get("products", []) or []
        for p in products:
            lines_rows.append({
                "orderNumber": order_id,
                "createdAt": created_at,
                "order_date": pd.to_datetime(created_at, errors="coerce", utc=True).date() if created_at else pd.NaT,
                "product_name": p.get("name"),
                "productId": str(p.get("productId")) if p.get("productId") is not None else None,
                "skuId": str(p.get("skuId")) if p.get("skuId") is not None else None,
                "unitPrice": float(p.get("unitPrice")) if p.get("unitPrice") is not None else np.nan,
                "extendedPrice": float(p.get("extendedPrice")) if p.get("extendedPrice") is not None else np.nan,
                "quantity": int(p.get("quantity")) if p.get("quantity") is not None else 0,
                "product_url": p.get("url"),
            })

    lines_df = pd.DataFrame(lines_rows)
    if lines_df.empty:
        # still return empty frames with expected columns
        lines_df = pd.DataFrame(columns=[
            "orderNumber","createdAt","order_date","product_name","productId","skuId",
            "unitPrice","extendedPrice","quantity","product_url"
        ])

    # Ensure types
    lines_df["quantity"] = lines_df["quantity"].fillna(0).astype(int)
    lines_df["unitPrice"] = pd.to_numeric(lines_df["unitPrice"], errors="coerce")
    lines_df["extendedPrice"] = pd.to_numeric(lines_df["extendedPrice"], errors="coerce")

    return orders_df, lines_df

def normalize_orders_with_refunds(orders: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    One-pass normalizer:
      - orders_df: 1 row per order with refund totals integrated
      - lines_df:  1 row per (order, skuId) line with product-level refund totals integrated

    Refund behavior:
      - Order-level: sums refunds[].amount and refunds[].shippingAmount; flags if any refund.type == "Full"
      - Line-level: sums refunds[].products[].amount by skuId (and productId if present)
      - Full refunds often have refunds[].products empty; line-level refund amounts may be 0 in that case
    """
    order_rows = []
    line_rows = []

    for o in orders:
        order_id = o.get("orderNumber")
        created_at = o.get("createdAt")

        # ----- refund accumulation (order-level + per-sku) -----
        refund_total_amount = 0.0
        refund_total_shipping = 0.0
        refund_count = 0
        has_full_refund = False

        # map: skuId -> refunded product amount (sum across refund events)
        refunded_by_sku: dict[str, float] = {}

        refunds = o.get("refunds", []) or []
        for r in refunds:
            refund_count += 1
            r_type = r.get("type")
            if r_type == "Full":
                has_full_refund = True

            refund_total_amount += float(r.get("amount", 0.0) or 0.0)
            refund_total_shipping += float(r.get("shippingAmount", 0.0) or 0.0)

            # partial refunds can include product-level refunds
            for rp in (r.get("products", []) or []):
                sku = rp.get("skuId")
                if sku is None:
                    continue
                sku = str(sku)
                refunded_by_sku[sku] = refunded_by_sku.get(sku, 0.0) + float(rp.get("amount", 0.0) or 0.0)

        # ----- build order row -----
        tx = o.get("transaction", {}) or {}
        gross = float(tx.get("grossAmount", 0.0) or 0.0)
        net = float(tx.get("netAmount", 0.0) or 0.0)

        order_rows.append({
            "orderNumber": order_id,
            "createdAt": created_at,
            "order_date": pd.to_datetime(created_at, errors="coerce", utc=True).date() if created_at else pd.NaT,
            "status": o.get("status"),
            "orderChannel": o.get("orderChannel"),
            "orderFulfillment": o.get("orderFulfillment"),
            "grossAmount": gross,
            "netAmount_reported": net,
            "feeAmount": float(tx.get("feeAmount", 0.0) or 0.0),
            "directFeeAmount": float(tx.get("directFeeAmount", 0.0) or 0.0),
            "productAmount": float(tx.get("productAmount", 0.0) or 0.0),
            "shippingAmount": float(tx.get("shippingAmount", 0.0) or 0.0),

            # integrated refund fields
            "refund_total_amount": refund_total_amount,
            "refund_total_shippingAmount": refund_total_shipping,
            "refund_count": refund_count,
            "has_full_refund": has_full_refund,

            # simple derived fields
            "gross_after_refunds": gross - refund_total_amount,
            # Conservative: subtract gross refunds from reported net (may understate if fees are also reversed)
            "net_after_refunds_conservative": net - refund_total_amount,
        })

        # ----- build line rows (with refund-by-sku baked in) -----
        products = o.get("products", []) or []
        for p in products:
            sku = p.get("skuId")
            sku_str = str(sku) if sku is not None else None

            qty = int(p.get("quantity", 0) or 0)
            unit_price = float(p.get("unitPrice", 0.0) or 0.0)
            ext_price = float(p.get("extendedPrice", unit_price * qty) or 0.0)

            refunded_amt = refunded_by_sku.get(sku_str, 0.0) if sku_str else 0.0

            line_rows.append({
                "orderNumber": order_id,
                "createdAt": created_at,
                "order_date": pd.to_datetime(created_at, errors="coerce", utc=True).date() if created_at else pd.NaT,

                "product_name": p.get("name"),
                "productId": str(p.get("productId")) if p.get("productId") is not None else None,
                "skuId": sku_str,
                "quantity": qty,
                "unitPrice": unit_price,
                "extendedPrice": ext_price,
                "product_url": p.get("url"),

                # integrated refund fields at line level
                "refund_product_amount": refunded_amt,
                "extended_after_refund": ext_price - refunded_amt,

                # if you want a quick flag
                "is_refunded_line": refunded_amt > 0,
            })

    orders_df = pd.DataFrame(order_rows)
    lines_df = pd.DataFrame(line_rows)

    # Ensure numeric types
    if not orders_df.empty:
        for c in ["grossAmount", "netAmount_reported", "feeAmount", "directFeeAmount", "productAmount",
                  "shippingAmount", "refund_total_amount", "refund_total_shippingAmount", "gross_after_refunds",
                  "net_after_refunds_conservative"]:
            orders_df[c] = pd.to_numeric(orders_df[c], errors="coerce").fillna(0.0)

    if not lines_df.empty:
        for c in ["quantity", "unitPrice", "extendedPrice", "refund_product_amount", "extended_after_refund"]:
            lines_df[c] = pd.to_numeric(lines_df[c], errors="coerce").fillna(0.0)

        # quantity should be int
        lines_df["quantity"] = lines_df["quantity"].astype(int)

    return orders_df, lines_df


def parse_money(x) -> float:
    """
    '$1.25' -> 1.25
    '($0.36)' -> -0.36
    ''/None -> 0.0
    """
    if x is None:
        return 0.0
    s = str(x).strip()
    if not s:
        return 0.0

    neg = s.startswith("(") and s.endswith(")")
    s = CURRENCY_RE.sub("", s).strip("()")
    val = float(s) if s else 0.0
    return -val if neg else val


def parse_payments_html(path: str):
    html = open(path, "r", encoding="utf-8").read()
    soup = BeautifulSoup(html, "lxml")

    # --- Table 1: Past Payments (per-order totals) ---
    orders_tbl = soup.select_one('table[data-testid="Payments_Orders_PastPayments"]')
    if orders_tbl is None:
        raise ValueError("Could not find Payments_Orders_PastPayments table")

    rows = []
    for tr in orders_tbl.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue

        a = tds[0].find("a")
        order_number = a.get_text(strip=True) if a else tds[0].get_text(strip=True)

        buyer = tds[1].get_text(strip=True)
        order_date = pd.to_datetime(tds[2].get_text(strip=True), errors="coerce")

        total_sale = parse_money(tds[3].get_text(strip=True))
        total_fees = parse_money(tds[4].get_text(strip=True))
        refunded_orders = parse_money(tds[5].get_text(" ", strip=True))
        refunded_fees = parse_money(tds[6].get_text(strip=True))

        # Optional: detect Direct icon in the first cell (there's an <img ... direct_icon.png>)
        is_direct = bool(tds[0].select_one('img[src*="tcgplayerdirect_icon.png"]'))

        rows.append({
            "orderNumber": order_number,
            "buyerName_payments": buyer,
            "orderDate_payments": order_date,
            "totalSale": total_sale,
            "totalFees": total_fees,
            "refundedOrders": refunded_orders,
            "refundedFees": refunded_fees,
            "isDirectIcon_payments": is_direct,
        })

    payment_orders_df = pd.DataFrame(rows)

    # --- Table 2: Adjustments (row-level) ---
    adj_tbl = soup.select_one('table[data-testid="Payment_Orders_Adjustments"]')
    if adj_tbl is None:
        # some reports might omit adjustments
        adjustments_df = pd.DataFrame(columns=["adjustmentAmount", "reason", "orderNumber_from_reason", "adjustment_type"])
        return payment_orders_df, adjustments_df

    adj_rows = []
    for tr in adj_tbl.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        amt = parse_money(tds[0].get_text(strip=True))
        reason = tds[1].get_text(" ", strip=True)

        # Pull orderNumber if this is "Direct Seller Order <ORDER> Refund"
        m = ORDER_RE.search(reason)
        order_from_reason = m.group(1).upper() if m else None

        # Lightweight categorization (you can extend this)
        if "Direct Seller Order" in reason and "Refund" in reason:
            adj_type = "DirectRefundAdj"
        elif "Discrepancy" in reason and "Reason - [Missing]" in reason:
            adj_type = "DirectMissingItem"
        else:
            adj_type = "Other"

        adj_rows.append({
            "adjustmentAmount": amt,
            "reason": reason,
            "orderNumber_from_reason": order_from_reason,
            "adjustment_type": adj_type,
        })

    adjustments_df = pd.DataFrame(adj_rows)

    return payment_orders_df, adjustments_df

def parse_payments_html_folder(folder: str | Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    folder = Path(folder)
    files = sorted(folder.glob("*.html"))
    if not files:
        raise FileNotFoundError(f"No .html files found in {folder}")

    all_orders = []
    all_adjustments = []

    for fp in files:
        payment_orders_df, adjustments_df = parse_payments_html(fp)

        payment_orders_df = payment_orders_df.copy()
        adjustments_df = adjustments_df.copy()

        # track provenance
        payment_orders_df["_source_file"] = fp.name
        adjustments_df["_source_file"] = fp.name

        all_orders.append(payment_orders_df)
        all_adjustments.append(adjustments_df)

    payment_orders_all = pd.concat(all_orders, ignore_index=True) if all_orders else pd.DataFrame()
    adjustments_all = pd.concat(all_adjustments, ignore_index=True) if all_adjustments else pd.DataFrame()

    # ---- De-dupe strategy ----
    # Same order might appear across multiple exports if date ranges overlap.
    # Use a conservative dedupe key on the orders table.
    if not payment_orders_all.empty:
        dedupe_cols = [
            "orderNumber",
            "orderDate_payments",
            "totalSale",
            "totalFees",
            "refundedOrders",
            "refundedFees",
        ]
        # Keep only columns that exist (in case you tweak parse output later)
        dedupe_cols = [c for c in dedupe_cols if c in payment_orders_all.columns]
        payment_orders_all = payment_orders_all.drop_duplicates(subset=dedupe_cols, keep="last")

    # Adjustments are row-level; de-dupe on (amount, reason, orderNumber_from_reason, source_file) is usually ok
    if not adjustments_all.empty:
        dedupe_cols = [
            "adjustmentAmount",
            "reason",
            "orderNumber_from_reason",
            "adjustment_type",
        ]
        dedupe_cols = [c for c in dedupe_cols if c in adjustments_all.columns]
        adjustments_all = adjustments_all.drop_duplicates(subset=dedupe_cols, keep="last")

    return payment_orders_all, adjustments_all


def main():

    output_dir = r'C:\temp\orders\output'
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    orders_dir = r'C:\temp\orders'
    orders = load_orders_jsons(orders_dir)
    orders_df, lines_df = normalize_orders_with_refunds(orders)
    products_df = lines_df.groupby(['product_name', 'productId', 'skuId']).agg({'quantity': 'sum'}).reset_index()

    payments_dir = r'C:\temp\orders\payments'
    payment_orders_df, adjustments_df = parse_payments_html_folder(payments_dir)

    orders_df.to_excel(output_dir / "orders.xlsx", index=False)
    lines_df.to_excel(output_dir / "order_lines.xlsx", index=False)
    payment_orders_df.to_excel(output_dir / "payments_orders.xlsx", index=False)
    adjustments_df.to_excel(output_dir / "payments_adjustments.xlsx", index=False)

    with pd.ExcelWriter(output_dir / "aggregated_summary.xlsx") as writer:
        orders_df.to_excel(writer, sheet_name="orders", index=False)
        lines_df.to_excel(writer, sheet_name="order_lines", index=False)
        products_df.to_excel(writer, sheet_name="products", index=False)
        payment_orders_df.to_excel(writer, sheet_name="payments_orders", index=False)
        adjustments_df.to_excel(writer, sheet_name="payments_adjustments", index=False)

    # parser = argparse.ArgumentParser(description='Extract TCGPlayer order information')
    # args = parser.parse_args()
    print('done')

if __name__ == "__main__":
    main()
