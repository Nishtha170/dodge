import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List

try:
    from .db import DB_PATH
except ImportError:
    from db import DB_PATH

DATASET_DIR = Path(os.environ.get("O2C_DATASET_DIR", Path(__file__).resolve().parents[1] / "sap-o2c-data"))

TABLES = [
    "sales_order_headers",
    "sales_order_items",
    "sales_order_schedule_lines",
    "outbound_delivery_headers",
    "outbound_delivery_items",
    "billing_document_headers",
    "billing_document_items",
    "billing_document_cancellations",
    "business_partners",
    "business_partner_addresses",
    "customer_company_assignments",
    "customer_sales_area_assignments",
    "journal_entry_items_accounts_receivable",
    "payments_accounts_receivable",
    "plants",
    "products",
    "product_descriptions",
    "product_plants",
    "product_storage_locations",
]


def _read_jsonl(folder: Path) -> Iterable[Dict[str, Any]]:
    for path in sorted(folder.glob("*.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)


def _normalize_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if value is None:
        return None
    return str(value)


def _infer_columns(records: List[Dict[str, Any]]) -> List[str]:
    columns = set()
    for record in records:
        columns.update(record.keys())
    return sorted(columns)


def _create_table(conn: sqlite3.Connection, table: str, columns: List[str]) -> None:
    cols = ", ".join([f"{col} TEXT" for col in columns])
    conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.execute(f"CREATE TABLE {table} ({cols})")


def _insert_rows(conn: sqlite3.Connection, table: str, columns: List[str], records: List[Dict[str, Any]]) -> None:
    if not records:
        return
    placeholders = ", ".join(["?" for _ in columns])
    columns_csv = ", ".join(columns)
    sql = f"INSERT INTO {table} ({columns_csv}) VALUES ({placeholders})"
    rows = []
    for record in records:
        row = [_normalize_value(record.get(col)) for col in columns]
        rows.append(row)
    conn.executemany(sql, rows)


def load_tables(conn: sqlite3.Connection) -> None:
    for table in TABLES:
        folder = DATASET_DIR / table
        if not folder.exists():
            print(f"Skipping missing folder: {folder}")
            continue
        records = list(_read_jsonl(folder))
        columns = _infer_columns(records)
        if not columns:
            print(f"Skipping empty table: {table}")
            continue
        _create_table(conn, table, columns)
        _insert_rows(conn, table, columns, records)
        print(f"Loaded {table}: {len(records)} rows")


def _ensure_graph_tables(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS nodes")
    conn.execute("DROP TABLE IF EXISTS edges")
    conn.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, type TEXT, label TEXT, props TEXT)")
    conn.execute("CREATE TABLE edges (id TEXT PRIMARY KEY, source_id TEXT, target_id TEXT, type TEXT, props TEXT)")


def _upsert_node(conn: sqlite3.Connection, node_id: str, node_type: str, label: str, props: Dict[str, Any]) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO nodes (id, type, label, props) VALUES (?, ?, ?, ?)",
        (node_id, node_type, label, json.dumps(props)),
    )


def _add_edge(conn: sqlite3.Connection, source_id: str, target_id: str, edge_type: str, props: Dict[str, Any]) -> None:
    edge_id = f"{source_id}|{edge_type}|{target_id}"
    conn.execute(
        "INSERT OR REPLACE INTO edges (id, source_id, target_id, type, props) VALUES (?, ?, ?, ?, ?)",
        (edge_id, source_id, target_id, edge_type, json.dumps(props)),
    )


def _fetch_map(conn: sqlite3.Connection, sql: str, key: str, value: str) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for row in conn.execute(sql):
        mapping[row[key]] = row[value]
    return mapping


def build_graph(conn: sqlite3.Connection) -> None:
    _ensure_graph_tables(conn)

    product_desc = _fetch_map(
        conn,
        "SELECT product, productDescription FROM product_descriptions WHERE language = 'EN'",
        "product",
        "productDescription",
    )

    for row in conn.execute("SELECT * FROM products"):
        data = dict(row)
        prod_id = f"PROD:{data['product']}"
        label = product_desc.get(data["product"], data["product"])
        _upsert_node(conn, prod_id, "Product", label, data)

    for row in conn.execute("SELECT * FROM business_partners"):
        data = dict(row)
        bp_id = f"BP:{data['businessPartner']}"
        label = data.get("businessPartnerName") or data.get("organizationBpName1") or data["businessPartner"]
        _upsert_node(conn, bp_id, "BusinessPartner", label, data)

    for row in conn.execute("SELECT * FROM business_partner_addresses"):
        data = dict(row)
        addr_id = f"ADDR:{data['addressId']}"
        label = f"{data.get('cityName', '')} {data.get('streetName', '')}".strip()
        _upsert_node(conn, addr_id, "Address", label or addr_id, data)
        bp_id = f"BP:{data['businessPartner']}"
        _add_edge(conn, bp_id, addr_id, "has_address", {})

    for row in conn.execute("SELECT * FROM plants"):
        data = dict(row)
        plant_id = f"PLANT:{data['plant']}"
        label = data.get("plantName") or data["plant"]
        _upsert_node(conn, plant_id, "Plant", label, data)

    for row in conn.execute("SELECT * FROM product_plants"):
        data = dict(row)
        prod_id = f"PROD:{data['product']}"
        plant_id = f"PLANT:{data['plant']}"
        _add_edge(conn, prod_id, plant_id, "available_in", data)

    for row in conn.execute("SELECT * FROM product_storage_locations"):
        data = dict(row)
        sloc_id = f"SLOC:{data['plant']}-{data['storageLocation']}"
        label = f"{data['plant']}-{data['storageLocation']}"
        _upsert_node(conn, sloc_id, "StorageLocation", label, data)
        prod_id = f"PROD:{data['product']}"
        plant_id = f"PLANT:{data['plant']}"
        _add_edge(conn, prod_id, sloc_id, "stored_in", {})
        _add_edge(conn, sloc_id, plant_id, "part_of", {})

    for row in conn.execute("SELECT * FROM sales_order_headers"):
        data = dict(row)
        so_id = f"SO:{data['salesOrder']}"
        label = f"Sales Order {data['salesOrder']}"
        _upsert_node(conn, so_id, "SalesOrder", label, data)
        if data.get("soldToParty"):
            bp_id = f"BP:{data['soldToParty']}"
            _add_edge(conn, so_id, bp_id, "sold_to", {})

    for row in conn.execute("SELECT * FROM sales_order_items"):
        data = dict(row)
        soi_id = f"SOI:{data['salesOrder']}-{data['salesOrderItem']}"
        label = f"SO Item {data['salesOrder']}-{data['salesOrderItem']}"
        _upsert_node(conn, soi_id, "SalesOrderItem", label, data)
        so_id = f"SO:{data['salesOrder']}"
        _add_edge(conn, so_id, soi_id, "has_item", {})
        if data.get("material"):
            prod_id = f"PROD:{data['material']}"
            _add_edge(conn, soi_id, prod_id, "ordered_product", {})
        if data.get("productionPlant"):
            plant_id = f"PLANT:{data['productionPlant']}"
            _add_edge(conn, soi_id, plant_id, "produced_at", {})
        if data.get("storageLocation") and data.get("productionPlant"):
            sloc_id = f"SLOC:{data['productionPlant']}-{data['storageLocation']}"
            _add_edge(conn, soi_id, sloc_id, "requested_from", {})

    for row in conn.execute("SELECT * FROM sales_order_schedule_lines"):
        data = dict(row)
        sl_id = f"SL:{data['salesOrder']}-{data['salesOrderItem']}-{data['scheduleLine']}"
        label = f"Schedule {data['salesOrder']}-{data['salesOrderItem']}-{data['scheduleLine']}"
        _upsert_node(conn, sl_id, "ScheduleLine", label, data)
        soi_id = f"SOI:{data['salesOrder']}-{data['salesOrderItem']}"
        _add_edge(conn, soi_id, sl_id, "has_schedule", {})

    for row in conn.execute("SELECT * FROM outbound_delivery_headers"):
        data = dict(row)
        del_id = f"DEL:{data['deliveryDocument']}"
        label = f"Delivery {data['deliveryDocument']}"
        _upsert_node(conn, del_id, "Delivery", label, data)
        if data.get("shippingPoint"):
            plant_id = f"PLANT:{data['shippingPoint']}"
            _add_edge(conn, del_id, plant_id, "shipped_from", {})

    for row in conn.execute("SELECT * FROM outbound_delivery_items"):
        data = dict(row)
        deli_id = f"DELI:{data['deliveryDocument']}-{data['deliveryDocumentItem']}"
        label = f"Delivery Item {data['deliveryDocument']}-{data['deliveryDocumentItem']}"
        _upsert_node(conn, deli_id, "DeliveryItem", label, data)
        del_id = f"DEL:{data['deliveryDocument']}"
        _add_edge(conn, del_id, deli_id, "has_item", {})
        if data.get("referenceSdDocument"):
            so_id = f"SO:{data['referenceSdDocument']}"
            _add_edge(conn, deli_id, so_id, "references_order", {})
        if data.get("referenceSdDocument") and data.get("referenceSdDocumentItem"):
            soi_id = f"SOI:{data['referenceSdDocument']}-{data['referenceSdDocumentItem']}"
            _add_edge(conn, deli_id, soi_id, "references_order_item", {})
        if data.get("plant"):
            plant_id = f"PLANT:{data['plant']}"
            _add_edge(conn, deli_id, plant_id, "delivered_from", {})
        if data.get("storageLocation") and data.get("plant"):
            sloc_id = f"SLOC:{data['plant']}-{data['storageLocation']}"
            _add_edge(conn, deli_id, sloc_id, "picked_from", {})

    for row in conn.execute("SELECT * FROM billing_document_headers"):
        data = dict(row)
        bill_id = f"BILL:{data['billingDocument']}"
        label = f"Billing {data['billingDocument']}"
        _upsert_node(conn, bill_id, "Billing", label, data)
        if data.get("soldToParty"):
            bp_id = f"BP:{data['soldToParty']}"
            _add_edge(conn, bill_id, bp_id, "billed_to", {})

    for row in conn.execute("SELECT * FROM billing_document_items"):
        data = dict(row)
        billi_id = f"BILLI:{data['billingDocument']}-{data['billingDocumentItem']}"
        label = f"Billing Item {data['billingDocument']}-{data['billingDocumentItem']}"
        _upsert_node(conn, billi_id, "BillingItem", label, data)
        bill_id = f"BILL:{data['billingDocument']}"
        _add_edge(conn, bill_id, billi_id, "has_item", {})
        if data.get("material"):
            prod_id = f"PROD:{data['material']}"
            _add_edge(conn, billi_id, prod_id, "billed_product", {})
        if data.get("referenceSdDocument"):
            del_id = f"DEL:{data['referenceSdDocument']}"
            _add_edge(conn, bill_id, del_id, "billed_from_delivery", {})
        if data.get("referenceSdDocument") and data.get("referenceSdDocumentItem"):
            deli_id = f"DELI:{data['referenceSdDocument']}-{data['referenceSdDocumentItem']}"
            _add_edge(conn, billi_id, deli_id, "billed_from_delivery_item", {})

    for row in conn.execute("SELECT * FROM journal_entry_items_accounts_receivable"):
        data = dict(row)
        je_id = f"JE:{data['accountingDocument']}-{data['accountingDocumentItem']}"
        label = f"Journal Entry {data['accountingDocument']}-{data['accountingDocumentItem']}"
        _upsert_node(conn, je_id, "JournalEntry", label, data)
        if data.get("referenceDocument"):
            bill_id = f"BILL:{data['referenceDocument']}"
            _add_edge(conn, je_id, bill_id, "references_billing", {})
        if data.get("customer"):
            bp_id = f"BP:{data['customer']}"
            _add_edge(conn, je_id, bp_id, "posted_to", {})

    for row in conn.execute("SELECT * FROM payments_accounts_receivable"):
        data = dict(row)
        pay_id = f"PAY:{data['accountingDocument']}-{data['accountingDocumentItem']}"
        label = f"Payment {data['accountingDocument']}-{data['accountingDocumentItem']}"
        _upsert_node(conn, pay_id, "Payment", label, data)
        je_id = f"JE:{data['accountingDocument']}-{data['accountingDocumentItem']}"
        _add_edge(conn, pay_id, je_id, "clears_entry", {})
        if data.get("customer"):
            bp_id = f"BP:{data['customer']}"
            _add_edge(conn, pay_id, bp_id, "paid_by", {})


def main() -> None:
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        load_tables(conn)
        build_graph(conn)
        conn.commit()
    finally:
        conn.close()
    print(f"Database ready at {DB_PATH}")


if __name__ == "__main__":
    main()
