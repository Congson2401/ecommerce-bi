# 🛒 E-commerce BI Pipeline

Dự án xây dựng **Data Warehouse và Dashboard BI** cho hệ thống E-commerce chạy trên Odoo ERP. Pipeline tự động hóa toàn bộ quá trình từ data thô → analytics-ready tables → Power BI dashboard.

## Tech Stack

| Thành phần | Công nghệ |
|---|---|
| Nguồn dữ liệu | PostgreSQL (Odoo ERP) |
| Orchestration | Apache Airflow (Docker) |
| Transformation | dbt (dbt-core + dbt-postgres) |
| Data Warehouse | PostgreSQL schema `dw` |
| Dashboard | Power BI |

---

## Kiến trúc tổng quan

```
PostgreSQL (Odoo)
      │
      ▼
┌─────────────────────────────────┐
│  Airflow DAG                    │
│  Schedule: 06:00 hàng ngày      │
│                                 │
│  1. wait_for_postgres           │
│  2. check_source_freshness      │
│     ├── OK  → transform         │
│     ├── WARN → log + transform  │
│     └── ERROR → dừng pipeline   │
│  3. dbt_transform               │
│  4. dbt_quality (test+snapshot) │
│  5. log_stats → success         │
└─────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────┐
│  dbt Pipeline (schema: dw)      │
│                                 │
│  Staging (14 models)            │
│    → Intermediate (5 models)    │
│      → Mart (5 dim + 5 fact)    │
│  + 11 Snapshots (SCD Type 2)    │
│  + 8 Data Quality Tests         │
│  + 2 Seeds (KPI Targets)        │
└─────────────────────────────────┘
      │
      ▼
  Power BI Dashboard
```

---

## Cấu trúc thư mục

```
ecommerce-bi/
├── airflow/                        # Airflow setup
│   ├── dockerfile                  # Custom image: Airflow + dbt
│   ├── docker-compose.yml          # Container config
│   └── airflow/
│       ├── airflow.cfg             # Airflow configuration
│       └── dags/
│           └── ecommerce_bi_dag.py # DAG chính — orchestrate toàn bộ pipeline
│
├── dbt/
│   └── ecommerce_dbt/              # dbt project
│       ├── dbt_project.yml         # Project config, schema targets
│       ├── create_profile.py       # Script tạo profiles.yml tự động
│       │
│       ├── models/
│       │   ├── staging/            # Tầng 1: làm sạch data Odoo
│       │   │   ├── sources.yml     # Khai báo nguồn + freshness policy
│       │   │   ├── schema.yml      # Schema tests cho staging
│       │   │   ├── stg_sale_orders.sql
│       │   │   ├── stg_sale_order_lines.sql
│       │   │   ├── stg_purchase_orders.sql
│       │   │   ├── stg_purchase_order_lines.sql
│       │   │   ├── stg_supplier_info.sql
│       │   │   ├── stg_stock_pickings.sql
│       │   │   ├── stg_stock_moves.sql
│       │   │   ├── stg_stock_quant.sql
│       │   │   ├── stg_invoices.sql
│       │   │   ├── stg_invoice_lines.sql
│       │   │   ├── stg_payments.sql
│       │   │   ├── stg_products.sql
│       │   │   ├── stg_partners.sql
│       │   │   ├── stg_locations.sql
│       │   │   └── stg_supplier_info.sql
│       │   │
│       │   ├── intermediate/       # Tầng 2: business logic
│       │   │   ├── int_sales.sql           # Cost, gross margin
│       │   │   ├── int_delivery.sql        # Fill rate, delay category
│       │   │   ├── int_purchasing.sql      # Lead time, price compliance
│       │   │   ├── int_inventory_moves.sql # Running stock (window function)
│       │   │   └── int_finance.sql         # Invoice-payment matching, DSO
│       │   │
│       │   └── mart/               # Tầng 3: star schema cho BI
│       │       ├── schema.yml      # FK tests, accepted_values
│       │       ├── dim_date.sql            # Date spine từ data thực tế
│       │       ├── dim_product.sql
│       │       ├── dim_partner.sql         # VIP/Silver/Regular tier
│       │       ├── dim_location.sql
│       │       ├── dim_payment_method.sql  # Static lookup (8 phương thức VN)
│       │       ├── fact_sales.sql          # Grain: order line
│       │       ├── fact_delivery.sql       # Grain: picking
│       │       ├── fact_inventory.sql      # Grain: stock move
│       │       ├── fact_purchasing.sql     # Grain: PO line
│       │       └── fact_finance.sql        # Grain: invoice
│       │
│       ├── snapshots/              # SCD Type 2 — lịch sử thay đổi
│       │   ├── snap_sale_orders.sql
│       │   ├── snap_sale_order_lines.sql
│       │   ├── snap_purchase_orders.sql
│       │   ├── snap_purchase_order_lines.sql
│       │   ├── snap_products.sql
│       │   ├── snap_partners.sql
│       │   ├── snap_stock_quant.sql
│       │   ├── snap_stock_pickings.sql
│       │   ├── snap_stock_moves.sql
│       │   ├── snap_invoices.sql
│       │   └── snap_payments.sql
│       │
│       ├── seeds/                  # KPI targets (static CSV)
│       │   ├── target_sale.csv     # Doanh số mục tiêu theo tháng/category
│       │   └── target_inventory.csv # Tồn kho mục tiêu (fill rate, turnover)
│       │
│       └── tests/                  # Custom data quality tests
│           ├── test_revenue_not_negative.sql
│           ├── test_discount_range.sql
│           ├── test_qty_sold_positive.sql
│           ├── test_gross_margin_range.sql
│           ├── test_lead_time_realistic.sql
│           ├── test_running_stock_not_negative.sql
│           ├── test_monthly_data_completeness.sql
│           └── test_sales_inventory_consitency.sql
│
├── data/                           # Raw data (.rar)
├── powerbi/                        # Power BI report files (.pbix)
└── logs/                           # Pipeline logs
```

---

## Cài đặt & Chạy

### Yêu cầu
- Docker Desktop
- PostgreSQL (cài trên host hoặc container riêng)
- Python 3.8+
- Power BI Desktop (để xem dashboard)

### 1. Clone repo

```bash
git clone <repo-url>
cd ecommerce-bi
```

### 2. Tạo profiles.yml cho dbt

```bash
cd dbt/ecommerce_dbt
python create_profile.py
```

File sẽ được tạo tại `~/.dbt/profiles.yml` với target `docker` trỏ vào PostgreSQL.

### 3. Khởi động Airflow

```bash
cd airflow
docker compose up -d
```

Airflow UI: http://localhost:8081
- Username: `admin`
- Password: xem trong `airflow/passwords.json`

### 4. Chạy dbt thủ công (tùy chọn)

```bash
cd dbt/ecommerce_dbt

# Cài packages
dbt deps

# Load KPI targets
dbt seed

# Chạy toàn bộ pipeline
dbt run

# Chạy tests
dbt test

# Chạy snapshots
dbt snapshot
```

### 5. Kết nối Power BI

Mở file `.pbix` trong thư mục `powerbi/`, cập nhật credentials:
```
File → Options → Data Source Settings
→ localhost;ecommerce_db
→ Edit Permissions → Username: postgres
```

---

## dbt Pipeline

### Staging — Làm sạch data Odoo

| Model | Nguồn | Điểm đặc biệt |
|---|---|---|
| stg_sale_orders | sale_order | Filter `state != 'draft'` |
| stg_sale_order_lines | sale_order_line | Filter `display_type IS NULL` (bỏ section/note) |
| stg_stock_moves | stock_move | Double-join stock_location phân loại in/out/internal |
| stg_products | product_template + product_product | JSONB operator `->>'en_US'` cho tên đa ngôn ngữ |
| stg_partners | res_partner | customer_rank/supplier_rank → partner_type |
| stg_invoices | account_move | Filter `out_invoice` + `out_refund` |

### Intermediate — Business Logic

| Model | Logic chính |
|---|---|
| int_sales | `LEAST(standard_cost, price × 0.95)` — tính cost tránh margin âm |
| int_delivery | Fill rate từ stock_moves, delay_category 5 nhóm |
| int_purchasing | `DISTINCT ON` dedup picking, lead time variance, price compliance ±5% |
| int_inventory_moves | Running stock bằng window `SUM() OVER (PARTITION BY product_id ORDER BY move_date)` |
| int_finance | Invoice-payment matching: cùng partner + 30 ngày + ±1% amount |

### Mart — Star Schema

```
fact_sales ──────────┐
fact_delivery ───────┤
fact_inventory ──────┼──→ dim_date
fact_purchasing ─────┤    dim_product
fact_finance ────────┘    dim_partner
                          dim_location
                          dim_payment_method
```

Tất cả fact tables dùng `materialized='incremental'` với `write_date` watermark.

---

## Data Quality

### Schema Tests (sources.yml / schema.yml)
- `unique` + `not_null` trên tất cả primary keys
- `accepted_values` cho move_type, payment_type
- `relationships` (FK checks) từ fact → dim

### Custom Tests
| Test | Mô tả |
|---|---|
| test_revenue_not_negative | Revenue không được âm |
| test_discount_range | Discount trong khoảng [0, 100] |
| test_qty_sold_positive | Số lượng bán phải > 0 |
| test_gross_margin_range | Margin trong [-50%, 100%] |
| test_lead_time_realistic | Lead time trong [1, 90] ngày |
| test_running_stock_not_negative | Running stock không âm quá -1000 |
| test_monthly_data_completeness | Mỗi tháng phải có ≥ 10 đơn hàng |
| test_sales_inventory_consitency | Sales qty vs inventory out qty chênh lệch ≤ 5% |

### Source Freshness
```yaml
warn_after:  { count: 24, period: hour }
error_after: { count: 48, period: hour }
```
DAG dừng pipeline nếu data cũ hơn 48 giờ (exit code 2).

---

## Snapshots (SCD Type 2)

11 snapshots ghi lại lịch sử thay đổi vào schema `dw`, chạy **sau** `dbt test` để chỉ snapshot data đã validated:

`snap_sale_orders` · `snap_sale_order_lines` · `snap_purchase_orders` · `snap_purchase_order_lines` · `snap_products` · `snap_partners` · `snap_stock_quant` · `snap_stock_pickings` · `snap_stock_moves` · `snap_invoices` · `snap_payments`

---

## Dashboard Power BI

5 trang báo cáo:

| Trang | Nội dung |
|---|---|
| Executive Overview | Tổng quan KPI: doanh thu, margin, fill rate, DSO |
| Sale & Profitability | Phân tích doanh thu, gross margin theo category/thời gian |
| Inventory & Warehouse | Tồn kho, fill rate, turnover rate, stock movement |
| Procurement & Supplier | Lead time, price compliance, supplier performance |
| Finance & Cash Flow | DSO, outstanding amount, payment method breakdown |

---

## Lưu ý quan trọng

> **`airflow/passwords.json`** chứa credentials — không commit lên repo công khai.

> **`dbt/ecommerce_dbt/create_profile.py`** ghi đè `~/.dbt/profiles.yml` — chạy cẩn thận nếu đã có profiles cho project khác.

> **Power BI** cần PostgreSQL đang chạy mới refresh được data — không có cache offline.
> **Các file .pbix (dashboard) và .rar (raw data) có dung lượng quá lớn nên không được đưa lên đây
