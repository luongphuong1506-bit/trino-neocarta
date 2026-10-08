# trino-neocarta

Đưa metadata của **Trino** (catalog → schema → bảng → cột, mô tả, khoá chính/khoá ngoại, giá trị mẫu) vào **Neo4j** theo data model của [neocarta](https://github.com/neo4j-labs/neocarta) (Neo4j Labs).

neocarta chưa có connector cho Trino; project này viết `TrinoSchemaConnector` kế thừa các base class RDBMS của neocarta, nên dùng lại toàn bộ phần transform, load và MCP/CLI của neocarta.

```
Trino ──(SQL metadata, HTTP)──> trino_neocarta (Python) ──(MERGE, Bolt)──> Neo4j ──> neocarta CLI / MCP
                                        │
                                        └──> export_cypher.py ──> file .cypher (chạy bằng DBeaver / Neo4j Browser / cypher-shell)
```

## Cấu trúc

| Đường dẫn | Nội dung |
|---|---|
| `docker-compose.yml` | Stack test: PostgreSQL 16 + Trino + Neo4j 5 (cấu hình RAM thấp, ~2.3 GB) |
| `trino/etc/` | Cấu hình Trino; catalog `postgres`, `iceberg` (JDBC catalog + file local), `memory` |
| `postgres/init/` | Tạo DB `shop` (schema `raw` / `staging` / `mart`) và DB `iceberg_catalog` |
| `iceberg/` | SQL tạo dữ liệu Iceberg mẫu và bộ test schema evolution |
| `trino_neocarta/` | Connector Trino cho neocarta (`extract.py`, `transform.py`, `connector.py`) |
| `ingest_trino.py` | Quét metadata Trino và ghi thẳng vào Neo4j |
| `export_cypher.py` | Quét metadata Trino và xuất ra file `.cypher` (cho môi trường không có Python) |
| `trino_auth.py` | Xác thực Trino: none / JWT / Keycloak client-credentials / OAuth2 |

## 1. Cài đặt

Yêu cầu: Docker Desktop (Compose v2), Python 3.10+ và [`uv`](https://docs.astral.sh/uv/).

```powershell
# Python env (neocarta được cố định theo commit trong requirements.txt)
uv venv .venv --python 3.11
uv pip install --python .venv/Scripts/python.exe -r requirements.txt
```

## 2. Dựng stack test

```powershell
docker compose up -d                 # khởi động Postgres + Trino + Neo4j
docker compose ps                    # chờ trino "healthy"
```

| Dịch vụ | Địa chỉ | Tài khoản |
|---|---|---|
| Trino | http://localhost:8080 | user bất kỳ, không mật khẩu |
| Neo4j Browser | http://localhost:7474 (Bolt `7687`) | `neo4j` / `password123` |
| PostgreSQL | `localhost:5432`, db `shop` | `demo` / `demo` |

> Mật khẩu trên chỉ dành cho stack test local.

Tạo dữ liệu Iceberg mẫu (chạy bằng **bash / Git Bash**; PowerShell pipe sẽ chèn BOM làm Trino báo lỗi):

```bash
docker exec -i trino trino < iceberg/01_lakehouse.sql
```

Lần đầu dựng trên volume Postgres có sẵn (script init không chạy lại) thì tạo DB cho Iceberg JDBC catalog trước rồi restart Trino:

```bash
docker exec pg psql -U demo -d shop -f /docker-entrypoint-initdb.d/00_iceberg_catalog.sql
docker restart trino
```

## 3. Ingest metadata vào Neo4j (trực tiếp)

```powershell
# Toàn bộ schema của catalog postgres, 5 giá trị mẫu/cột, bỏ qua cột PII
.\.venv\Scripts\python.exe ingest_trino.py --catalog postgres --values 5 --exclude-columns email full_name

# Catalog iceberg
.\.venv\Scripts\python.exe ingest_trino.py --catalog iceberg --values 3 --exclude-columns full_name

# Chỉ một số schema
.\.venv\Scripts\python.exe ingest_trino.py --catalog postgres --schemas raw mart
```

| Tham số | Mặc định | Ý nghĩa |
|---|---|---|
| `--catalog` | `postgres` | Catalog Trino cần quét |
| `--schemas` | tất cả (trừ schema hệ thống) | Danh sách schema |
| `--values` | `0` (tắt) | Số giá trị mẫu mỗi cột (đọc dữ liệu thật) |
| `--exclude-columns` | (trống) | Cột không lấy giá trị mẫu (PII) |

Biến môi trường kết nối: `TRINO_HOST` (localhost), `TRINO_PORT` (8080), `TRINO_USER`, `NEO4J_URI` (bolt://localhost:7687), `NEO4J_USERNAME` (neo4j), `NEO4J_PASSWORD` (password123), `NEO4J_DATABASE` (neo4j).

## 4. Xuất file Cypher (server Neo4j không có Python)

Chạy ở máy có Python và kết nối được tới Trino, rồi đem file `.cypher` sang chạy trên Neo4j:

```powershell
.\.venv\Scripts\python.exe export_cypher.py --catalog postgres --values 5 --exclude-columns email full_name -o out\postgres.cypher
.\.venv\Scripts\python.exe export_cypher.py --catalog iceberg  --values 3 --exclude-columns full_name -o out\iceberg.cypher
```

Thêm tham số: `-o/--output` (bắt buộc), `--batch-size` (500 dòng mỗi câu `UNWIND`), `--neo4j-edition community|enterprise` (mặc định `community` → constraint UNIQUE, chạy được trên cả hai edition).

Chạy file trên Neo4j (chọn một):

```bash
cypher-shell -u neo4j -p <password> -f out/postgres.cypher         # có sẵn trong bin/ của Neo4j server
docker exec -i neo4j cypher-shell -u neo4j -p password123 < out/postgres.cypher   # stack test
```

- **Neo4j Browser**: dán nội dung file (chạy được nhiều câu cách nhau bởi `;`).
- **DBeaver**: mở file trong SQL editor kết nối Neo4j → *Execute script* (Alt+X). Nếu DBeaver hỏi giá trị cho `:Column`, `:Table`… thì tắt xử lý tham số ở *Preferences → Editors → SQL Editor → SQL Processing*. (Chưa kiểm thử trên DBeaver.)

File sinh ra chứa đúng các câu lệnh mà ingest trực tiếp chạy (đã kiểm chứng graph giống hệt) và chạy lại nhiều lần không sinh trùng.

## 5. Xác thực Trino (Keycloak)

Token được gửi qua header `Authorization: Bearer ...`; client từ chối gửi token qua `http`, production phải dùng `https`.

```powershell
$env:TRINO_HOST="trino.example.com"; $env:TRINO_PORT="443"; $env:TRINO_HTTP_SCHEME="https"
$env:TRINO_VERIFY="true"            # hoặc false, hoặc đường dẫn CA bundle
$env:TRINO_AUTH="keycloak"
$env:KEYCLOAK_TOKEN_URL="https://keycloak.example.com/realms/<realm>/protocol/openid-connect/token"
$env:KEYCLOAK_CLIENT_ID="neocarta-ingest"
$env:KEYCLOAK_CLIENT_SECRET="<secret>"
.\.venv\Scripts\python.exe export_cypher.py --catalog postgres -o out\postgres.cypher
```

| `TRINO_AUTH` | Cách lấy token |
|---|---|
| `none` (mặc định) | Không xác thực (stack test) |
| `keycloak` | Client credentials (service account), tự làm mới token trước khi hết hạn |
| `jwt` | Token có sẵn trong `TRINO_JWT_TOKEN` (token Keycloak mặc định chỉ sống 5 phút) |
| `oauth2` | Đăng nhập qua trình duyệt |

Khi bật xác thực, script không gửi `TRINO_USER` (Trino lấy user từ token); đặt user khác sẽ bị coi là impersonation.

## 6. Kiểm tra trong Neo4j

```cypher
// Toàn bộ graph metadata
MATCH p=(:Database)-[:HAS_SCHEMA]->(:Schema)-[:HAS_TABLE]->(:Table)-[:HAS_COLUMN]->(:Column) RETURN p;

// Bảng kèm mô tả
MATCH (d:Database)-[:HAS_SCHEMA]->(s:Schema)-[:HAS_TABLE]->(t:Table)
RETURN d.name, d.service, s.name, t.name, t.description ORDER BY s.name, t.name;

// Khoá ngoại
MATCH (t1:Table)-[:HAS_COLUMN]->(c1)-[:REFERENCES]->(c2)<-[:HAS_COLUMN]-(t2:Table)
RETURN t1.name, c1.name, c2.name, t2.name;
```

Tool của neocarta trên graph này:

```powershell
$env:NEO4J_URI="bolt://localhost:7687"; $env:NEO4J_USERNAME="neo4j"; $env:NEO4J_PASSWORD="password123"; $env:NEO4J_DATABASE="neo4j"
.\.venv\Scripts\neocarta.exe tool list-schemas --json
.\.venv\Scripts\neocarta.exe tool get-context-by-table-full-text-search --text-content "khách hàng" --json
```

## 7. Test schema evolution (Iceberg)

```bash
docker exec -i trino trino < iceberg/02_schema_evolution_v1.sql
.venv/Scripts/python.exe ingest_trino.py --catalog iceberg --schemas evo --values 3
docker exec -i trino trino < iceberg/03_schema_evolution_v2.sql
.venv/Scripts/python.exe ingest_trino.py --catalog iceberg --schemas evo --values 3
```

## Hạn chế đã biết

- neocarta chỉ ghi thuộc tính khi **tạo node mới** (`ON CREATE SET`) và **không xoá** node cũ: đổi kiểu/comment không được cập nhật; cột/bảng bị xoá hoặc đổi tên vẫn còn trong graph.
- PK/FK chỉ lấy được với catalog chạy trên PostgreSQL (qua `system.query` pass-through).
- View được ghi như bảng thường; chưa có lineage.
- Connector dùng class nội bộ của neocarta (`neocarta.connectors.utils.*`) → phải cố định phiên bản neocarta.

## Dọn dẹp

```powershell
docker compose stop          # tắt, giữ dữ liệu
docker compose down -v       # xoá cả dữ liệu
```
