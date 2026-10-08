# trino-neocarta

Xuất metadata của **Trino** (catalog → schema → bảng → cột, mô tả, kiểu dữ liệu, khoá chính/khoá ngoại, giá trị mẫu) thành **file Cypher** theo data model của [neocarta](https://github.com/neo4j-labs/neocarta) (Neo4j Labs), để chạy trên Neo4j bằng DBeaver / Neo4j Browser / cypher-shell. Hỗ trợ Trino xác thực bằng **OAuth2**.

```
Trino (OAuth2) ──SQL metadata──> export_cypher.py ──> metadata.cypher ──> Neo4j (DBeaver / Browser / cypher-shell)
```

neocarta chưa có connector cho Trino; project này viết `TrinoSchemaConnector` kế thừa các base class RDBMS của neocarta. `export_cypher.py` chạy đúng pipeline của neocarta nhưng ghi các câu lệnh ra file thay vì gửi vào Neo4j — đã kiểm chứng graph tạo từ file giống hệt graph khi ingest trực tiếp.

## 1. Cài đặt (máy chạy script)

Yêu cầu: Python 3.10+, [`uv`](https://docs.astral.sh/uv/) (hoặc pip), kết nối được tới Trino. Server Neo4j **không cần** Python.

```powershell
git clone https://github.com/luongphuong1506-bit/trino-neocarta.git
cd trino-neocarta
uv venv .venv --python 3.11
uv pip install --python .venv/Scripts/python.exe -r requirements.txt
```

## 2. Điền cấu hình

```powershell
copy config.example.env prod.env
notepad prod.env
```

Các tham số chính trong `prod.env` (file mẫu [config.example.env](config.example.env) có chú thích đầy đủ):

| Tham số | Ví dụ | Ý nghĩa |
|---|---|---|
| `TRINO_HOST` / `TRINO_PORT` | `trino.company.com` / `443` | Địa chỉ Trino |
| `TRINO_HTTP_SCHEME` | `https` | Bắt buộc `https` khi xác thực |
| `TRINO_VERIFY` | `true` / `C:\certs\ca.pem` | Kiểm tra chứng chỉ TLS (đường dẫn CA nếu chứng chỉ nội bộ) |
| `TRINO_AUTH` | `oauth2` | `oauth2` / `client_credentials` / `jwt` / `none` |
| `CATALOGS` | `postgres iceberg` | Một hoặc nhiều catalog |
| `SCHEMAS` | *(trống)* | Trống = tất cả; hoặc `raw`, `iceberg.sales` |
| `SAMPLE_VALUES` | `0` | Số giá trị mẫu mỗi cột (đọc dữ liệu thật); `0` = tắt |
| `EXCLUDE_COLUMNS` | `email phone` | Cột không lấy giá trị mẫu (PII) |
| `OUTPUT` | `out/metadata.cypher` | File xuất ra |
| `NEO4J_EDITION` | `community` | `community` → constraint UNIQUE (chạy được trên cả hai edition) |

`prod.env` đã nằm trong `.gitignore` — không commit file đã điền.

## 3. Chạy

```powershell
.\.venv\Scripts\python.exe export_cypher.py --env-file prod.env
```

Tham số dòng lệnh ghi đè giá trị trong file, ví dụ:

```powershell
.\.venv\Scripts\python.exe export_cypher.py --env-file prod.env --catalog iceberg --schemas sales -o out\sales.cypher
```

Tất cả catalog dùng **chung một kết nối** → với OAuth2 chỉ đăng nhập **một lần**, và ghi vào **một file**.

### Các chế độ xác thực (`TRINO_AUTH`)

| Giá trị | Cách hoạt động | Khi nào dùng |
|---|---|---|
| `oauth2` | Script in ra link đăng nhập và mở trình duyệt; đăng nhập xong script tự chạy tiếp. Token giữ trong suốt lần chạy | Người chạy tay |
| `client_credentials` | Script tự lấy token từ IdP bằng service account (`OAUTH2_TOKEN_URL`, `OAUTH2_CLIENT_ID`, `OAUTH2_CLIENT_SECRET`) và tự làm mới trước khi hết hạn | Chạy tự động / theo lịch |
| `jwt` | Dùng token có sẵn trong `TRINO_JWT_TOKEN` (token Keycloak mặc định chỉ sống 5 phút) | Thử nhanh |
| `none` | Không xác thực | Stack Docker test |

- Để trống `TRINO_USER` khi xác thực: Trino lấy user từ token; điền user khác sẽ bị coi là impersonation.
- Với `client_credentials` + Keycloak: client cần bật *Client authentication* và *Service accounts roles*; nếu Trino kiểm tra audience thì thêm *Audience mapper*; user Trino nhận được phụ thuộc `principal-field` của Trino (`sub` của service account là UUID).

## 4. Chạy file Cypher trên Neo4j

- **DBeaver**: mở file trong SQL editor kết nối Neo4j → **Execute script** (Alt+X). Nếu DBeaver hỏi giá trị cho `:Column`, `:Table`… thì tắt xử lý tham số ở *Preferences → Editors → SQL Editor → SQL Processing*. *(Chưa kiểm thử trên DBeaver.)*
- **Neo4j Browser**: dán nội dung file (chạy được nhiều câu cách nhau bởi `;`).
- **cypher-shell** (có sẵn trong `bin/` của Neo4j server): `cypher-shell -u neo4j -p <password> -f metadata.cypher`

File chạy lại nhiều lần không sinh trùng (MERGE theo id).

Kiểm tra sau khi chạy:

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

## 5. Ghi thẳng vào Neo4j (khi máy chạy script kết nối được Neo4j)

`ingest_trino.py` nhận cùng file cấu hình, thêm `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, `NEO4J_DATABASE`:

```powershell
.\.venv\Scripts\python.exe ingest_trino.py --env-file prod.env
```

## 6. Stack Docker để test (tuỳ chọn)

Chỉ dùng để thử nghiệm local: PostgreSQL 16 + Trino (catalog `postgres`, `iceberg` dùng JDBC catalog + file local, `memory`) + Neo4j 5, cấu hình RAM thấp (~2.3 GB). Mật khẩu trong `docker-compose.yml` chỉ dành cho test.

```powershell
docker compose up -d
```

| Dịch vụ | Địa chỉ | Tài khoản |
|---|---|---|
| Trino | http://localhost:8080 | không xác thực |
| Neo4j Browser | http://localhost:7474 (Bolt `7687`) | `neo4j` / `password123` |
| PostgreSQL | `localhost:5432`, db `shop` | `demo` / `demo` |

Tạo dữ liệu Iceberg mẫu (bash / Git Bash — PowerShell pipe chèn BOM làm Trino báo lỗi):

```bash
docker exec -i trino trino < iceberg/01_lakehouse.sql
```

Không cần file cấu hình: mặc định script trỏ vào stack này (`localhost:8080`, không xác thực).

```bash
.venv/Scripts/python.exe export_cypher.py --catalog postgres iceberg --values 3 --exclude-columns email full_name -o out/test.cypher
# bash / Git Bash (PowerShell không hỗ trợ "<" và pipe làm hỏng tiếng Việt)
docker exec -i neo4j cypher-shell -u neo4j -p password123 < out/test.cypher
```

Test schema evolution trên Iceberg: `iceberg/02_schema_evolution_v1.sql`, `iceberg/03_schema_evolution_v2.sql`.

Dọn dẹp: `docker compose stop` (giữ dữ liệu) hoặc `docker compose down -v` (xoá dữ liệu).

## Cấu trúc

| Đường dẫn | Nội dung |
|---|---|
| `export_cypher.py` | Quét Trino → file `.cypher` |
| `ingest_trino.py` | Quét Trino → ghi thẳng Neo4j; đọc cấu hình dùng chung |
| `trino_auth.py` | Xác thực Trino: OAuth2 / client credentials / JWT |
| `trino_neocarta/` | Connector Trino cho neocarta (`extract.py`, `transform.py`, `connector.py`) |
| `config.example.env` | File cấu hình mẫu |
| `docker-compose.yml`, `trino/`, `postgres/`, `iceberg/` | Stack và dữ liệu test |

## Hạn chế đã biết

- neocarta chỉ ghi thuộc tính khi **tạo node mới** (`ON CREATE SET`) và **không xoá** node cũ: đổi kiểu/comment không được cập nhật; cột/bảng bị xoá hoặc đổi tên vẫn còn trong graph.
- PK/FK chỉ lấy được với catalog chạy trên PostgreSQL (qua `system.query` pass-through).
- View được ghi như bảng thường; chưa có lineage.
- Connector dùng class nội bộ của neocarta (`neocarta.connectors.utils.*`) → `requirements.txt` cố định neocarta theo commit.
