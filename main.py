"""
搜索评测 Web 应用 - FastAPI 后端
"""

import os
import csv
import json
import time
import sqlite3
import asyncio
from io import BytesIO, StringIO
from datetime import datetime
from typing import List

import aiosqlite
import openpyxl
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, HTMLResponse
from pydantic import BaseModel

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(APP_DIR, "data.db")
SCREENSHOTS_DIR = os.path.join(APP_DIR, "static", "screenshots")
STATIC_DIR = os.path.join(APP_DIR, "static")

os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

app = FastAPI(title="搜索评测系统")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# ============================================================
# Database
# ============================================================

COLUMNS = [
    "id", "query", "query_length", "business_category", "search_count",
    "is_short_query", "frequency_level", "original_group", "search_result",
    "text_screenshot_url", "ai_screenshot_url",
    "relevance", "accuracy", "ranking", "diversity",
    "quality_threshold", "copy_recommendation", "click_desire",
    "vs_text_search", "ai_search_score",
    "created_at", "updated_at"
]

EXCEL_COLUMNS = [
    "query", "query_length", "business_category", "search_count",
    "is_short_query", "frequency_level", "original_group", "search_result",
    "expected_screenshot", "expected_result",
    "text_screenshot_url", "ai_screenshot_url",
    "relevance", "accuracy", "ranking", "diversity",
    "quality_threshold", "copy_recommendation", "click_desire",
    "vs_text_search", "ai_search_score",
]

EXCEL_HEADERS = [
    "query", "query_length", "业务分类", "搜索次数",
    "是否短query", "频率层级", "原始分组", "搜索结果",
    "文本搜截图", "AI搜截图",
    "相关性", "准确性", "排序合理性", "多样性",
    "质量门槛（与文本搜索对比）", "文案/推荐语", "点击欲望",
    "与文本搜相比", "文本搜索综合分", "ai搜索综合分",
    "业务分类"
]

# Map Chinese Excel header -> DB field name
HEADER_TO_FIELD = {
    "query": "query",
    "query_length": "query_length",
    "业务分类": "business_category",
    "搜索次数": "search_count",
    "是否短query": "is_short_query",
    "频率层级": "frequency_level",
    "原始分组": "original_group",
    "期望结果截图": "expected_screenshot",
    "期望搜索结果": "expected_result",
    "搜索结果": "search_result",
    "文本搜截图": "text_screenshot_url",
    "AI搜截图": "ai_screenshot_url",
    "ai搜截图": "ai_screenshot_url",
    "相关性": "relevance",
    "准确性": "accuracy",
    "排序合理性": "ranking",
    "多样性": "diversity",
    "质量门槛": "quality_threshold",
    "质量门槛（与文本搜索对比）": "quality_threshold",
    "文案/推荐语": "copy_recommendation",
    "点击欲望": "click_desire",
    "与文本搜相比": "vs_text_search",
    "文本搜索综合分": None,
    "ai搜索综合分": "ai_search_score",
    "AI搜索综合分": "ai_search_score",
    "业务分类2": "business_category",
}


# ============================================================
# Custom Columns
# ============================================================

CUSTOM_COLUMNS_PATH = os.path.join(APP_DIR, "custom_columns.json")


def load_custom_columns():
    """Load custom column definitions: [{field, label, type}]"""
    if os.path.exists(CUSTOM_COLUMNS_PATH):
        with open(CUSTOM_COLUMNS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_custom_columns(cols):
    with open(CUSTOM_COLUMNS_PATH, "w", encoding="utf-8") as f:
        json.dump(cols, f, ensure_ascii=False, indent=2)


def get_dynamic_header_mapping():
    """Return HEADER_TO_FIELD merged with custom columns"""
    mapping = dict(HEADER_TO_FIELD)
    for col in load_custom_columns():
        mapping[col["label"]] = col["field"]
    return mapping


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT,
            query_length INTEGER,
            business_category TEXT,
            search_count TEXT,
            is_short_query TEXT,
            frequency_level TEXT,
            original_group TEXT,
            search_result TEXT,
            expected_screenshot TEXT,
            expected_result TEXT,
            text_screenshot_url TEXT,
            ai_screenshot_url TEXT,
            relevance REAL,
            accuracy REAL,
            ranking REAL,
            diversity REAL,
            quality_threshold REAL,
            copy_recommendation TEXT,
            click_desire REAL,
            vs_text_search TEXT,
            ai_search_score REAL,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    # Test sets table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS test_sets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TEXT
        )
    """)
    # Ensure test_set_id column exists
    existing = {row[1] for row in conn.execute("PRAGMA table_info(queries)").fetchall()}
    if "test_set_id" not in existing:
        conn.execute("ALTER TABLE queries ADD COLUMN test_set_id INTEGER DEFAULT 1")
    if "expected_screenshot" not in existing:
        conn.execute("ALTER TABLE queries ADD COLUMN expected_screenshot TEXT")
    if "expected_result" not in existing:
        conn.execute("ALTER TABLE queries ADD COLUMN expected_result TEXT")
    # Ensure at least one test set exists
    count = conn.execute("SELECT COUNT(*) FROM test_sets").fetchone()[0]
    if count == 0:
        conn.execute("INSERT INTO test_sets (id, name, created_at) VALUES (1, '测试集 1', datetime('now'))")
    # Add custom columns to DB if missing
    existing = {row[1] for row in conn.execute("PRAGMA table_info(queries)").fetchall()}
    for col in load_custom_columns():
        if col["field"] not in existing:
            col_type = "REAL" if col.get("type") == "number" else "TEXT"
            conn.execute(f'ALTER TABLE queries ADD COLUMN {col["field"]} {col_type}')
    conn.commit()
    conn.close()


init_db()


async def get_db():
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    return db


# ============================================================
# WebSocket Manager
# ============================================================

class ConnectionManager:
    def __init__(self):
        self.connections: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.connections.append(ws)

    def disconnect(self, ws: WebSocket):
        self.connections.remove(ws)

    async def broadcast(self, message: dict):
        for ws in self.connections[:]:
            try:
                await ws.send_json(message)
            except Exception:
                self.connections.remove(ws)


manager = ConnectionManager()


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)


# ============================================================
# API Routes
# ============================================================

from fastapi import Query as QueryParam


@app.get("/api/test-sets")
async def get_test_sets():
    db = await get_db()
    cursor = await db.execute("SELECT * FROM test_sets ORDER BY id")
    rows = await cursor.fetchall()
    await db.close()
    return [dict(row) for row in rows]


@app.post("/api/test-sets")
async def create_test_set(body: dict):
    name = body.get("name", "").strip()
    copy_from = body.get("copy_from")  # test_set_id to copy queries from
    if not name:
        raise HTTPException(400, "名称不能为空")

    db = await get_db()
    now = datetime.now().isoformat()
    cursor = await db.execute("INSERT INTO test_sets (name, created_at) VALUES (?, ?)", [name, now])
    new_id = cursor.lastrowid

    if copy_from:
        # Copy queries but clear scoring/screenshot fields
        src_cursor = await db.execute("SELECT * FROM queries WHERE test_set_id = ?", [copy_from])
        src_rows = [dict(r) for r in await src_cursor.fetchall()]

        score_fields = ['relevance', 'accuracy', 'ranking', 'diversity', 'quality_threshold',
                        'copy_recommendation', 'click_desire', 'vs_text_search', 'ai_search_score',
                        'text_screenshot_url', 'ai_screenshot_url', 'search_result']
        custom_cols = load_custom_columns()
        score_fields += [c["field"] for c in custom_cols]

        for row in src_rows:
            row.pop("id", None)
            row["test_set_id"] = new_id
            row["created_at"] = now
            row["updated_at"] = now
            for sf in score_fields:
                row[sf] = None

            fields = [k for k in row.keys()]
            placeholders = ", ".join(["?"] * len(fields))
            field_names = ", ".join(fields)
            await db.execute(f"INSERT INTO queries ({field_names}) VALUES ({placeholders})", list(row.values()))

    await db.commit()
    await db.close()
    return {"id": new_id, "name": name, "created_at": now}


@app.put("/api/test-sets/{set_id}")
async def rename_test_set(set_id: int, body: dict):
    name = body.get("name", "").strip()
    if not name:
        raise HTTPException(400, "名称不能为空")
    db = await get_db()
    await db.execute("UPDATE test_sets SET name = ? WHERE id = ?", [name, set_id])
    await db.commit()
    await db.close()
    return {"ok": True}


@app.delete("/api/test-sets/{set_id}")
async def delete_test_set(set_id: int):
    db = await get_db()
    count = await db.execute("SELECT COUNT(*) FROM test_sets")
    total = (await count.fetchone())[0]
    if total <= 1:
        await db.close()
        raise HTTPException(400, "至少保留一个测试集")
    await db.execute("DELETE FROM queries WHERE test_set_id = ?", [set_id])
    await db.execute("DELETE FROM test_sets WHERE id = ?", [set_id])
    await db.commit()
    await db.close()
    return {"ok": True}


@app.get("/api/queries")
async def get_queries(test_set_id: int = QueryParam(default=None)):
    db = await get_db()
    if test_set_id:
        cursor = await db.execute("SELECT * FROM queries WHERE test_set_id = ? ORDER BY id", [test_set_id])
    else:
        cursor = await db.execute("SELECT * FROM queries ORDER BY id")
    rows = await cursor.fetchall()
    await db.close()
    return [dict(row) for row in rows]


class QueryUpdate(BaseModel):
    model_config = {"extra": "allow"}


@app.put("/api/queries/{query_id}")
async def update_query(query_id: int, data: QueryUpdate):
    updates = data.model_extra or {}
    if not updates:
        raise HTTPException(400, "No fields to update")

    updates["updated_at"] = datetime.now().isoformat()
    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
    values = list(updates.values()) + [query_id]

    db = await get_db()
    await db.execute(f"UPDATE queries SET {set_clause} WHERE id = ?", values)
    await db.commit()

    cursor = await db.execute("SELECT * FROM queries WHERE id = ?", [query_id])
    row = await cursor.fetchone()
    await db.close()

    if row is None:
        raise HTTPException(404, "Query not found")
    return dict(row)


@app.post("/api/upload-screenshot")
async def upload_screenshot(query_id: int, type: str, file: UploadFile = File(...)):
    if type not in ("text", "ai"):
        raise HTTPException(400, "type must be 'text' or 'ai'")

    filename = f"q{query_id}_{type}_{int(datetime.now().timestamp())}.jpg"
    filepath = os.path.join(SCREENSHOTS_DIR, filename)

    content = await file.read()
    with open(filepath, "wb") as f:
        f.write(content)

    url = f"/static/screenshots/{filename}"
    col = "text_screenshot_url" if type == "text" else "ai_screenshot_url"

    db = await get_db()
    await db.execute(
        f"UPDATE queries SET {col} = ?, updated_at = ? WHERE id = ?",
        [url, datetime.now().isoformat(), query_id]
    )
    await db.commit()
    await db.close()

    await manager.broadcast({
        "type": "screenshot_update",
        "query_id": query_id,
        "screenshot_type": type,
        "url": url
    })

    return {"url": url, "query_id": query_id}


@app.post("/api/import")
async def import_excel(file: UploadFile = File(...), skip_unknown: int = 0, test_set_id: int = 1):
    import re
    import zipfile
    from xml.etree import ElementTree as ET

    content = await file.read()

    # --- Extract WPS DISPIMG images from xlsx zip ---
    dispimg_map = {}  # image_id -> image bytes
    try:
        zf = zipfile.ZipFile(BytesIO(content))
        # Parse cellimages.xml to get name -> rId mapping
        name_to_rid = {}
        if 'xl/cellimages.xml' in zf.namelist():
            ci_xml = zf.read('xl/cellimages.xml')
            root = ET.fromstring(ci_xml)
            for elem in root.iter():
                if 'cNvPr' in elem.tag:
                    name = elem.get('name', '')
                    if name.startswith('ID_'):
                        # Find sibling blip element for rId
                        parent = None
                        for pic in root.iter():
                            if 'pic' in pic.tag:
                                cnv = pic.find('.//' + elem.tag.split('}')[0] + '}cNvPr[@name="' + name + '"]') if '}' in elem.tag else None
                                # Simpler: iterate all pics, match by name
                                pass
                        pass

            # Re-parse more carefully
            ns = {
                'etc': 'http://www.wps.cn/officeDocument/2017/etCustomData',
                'xdr': 'http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing',
                'a': 'http://schemas.openxmlformats.org/drawingml/2006/main',
                'r': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships',
            }
            name_to_rid = {}
            for cell_img in root.findall('.//etc:cellImage', ns):
                cnv_pr = cell_img.find('.//xdr:cNvPr', ns)
                blip = cell_img.find('.//a:blip', ns)
                if cnv_pr is not None and blip is not None:
                    img_name = cnv_pr.get('name', '')
                    rid = blip.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed', '')
                    if img_name and rid:
                        name_to_rid[img_name] = rid

        # Parse cellimages.xml.rels to get rId -> file path
        rid_to_path = {}
        if 'xl/_rels/cellimages.xml.rels' in zf.namelist():
            rels_xml = zf.read('xl/_rels/cellimages.xml.rels')
            rels_root = ET.fromstring(rels_xml)
            for rel in rels_root:
                rid = rel.get('Id', '')
                target = rel.get('Target', '')
                if rid and target:
                    rid_to_path[rid] = 'xl/' + target

        # Build final map: image_name -> bytes
        for img_name, rid in name_to_rid.items():
            fpath = rid_to_path.get(rid)
            if fpath and fpath in zf.namelist():
                dispimg_map[img_name] = zf.read(fpath)

        zf.close()
    except Exception:
        pass

    # --- Load workbook with formulas (not data_only) to read DISPIMG ---
    wb_formula = openpyxl.load_workbook(BytesIO(content), data_only=False)
    ws_formula = wb_formula.active

    # --- Load workbook with data_only for values ---
    wb = openpyxl.load_workbook(BytesIO(content), data_only=True)
    ws = wb.active

    # --- Parse headers (row 1) and map to DB fields ---
    header_row = []
    for col_idx in range(1, ws.max_column + 1):
        h = ws.cell(1, col_idx).value
        header_row.append(str(h).strip() if h else '')

    col_mapping = {}  # col_idx (1-based) -> db_field
    screenshot_cols = {}  # col_idx -> 'text' or 'ai'
    unknown_headers = []

    for col_idx, header in enumerate(header_row, start=1):
        if not header:
            continue
        dynamic_mapping = get_dynamic_header_mapping()
        field = dynamic_mapping.get(header)
        if field:
            if field == 'text_screenshot_url':
                screenshot_cols[col_idx] = 'text'
            elif field == 'ai_screenshot_url':
                screenshot_cols[col_idx] = 'ai'
            else:
                col_mapping[col_idx] = field
        else:
            # Try case-insensitive / partial match
            matched = False
            for k, v in dynamic_mapping.items():
                if v and (k.lower() in header.lower() or header.lower() in k.lower()):
                    col_mapping[col_idx] = v
                    matched = True
                    break
            if not matched:
                unknown_headers.append(header)

    if unknown_headers:
        if not skip_unknown:
            return {"needs_confirmation": True, "unknown_columns": unknown_headers, "imported": 0, "duplicates_count": 0}

    # Find query column
    query_col = None
    for cidx, field in col_mapping.items():
        if field == 'query':
            query_col = cidx
            break
    if query_col is None:
        return {"error": "未找到 query 列", "imported": 0, "duplicates_count": 0}

    db = await get_db()

    cursor = await db.execute("SELECT query FROM queries WHERE test_set_id = ?", [test_set_id])
    existing = {row[0] for row in await cursor.fetchall()}

    imported = 0
    duplicates = []
    now = datetime.now().isoformat()
    dispimg_re = re.compile(r'DISPIMG\("([^"]+)"')

    # DB fields for insert (excluding screenshots which are handled separately)
    db_fields = EXCEL_COLUMNS  # all possible fields

    for row_idx in range(2, ws.max_row + 1):
        query_val = ws.cell(row_idx, query_col).value
        if query_val is None or str(query_val).strip() == "":
            continue

        query_str = str(query_val).strip()
        if query_str in existing:
            duplicates.append(query_str)
            continue

        # Build row dict from column mapping
        row_dict = {f: None for f in EXCEL_COLUMNS}
        for col_idx, field in col_mapping.items():
            val = ws.cell(row_idx, col_idx).value
            if isinstance(val, str) and val.startswith("="):
                val = None
            row_dict[field] = val

        row_data = [row_dict.get(f) for f in EXCEL_COLUMNS]

        # Also include custom columns
        custom_cols = load_custom_columns()
        all_fields = EXCEL_COLUMNS + [c["field"] for c in custom_cols]
        row_data_full = [row_dict.get(f) for f in all_fields]

        field_names = ", ".join(all_fields + ["test_set_id", "created_at", "updated_at"])
        placeholders = ", ".join(["?"] * (len(all_fields) + 3))
        await db.execute(
            f"INSERT INTO queries ({field_names}) VALUES ({placeholders})",
            row_data_full + [test_set_id, now, now]
        )
        existing.add(query_str)
        imported += 1

        new_id_cursor = await db.execute("SELECT last_insert_rowid()")
        new_id = (await new_id_cursor.fetchone())[0]

        # Extract DISPIMG images for screenshot columns
        for col_idx, stype in screenshot_cols.items():
            formula_val = ws_formula.cell(row_idx, col_idx).value
            if formula_val and isinstance(formula_val, str) and 'DISPIMG' in formula_val:
                m = dispimg_re.search(formula_val)
                if m:
                    img_id = m.group(1)
                    img_bytes = dispimg_map.get(img_id)
                    if img_bytes:
                        ext = 'png' if img_bytes[:4] == b'\x89PNG' else 'jpg'
                        filename = f"q{new_id}_{stype}_{int(datetime.now().timestamp())}.{ext}"
                        filepath = os.path.join(SCREENSHOTS_DIR, filename)
                        with open(filepath, "wb") as f:
                            f.write(img_bytes)
                        url = f"/static/screenshots/{filename}"
                        col_name = "text_screenshot_url" if stype == "text" else "ai_screenshot_url"
                        await db.execute(f"UPDATE queries SET {col_name} = ? WHERE id = ?", [url, new_id])

    await db.commit()
    await db.close()

    result = {"imported": imported, "duplicates_count": len(duplicates)}
    if duplicates:
        result["duplicates"] = duplicates

    await manager.broadcast({"type": "data_refresh"})
    return result


@app.get("/api/export/excel")
async def export_excel():
    from openpyxl.drawing.image import Image as XlImage
    from openpyxl.utils import get_column_letter
    from PIL import Image as PILImage

    db = await get_db()
    cursor = await db.execute("SELECT * FROM queries ORDER BY id")
    rows = await cursor.fetchall()
    await db.close()

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sheet1"

    ws.append(EXCEL_HEADERS)

    text_col_idx = EXCEL_COLUMNS.index("text_screenshot_url") + 1
    ai_col_idx = EXCEL_COLUMNS.index("ai_screenshot_url") + 1

    for row_idx, row in enumerate(rows, start=2):
        row_dict = dict(row)
        row_data = []
        for col in EXCEL_COLUMNS:
            val = row_dict.get(col)
            if col in ("text_screenshot_url", "ai_screenshot_url"):
                row_data.append(None)
            else:
                row_data.append(val)
        ws.append(row_data)

        for col_idx, field in [(text_col_idx, "text_screenshot_url"), (ai_col_idx, "ai_screenshot_url")]:
            url = row_dict.get(field)
            if url:
                img_path = os.path.join(APP_DIR, url.lstrip("/").replace("/", os.sep))
                if os.path.exists(img_path):
                    try:
                        pil_img = PILImage.open(img_path)
                        if pil_img.width > 960:
                            ratio = 960 / pil_img.width
                            pil_img = pil_img.resize((960, int(pil_img.height * ratio)), PILImage.LANCZOS)
                        pil_img = pil_img.convert("RGB")
                        tmp_buf = BytesIO()
                        pil_img.save(tmp_buf, "JPEG", quality=80)
                        tmp_buf.seek(0)

                        img = XlImage(tmp_buf)
                        cell_w = 180
                        img.height = int(cell_w * pil_img.height / pil_img.width)
                        img.width = cell_w
                        cell_ref = f"{get_column_letter(col_idx)}{row_idx}"
                        ws.add_image(img, cell_ref)
                        ws.row_dimensions[row_idx].height = max(ws.row_dimensions[row_idx].height or 15, img.height * 0.75)
                    except Exception:
                        pass

    ws.column_dimensions[get_column_letter(text_col_idx)].width = 28
    ws.column_dimensions[get_column_letter(ai_col_idx)].width = 28

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=export.xlsx"}
    )


@app.get("/api/export/csv")
async def export_csv():
    db = await get_db()
    cursor = await db.execute("SELECT * FROM queries ORDER BY id")
    rows = await cursor.fetchall()
    await db.close()

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(EXCEL_HEADERS)

    for row in rows:
        row_dict = dict(row)
        writer.writerow([row_dict.get(col) for col in EXCEL_COLUMNS])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=export.csv"}
    )


@app.delete("/api/queries/{query_id}")
async def delete_query(query_id: int):
    db = await get_db()
    await db.execute("DELETE FROM queries WHERE id = ?", [query_id])
    await db.commit()
    await db.close()
    return {"deleted": query_id}


@app.delete("/api/queries")
async def delete_all_queries(no_backup: int = 0, test_set_id: int = None):
    import shutil
    import glob

    backup_name = None
    if not no_backup:
        backup_dir = os.path.join(APP_DIR, "backups")
        os.makedirs(backup_dir, exist_ok=True)
        backup_name = f"data_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2(DB_PATH, os.path.join(backup_dir, backup_name))

        for f in glob.glob(os.path.join(backup_dir, "data_backup_*.db")):
            age = time.time() - os.path.getmtime(f)
            if age > 7 * 86400:
                os.remove(f)

    db = await get_db()
    if test_set_id:
        cursor = await db.execute("SELECT COUNT(*) FROM queries WHERE test_set_id = ?", [test_set_id])
        count = (await cursor.fetchone())[0]
        await db.execute("DELETE FROM queries WHERE test_set_id = ?", [test_set_id])
    else:
        cursor = await db.execute("SELECT COUNT(*) FROM queries")
        count = (await cursor.fetchone())[0]
        await db.execute("DELETE FROM queries")
    try:
        await db.execute("DELETE FROM sqlite_sequence WHERE name='queries'")
    except Exception:
        pass
    await db.commit()
    await db.close()
    await manager.broadcast({"type": "data_refresh"})
    result = {"deleted": count}
    if backup_name:
        result["backup"] = backup_name
    return result


@app.post("/api/restore")
async def restore_backup(filename: str):
    """从备份恢复数据"""
    import shutil
    backup_path = os.path.join(APP_DIR, "backups", filename)
    if not os.path.exists(backup_path):
        raise HTTPException(404, "Backup not found")
    shutil.copy2(backup_path, DB_PATH)
    await manager.broadcast({"type": "data_refresh"})
    return {"restored": filename}


@app.get("/api/backups")
async def list_backups():
    """列出所有可用备份"""
    backup_dir = os.path.join(APP_DIR, "backups")
    if not os.path.exists(backup_dir):
        return []
    import glob
    files = glob.glob(os.path.join(backup_dir, "data_backup_*.db"))
    files.sort(key=os.path.getmtime, reverse=True)
    return [{"filename": os.path.basename(f), "time": datetime.fromtimestamp(os.path.getmtime(f)).strftime('%Y-%m-%d %H:%M:%S')} for f in files]


@app.post("/api/queries")
async def create_query(data: QueryUpdate):
    fields = data.model_dump(exclude_unset=True)
    now = datetime.now().isoformat()
    fields["created_at"] = now
    fields["updated_at"] = now

    cols = ", ".join(fields.keys())
    placeholders = ", ".join("?" * len(fields))
    values = list(fields.values())

    db = await get_db()
    cursor = await db.execute(f"INSERT INTO queries ({cols}) VALUES ({placeholders})", values)
    await db.commit()
    new_id = cursor.lastrowid

    cursor = await db.execute("SELECT * FROM queries WHERE id = ?", [new_id])
    row = await cursor.fetchone()
    await db.close()
    return dict(row)


# ============================================================
# Analysis API
# ============================================================

@app.get("/api/analysis")
async def get_analysis(test_set_id: int = QueryParam(default=None)):
    db = await get_db()
    if test_set_id:
        cursor = await db.execute("SELECT * FROM queries WHERE test_set_id = ?", [test_set_id])
    else:
        cursor = await db.execute("SELECT * FROM queries")
    rows = [dict(r) for r in await cursor.fetchall()]
    await db.close()

    import statistics

    score_fields = ['relevance', 'accuracy', 'ranking', 'diversity', 'quality_threshold', 'copy_recommendation', 'click_desire']

    def to_float(v):
        if v is None: return None
        try: return float(v)
        except (ValueError, TypeError): return None

    # Overview
    total = len(rows)
    scored = [r for r in rows if to_float(r.get('relevance')) is not None]
    scored_count = len(scored)
    ai_scores = [to_float(r['ai_search_score']) for r in rows if to_float(r.get('ai_search_score')) is not None]

    overview = {
        "total": total,
        "scored": scored_count,
        "ai_avg": round(statistics.mean(ai_scores), 2) if ai_scores else 0,
    }

    # Dimensions
    dimensions = []
    for f in score_fields:
        vals = [to_float(r[f]) for r in rows if to_float(r.get(f)) is not None]
        if vals:
            dimensions.append({
                "name": f,
                "mean": round(statistics.mean(vals), 2),
                "min": min(vals),
                "max": max(vals),
                "count": len(vals),
            })
        else:
            dimensions.append({"name": f, "mean": 0, "min": 0, "max": 0, "count": 0})

    # VS text search
    vs_vals = [r['vs_text_search'] for r in rows if r.get('vs_text_search') is not None]
    better = sum(1 for v in vs_vals if str(v).strip() == '1')
    same = sum(1 for v in vs_vals if str(v).strip() == '0')
    worse = sum(1 for v in vs_vals if str(v).strip() == '-1')
    vs_text = {"better": better, "same": same, "worse": worse, "total": better + same + worse}

    # By category
    from collections import defaultdict
    cat_data = defaultdict(list)
    for r in rows:
        cat = r.get('business_category') or '未分类'
        cat_data[cat].append(r)

    by_category = []
    for cat, cat_rows in sorted(cat_data.items(), key=lambda x: -len(x[1])):
        cat_info = {"category": cat, "n": len(cat_rows), "dims": {}}
        for f in score_fields:
            vals = [to_float(r[f]) for r in cat_rows if to_float(r.get(f)) is not None]
            cat_info["dims"][f] = round(statistics.mean(vals), 2) if vals else 0
        ai = [to_float(r['ai_search_score']) for r in cat_rows if to_float(r.get('ai_search_score')) is not None]
        cat_info["ai_avg"] = round(statistics.mean(ai), 2) if ai else 0
        # VS for this category
        cat_vs = [r['vs_text_search'] for r in cat_rows if r.get('vs_text_search') is not None]
        cat_info["vs"] = {
            "better": sum(1 for v in cat_vs if str(v).strip() == '1'),
            "same": sum(1 for v in cat_vs if str(v).strip() == '0'),
            "worse": sum(1 for v in cat_vs if str(v).strip() == '-1'),
        }
        by_category.append(cat_info)

    # By query level (short/long based on is_short_query field)
    short_rows = [r for r in rows if r.get('is_short_query') and '短' in str(r['is_short_query'])]
    long_rows = [r for r in rows if r.get('is_short_query') and '长' in str(r['is_short_query'])]

    def calc_group_dims(group_rows):
        result = {}
        for f in score_fields:
            vals = [to_float(r[f]) for r in group_rows if to_float(r.get(f)) is not None]
            result[f] = round(statistics.mean(vals), 2) if vals else 0
        ai = [to_float(r['ai_search_score']) for r in group_rows if to_float(r.get('ai_search_score')) is not None]
        result['ai_avg'] = round(statistics.mean(ai), 2) if ai else 0
        return result

    by_query_level = {
        "short": {"n": len(short_rows), "dims": calc_group_dims(short_rows)},
        "long": {"n": len(long_rows), "dims": calc_group_dims(long_rows)},
    }

    # By frequency
    freq_groups = defaultdict(list)
    for r in rows:
        freq = r.get('frequency_level') or '未知'
        freq_groups[freq].append(r)

    by_frequency = {}
    for freq, freq_rows in freq_groups.items():
        by_frequency[freq] = {"n": len(freq_rows), "dims": calc_group_dims(freq_rows)}

    # TOP5 / BOTTOM5
    scored_with_ai = [r for r in rows if to_float(r.get('ai_search_score')) is not None]
    sorted_by_ai = sorted(scored_with_ai, key=lambda r: to_float(r['ai_search_score']), reverse=True)

    def pick_fields(r):
        return {"id": r["id"], "query": r["query"], "ai_score": to_float(r["ai_search_score"]),
                "category": r.get("business_category")}

    top5 = [pick_fields(r) for r in sorted_by_ai[:5]]
    bottom5 = [pick_fields(r) for r in sorted_by_ai[-5:]]

    return {
        "overview": overview,
        "dimensions": dimensions,
        "vs_text": vs_text,
        "by_category": by_category,
        "by_query_level": by_query_level,
        "by_frequency": by_frequency,
        "top5": top5,
        "bottom5": bottom5,
    }


@app.post("/api/analysis/ai-text")
async def ai_text_analysis():
    """Call Kimi API for deep analysis"""
    import httpx

    api_key = os.environ.get("KIMI_API_KEY", "")
    if not api_key:
        raise HTTPException(400, "KIMI_API_KEY not configured")

    # Get analysis data
    from starlette.testclient import TestClient
    # Simpler: just call get_analysis directly
    analysis = await get_analysis()

    prompt = f"""你是一个搜索质量评测专家。以下是 AI 搜索评测的统计数据，请给出专业的分析报告（中文），包括：
1. 总体评价
2. 各维度优劣势分析
3. 与文本搜索对比的结论
4. 按业务分类的差异化表现
5. 改进建议

数据：
- 总览：{json.dumps(analysis['overview'], ensure_ascii=False)}
- 各维度均分：{json.dumps(analysis['dimensions'], ensure_ascii=False)}
- AI搜 vs 文本搜：{json.dumps(analysis['vs_text'], ensure_ascii=False)}
- 按业务分类：{json.dumps(analysis['by_category'][:6], ensure_ascii=False)}
"""

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.moonshot.cn/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": "moonshot-v1-8k",
                "messages": [{"role": "user", "content": prompt}],
            }
        )
        resp.raise_for_status()
        data = resp.json()
        return {"text": data["choices"][0]["message"]["content"]}


@app.post("/api/export/report")
async def export_report():
    """Generate Word report with charts and analysis"""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    matplotlib.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei']
    matplotlib.rcParams['axes.unicode_minus'] = False
    import numpy as np

    try:
        from docx import Document
        from docx.shared import Pt, Inches, Cm
        from docx.enum.text import WD_ALIGN_PARAGRAPH
    except ImportError:
        raise HTTPException(500, "python-docx not installed")

    analysis = await get_analysis()
    doc = Document()

    # Title
    title = doc.add_heading('AI 搜索评测分析报告', 0)
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    doc.add_paragraph(f'生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M")}', style='Normal').alignment = WD_ALIGN_PARAGRAPH.CENTER
    doc.add_page_break()

    # 1. Overview
    doc.add_heading('1. 总体概览', level=1)
    ov = analysis['overview']
    doc.add_paragraph(f"总 Query 数: {ov['total']}")
    doc.add_paragraph(f"已评分数: {ov['scored']}")
    doc.add_paragraph(f"AI 搜索综合均分: {ov['ai_avg']}")
    doc.add_paragraph(f"文本搜索综合均分: {ov['text_avg']}")

    # 2. Dimensions chart
    doc.add_heading('2. 各维度得分分析', level=1)
    dims = analysis['dimensions']
    dim_labels = {'relevance': '相关性', 'accuracy': '准确性', 'ranking': '排序合理性', 'diversity': '多样性', 'quality_threshold': '质量门槛', 'copy_recommendation': '文案/推荐语', 'click_desire': '点击欲望'}

    fig, ax = plt.subplots(figsize=(8, 4))
    names = [dim_labels.get(d['name'], d['name']) for d in dims]
    values = [d['mean'] for d in dims]
    bars = ax.bar(names, values, color='#0071e3')
    ax.set_ylim(0, 5)
    ax.set_ylabel('得分')
    ax.set_title('各评分维度均值')
    for bar, v in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, f'{v:.2f}', ha='center', fontsize=9)
    plt.tight_layout()
    img_buf = BytesIO()
    plt.savefig(img_buf, format='png', dpi=150)
    plt.close()
    img_buf.seek(0)
    doc.add_picture(img_buf, width=Inches(6))

    sorted_dims = sorted(dims, key=lambda d: d['mean'], reverse=True)
    doc.add_paragraph(f"表现最好: {dim_labels.get(sorted_dims[0]['name'])}（{sorted_dims[0]['mean']}分）")
    doc.add_paragraph(f"表现最弱: {dim_labels.get(sorted_dims[-1]['name'])}（{sorted_dims[-1]['mean']}分）")

    # 3. VS comparison
    doc.add_heading('3. AI 搜索 vs 文本搜索', level=1)
    vs = analysis['vs_text']
    total_vs = vs['total'] or 1

    fig, ax = plt.subplots(figsize=(6, 4))
    labels = ['AI更优', '持平', 'AI更差']
    sizes = [vs['better'], vs['same'], vs['worse']]
    colors = ['#34c759', '#ffcc00', '#ff3b30']
    ax.pie(sizes, labels=labels, colors=colors, autopct='%1.0f%%', startangle=90, textprops={'fontsize': 12})
    ax.set_title('AI搜索 vs 文本搜索对比')
    img_buf = BytesIO()
    plt.savefig(img_buf, format='png', dpi=150)
    plt.close()
    img_buf.seek(0)
    doc.add_picture(img_buf, width=Inches(4))
    doc.add_paragraph(f"AI搜在 {vs['better']/total_vs*100:.0f}% 场景优于文本搜，{vs['worse']/total_vs*100:.0f}% 场景表现更差。")

    # 4. By category
    doc.add_heading('4. 按业务分类分析', level=1)
    cats = analysis['by_category'][:8]

    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(cats))
    w = 0.25
    ax.bar(x - w, [c['ai_avg'] for c in cats], w, label='AI综合分', color='#0071e3')
    ax.bar(x, [c['dims']['accuracy'] for c in cats], w, label='准确性', color='#ff9500')
    ax.bar(x + w, [c['dims']['click_desire'] for c in cats], w, label='点击欲望', color='#af52de')
    ax.set_xticks(x)
    ax.set_xticklabels([c['category'] for c in cats], rotation=15)
    ax.set_ylim(0, 5)
    ax.legend()
    ax.set_title('各业务分类关键维度得分')
    plt.tight_layout()
    img_buf = BytesIO()
    plt.savefig(img_buf, format='png', dpi=150)
    plt.close()
    img_buf.seek(0)
    doc.add_picture(img_buf, width=Inches(6))

    # 5. Conclusion
    doc.add_heading('5. 结论与建议', level=1)
    doc.add_paragraph(f"本次评测共覆盖 {ov['total']} 条 query，有效评分 {ov['scored']} 条。")
    doc.add_paragraph(f"AI搜索整体表现{'优于' if vs['better'] > vs['worse'] else '弱于'}文本搜索（胜率 {vs['better']/total_vs*100:.0f}%）。")
    doc.add_paragraph(f"重点改进方向：{dim_labels.get(sorted_dims[-1]['name'])}（当前仅 {sorted_dims[-1]['mean']} 分）。")

    buf = BytesIO()
    doc.save(buf)
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers={"Content-Disposition": "attachment; filename=AI_search_report.docx"}
    )


# ============================================================
# Custom Columns API
# ============================================================

@app.get("/api/custom-columns")
async def get_custom_columns():
    return load_custom_columns()


@app.post("/api/custom-columns")
async def add_custom_column(body: dict):
    label = body.get("label", "").strip()
    col_type = body.get("type", "text")  # "text" or "number"
    if not label:
        raise HTTPException(400, "列名不能为空")

    # Generate field name from label (safe identifier)
    field = "custom_" + label.replace(" ", "_").replace("/", "_").replace("（", "_").replace("）", "_")
    # Ensure unique
    cols = load_custom_columns()
    existing_fields = {c["field"] for c in cols}
    existing_labels = {c["label"] for c in cols}
    if label in existing_labels:
        raise HTTPException(400, f"列 '{label}' 已存在")
    if field in existing_fields:
        field = field + "_" + str(len(cols))

    new_col = {"field": field, "label": label, "type": col_type}
    cols.append(new_col)
    save_custom_columns(cols)

    # ALTER TABLE
    col_type_sql = "REAL" if col_type == "number" else "TEXT"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f'ALTER TABLE queries ADD COLUMN {field} {col_type_sql}')
        await db.commit()

    return new_col


@app.delete("/api/custom-columns/{field}")
async def delete_custom_column(field: str):
    cols = load_custom_columns()
    cols = [c for c in cols if c["field"] != field]
    save_custom_columns(cols)
    return {"ok": True}


# ============================================================
# Test Automation
# ============================================================

import subprocess
import threading
import signal

PROGRESS_FILE = os.path.join(os.path.dirname(APP_DIR), "search_progress.txt")
test_state = {"running": False, "progress": [], "total": 0, "current": 0, "status": "idle", "pid": None}


def read_breakpoint():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            content = f.read().strip()
            return int(content) if content else 0
    return 0


def write_breakpoint(val):
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(val))


@app.get("/api/test/status")
async def get_test_status():
    return {
        **test_state,
        "breakpoint": read_breakpoint()
    }


@app.post("/api/test/start")
async def start_test(body: dict = {}):
    if test_state["running"]:
        raise HTTPException(400, "测试正在运行中")

    # Allow overriding breakpoint
    bp = body.get("breakpoint")
    if bp is not None:
        write_breakpoint(int(bp))

    test_set_id = body.get("test_set_id", 1)

    test_state["running"] = True
    test_state["progress"] = []
    test_state["status"] = "starting"
    test_state["current"] = 0
    test_state["total"] = 0

    def run_test():
        try:
            script_path = os.path.join(os.path.dirname(APP_DIR), "search_auto_test.py")
            env = os.environ.copy()
            env["TEST_SET_ID"] = str(test_set_id)
            proc = subprocess.Popen(
                ["python", "-u", script_path],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                cwd=os.path.dirname(APP_DIR), text=True, encoding="utf-8", errors="replace",
                env=env
            )
            test_state["pid"] = proc.pid
            for line in proc.stdout:
                line = line.strip()
                if line:
                    test_state["progress"].append(line)
                    # Keep last 200 lines
                    if len(test_state["progress"]) > 200:
                        test_state["progress"] = test_state["progress"][-200:]
                    if line.startswith("[") and "/" in line.split("]")[0]:
                        try:
                            parts = line.split("]")[0].strip("[")
                            cur, tot = parts.split("/")
                            test_state["current"] = int(cur)
                            test_state["total"] = int(tot)
                        except:
                            pass
                    test_state["status"] = "running"
            proc.wait()
            test_state["status"] = "done" if proc.returncode == 0 else "error"
        except Exception as e:
            test_state["progress"].append(f"启动失败: {e}")
            test_state["status"] = "error"
        finally:
            test_state["running"] = False
            test_state["pid"] = None

    threading.Thread(target=run_test, daemon=True).start()
    return {"ok": True, "message": "测试已启动"}


@app.post("/api/test/stop")
async def stop_test():
    if test_state["pid"]:
        try:
            os.kill(test_state["pid"], signal.SIGTERM)
        except:
            pass
    test_state["running"] = False
    test_state["status"] = "stopped"
    test_state["pid"] = None
    return {"ok": True}


@app.post("/api/test/breakpoint")
async def set_breakpoint(body: dict):
    val = body.get("value", 0)
    write_breakpoint(int(val))
    return {"ok": True, "breakpoint": int(val)}


# ============================================================
# Frontend
# ============================================================

@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = os.path.join(STATIC_DIR, "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()


@app.get("/analysis", response_class=HTMLResponse)
async def analysis_page():
    html_path = os.path.join(STATIC_DIR, "analysis.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
