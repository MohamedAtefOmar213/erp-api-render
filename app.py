import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Header, HTTPException, Query
from db import query_all, query_one, exec_write

API_TOKEN = os.getenv("API_TOKEN", "change-this-token")

app = FastAPI(title="ERP API (Render)", version="1.0.0")

def require_token(authorization: Optional[str]):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    token = authorization.split(" ", 1)[1]
    if token != API_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

# ---------------- STORES ----------------
@app.get("/api/v1/stores")
def stores_list(
    authorization: Optional[str] = Header(None),
    q: Optional[str] = Query(None),
    isActive: Optional[bool] = Query(None),
    page: int = Query(1, ge=1),
    pageSize: int = Query(50, ge=1, le=500)
):
    require_token(authorization)
    offset = (page - 1) * pageSize
    where = []
    params: List[Any] = []
    if q:
        where.append("(name LIKE @P1 OR store_code LIKE @P2 OR location LIKE @P3 OR city LIKE @P4)")
        like = f"%{q}%"; params += [like, like, like, like]
    if isActive is not None:
        where.append("is_active = @P?")
        params.append(1 if isActive else 0)
    # python-tds لا يدعم positional params بـ "?"، سنُبدّل إلى named style مبسط:
    # سنبني يدويًا مع عدّاد
    built_params = []
    built_where = []
    n = 1
    for clause in where:
        # استبدال @P? بـ @P{n}
        while "@P?" in clause:
            clause = clause.replace("@P?", f"@P{n}", 1)
            n += 1
        built_where.append(clause)
    # أعد بناء params بنفس الترتيب
    # أسهل: نعيد التجهيز
    built_params = []
    if q:
        like = f"%{q}%"
        built_params += [("P1", like), ("P2", like), ("P3", like), ("P4", like)]
    if isActive is not None:
        built_params += [(f"P{n}", 1 if isActive else 0)]
    where_clause = "WHERE " + " AND ".join(built_where) if built_where else ""

    sql = f"""
        SELECT id, store_code, name, location, city, region, country, manager_name,
               phone, email, capacity_units, is_active, notes
          FROM stores
         {where_clause}
         ORDER BY id DESC
         OFFSET {offset} ROWS FETCH NEXT {pageSize} ROWS ONLY
    """
    rows = query_all(sql, built_params)
    out = []
    for r in rows:
        out.append({
            "id": r[0], "store_code": r[1], "name": r[2], "location": r[3],
            "city": r[4], "region": r[5], "country": r[6], "manager_name": r[7],
            "phone": r[8], "email": r[9], "capacity_units": r[10],
            "is_active": bool(r[11]), "notes": r[12]
        })
    return out

@app.post("/api/v1/stores")
def stores_create(body: Dict[str, Any], authorization: Optional[str] = Header(None)):
    require_token(authorization)
    fields = ["store_code","name","location","city","region","country","manager_name",
              "phone","email","capacity_units","is_active","notes"]
    values = [body.get(f) for f in fields]
    placeholders = ", ".join([f"@{f}" for f in fields])
    params = [(f, v) for f, v in zip(fields, values)]
    sql = f"""
        INSERT INTO stores ({", ".join(fields)}, created_at)
        VALUES ({placeholders}, SYSUTCDATETIME());
        SELECT SCOPE_IDENTITY();
    """
    new_id = int(query_one(sql, params)[0])
    row = query_one("""
        SELECT id, store_code, name, location, city, region, country, manager_name,
               phone, email, capacity_units, is_active, notes
          FROM stores WHERE id=@id
    """, [("id", new_id)])
    return {
        "id": row[0], "store_code": row[1], "name": row[2], "location": row[3],
        "city": row[4], "region": row[5], "country": row[6], "manager_name": row[7],
        "phone": row[8], "email": row[9], "capacity_units": row[10],
        "is_active": bool(row[11]), "notes": row[12]
    }

@app.put("/api/v1/stores/{store_id}")
def stores_update(store_id: int, body: Dict[str, Any], authorization: Optional[str] = Header(None)):
    require_token(authorization)
    params = [
        ("store_code", body.get("store_code")),
        ("name", body.get("name")),
        ("location", body.get("location")),
        ("city", body.get("city")),
        ("region", body.get("region")),
        ("country", body.get("country")),
        ("manager_name", body.get("manager_name")),
        ("phone", body.get("phone")),
        ("email", body.get("email")),
        ("capacity_units", body.get("capacity_units")),
        ("is_active", 1 if body.get("is_active", True) else 0),
        ("notes", body.get("notes")),
        ("id", store_id)
    ]
    exec_write("""
        UPDATE stores
           SET store_code=@store_code, name=@name, location=@location, city=@city,
               region=@region, country=@country, manager_name=@manager_name, phone=@phone,
               email=@email, capacity_units=@capacity_units, is_active=@is_active,
               notes=@notes, updated_at=SYSUTCDATETIME()
         WHERE id=@id
    """, params)
    row = query_one("""
        SELECT id, store_code, name, location, city, region, country, manager_name,
               phone, email, capacity_units, is_active, notes
          FROM stores WHERE id=@id
    """, [("id", store_id)])
    if not row:
        raise HTTPException(status_code=404, detail="Store not found")
    return {
        "id": row[0], "store_code": row[1], "name": row[2], "location": row[3],
        "city": row[4], "region": row[5], "country": row[6], "manager_name": row[7],
        "phone": row[8], "email": row[9], "capacity_units": row[10],
        "is_active": bool(row[11]), "notes": row[12]
    }

@app.delete("/api/v1/stores/{store_id}", status_code=204)
def stores_delete(store_id: int, authorization: Optional[str] = Header(None)):
    require_token(authorization)
    exec_write("DELETE FROM stores WHERE id=@id", [("id", store_id)])
    return

# --------------- REPORTS (مختصرة تكفي التشغيل) ---------------
@app.get("/api/v1/reports/valuation")
def report_valuation(authorization: Optional[str] = Header(None)):
    require_token(authorization)
    sql = """
        SELECT p.barcode, p.name, p.price, p.cost_price,
               ISNULL(SUM(s.qty), 0) AS total_qty,
               (p.cost_price * ISNULL(SUM(s.qty), 0)) AS value
          FROM products p
          LEFT JOIN stock s ON p.barcode = s.barcode
         GROUP BY p.barcode, p.name, p.price, p.cost_price
         ORDER BY p.name
    """
    rows = query_all(sql)
    return [
        {
            "barcode": r[0], "name": r[1], "price": float(r[2] or 0),
            "cost_price": float(r[3] or 0), "total_qty": float(r[4] or 0),
            "value": float(r[5] or 0)
        } for r in rows
    ]