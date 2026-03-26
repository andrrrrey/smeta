import asyncio
import hashlib
import logging
import os
import tempfile
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Optional

from fastapi import BackgroundTasks, Cookie, FastAPI, HTTPException, Response, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from db import (
    Calculation,
    CalculationItem,
    StopWord,
    User,
    async_session_factory,
    init_db,
)
from services import ai_service_instance, price_logic_instance
from utils import (
    create_calculation_excel,
    extract_specification_tables_streaming,
    parse_excel_for_update,
)

logger = logging.getLogger(__name__)

app = FastAPI(title="СМЕТА — Веб-интерфейс")

# In-memory job store: job_id -> progress/status dict
job_store: dict = {}

TEMPLATES_DIR = Path(__file__).parent / "templates"


# ─── Session helpers ────────────────────────────────────────────────────────

def session_to_user_id(session_id: str) -> int:
    """Convert a session UUID string to a stable BigInteger user_id.
    Uses the range 10^14–2×10^14 so it never collides with Telegram IDs (~10^10).
    """
    h = int(hashlib.sha256(session_id.encode()).hexdigest(), 16)
    return h % (10 ** 14) + 10 ** 14


async def ensure_web_user(session_id: str) -> int:
    user_id = session_to_user_id(session_id)
    async with async_session_factory() as session:
        user = await session.get(User, user_id)
        if not user:
            session.add(User(user_id=user_id, first_name="web", username="web_user"))
            await session.commit()
    return user_id


def get_or_create_session(session_id: Optional[str], response: Response) -> str:
    if not session_id:
        session_id = str(uuid.uuid4())
        response.set_cookie(
            key="smeta_session",
            value=session_id,
            max_age=60 * 60 * 24 * 365,  # 1 year
            httponly=True,
            samesite="lax",
        )
    return session_id


async def verify_calc_access(calc_id: int, session_id: str) -> Calculation:
    """Load a Calculation and verify it belongs to this session's user."""
    user_id = session_to_user_id(session_id)
    async with async_session_factory() as session:
        calc = await session.get(
            Calculation, calc_id, options=[selectinload(Calculation.items)]
        )
    if not calc:
        raise HTTPException(status_code=404, detail="Расчет не найден")
    if calc.user_id != user_id:
        raise HTTPException(status_code=403, detail="Нет доступа к этому расчету")
    return calc


# ─── Background calculation task ────────────────────────────────────────────

async def run_calculation_job(job_id: str, user_id: int, pdf_path: str, filename: str):
    job_store[job_id].update({"status": "processing"})
    calc_id = None
    try:
        async with async_session_factory() as session:
            stream = extract_specification_tables_streaming(pdf_path, page_indices=None)

            async def progress_cb(pages_done, pages_total, priced, total):
                job_store[job_id].update(
                    {
                        "pages_done": pages_done,
                        "pages_total": pages_total,
                        "priced": priced,
                        "total": total,
                    }
                )

            calc = await price_logic_instance.process_specification_streaming(
                session, user_id, filename, stream, progress_cb
            )
            calc_id = calc.id

        job_store[job_id].update({"status": "done", "calc_id": calc_id})
    except ValueError as e:
        err = str(e)
        if "no_items_found" in err:
            msg = "В документе не найдено позиций спецификации. Проверьте файл."
        elif "quota" in err.lower() or "429" in err:
            msg = "Исчерпан лимит API. Попробуйте позже."
        else:
            msg = f"Ошибка обработки: {err}"
        job_store[job_id].update({"status": "error", "error_msg": msg})
    except Exception as e:
        logger.exception("Calculation job %s failed", job_id)
        job_store[job_id].update({"status": "error", "error_msg": str(e)})
    finally:
        try:
            os.unlink(pdf_path)
        except OSError:
            pass


# ─── Routes ─────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(response: Response, smeta_session: Optional[str] = Cookie(None)):
    session_id = get_or_create_session(smeta_session, response)
    html_path = TEMPLATES_DIR / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.post("/api/upload")
async def upload_pdf(
    file: UploadFile,
    background_tasks: BackgroundTasks,
    response: Response,
    smeta_session: Optional[str] = Cookie(None),
):
    session_id = get_or_create_session(smeta_session, response)

    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Можно загружать только PDF файлы")

    content = await file.read()
    if len(content) > 50 * 1024 * 1024:  # 50 MB limit
        raise HTTPException(status_code=400, detail="Файл слишком большой (макс. 50 МБ)")
    if len(content) < 100:
        raise HTTPException(status_code=400, detail="Файл пустой или повреждён")

    user_id = await ensure_web_user(session_id)

    # Save to temp file
    suffix = ".pdf"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, dir="/tmp")
    tmp.write(content)
    tmp.close()

    job_id = str(uuid.uuid4())
    job_store[job_id] = {
        "status": "processing",
        "calc_id": None,
        "pages_done": 0,
        "pages_total": 0,
        "priced": 0,
        "total": 0,
        "error_msg": None,
    }

    background_tasks.add_task(
        run_calculation_job, job_id, user_id, tmp.name, file.filename
    )

    return {"job_id": job_id}


@app.get("/api/job/{job_id}")
async def get_job_status(
    job_id: str,
    response: Response,
    smeta_session: Optional[str] = Cookie(None),
):
    get_or_create_session(smeta_session, response)
    info = job_store.get(job_id)
    if info is None:
        raise HTTPException(status_code=404, detail="Задача не найдена")
    return info


@app.get("/api/calculation/{calc_id}")
async def get_calculation(
    calc_id: int,
    response: Response,
    smeta_session: Optional[str] = Cookie(None),
):
    session_id = get_or_create_session(smeta_session, response)
    calc = await verify_calc_access(calc_id, session_id)

    items = []
    for item in calc.items:
        items.append(
            {
                "id": item.id,
                "name": item.name,
                "code": item.code or "",
                "quantity": float(item.quantity),
                "unit": item.unit,
                "cost_per_unit": float(item.cost_per_unit),
                "cost_material_per_unit": float(item.cost_material_per_unit),
                "total_cost": float(item.total_cost),
                "source": item.source,
            }
        )

    return {
        "id": calc.id,
        "status": calc.status,
        "total_cost": float(calc.total_cost),
        "pdf_filename": calc.pdf_filename or "",
        "created_at": calc.created_at.isoformat() if calc.created_at else None,
        "items": items,
    }


@app.get("/api/download/{calc_id}")
async def download_excel(
    calc_id: int,
    response: Response,
    smeta_session: Optional[str] = Cookie(None),
):
    session_id = get_or_create_session(smeta_session, response)
    calc = await verify_calc_access(calc_id, session_id)

    # Load consumable words for Excel colouring
    async with async_session_factory() as session:
        result = await session.execute(select(StopWord))
        stop_words = [row.word for row in result.scalars().all()]

    buf = create_calculation_excel(calc.items, float(calc.total_cost), stop_words)
    buf.seek(0)

    filename = f"calculation_{calc_id}.xlsx"
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }
    return StreamingResponse(buf, headers=headers)


@app.post("/api/update/{calc_id}")
async def update_calculation(
    calc_id: int,
    file: UploadFile,
    response: Response,
    smeta_session: Optional[str] = Cookie(None),
):
    session_id = get_or_create_session(smeta_session, response)
    calc = await verify_calc_access(calc_id, session_id)

    if calc.status == "approved":
        raise HTTPException(status_code=400, detail="Нельзя редактировать утвержденный расчет")

    if not file.filename or not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Можно загружать только Excel файлы (.xlsx, .xls)")

    content = await file.read()
    ext = ".xlsx" if file.filename.lower().endswith(".xlsx") else ".xls"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=ext, dir="/tmp")
    tmp.write(content)
    tmp.close()

    try:
        updates = parse_excel_for_update(tmp.name)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Ошибка чтения Excel: {e}")
    finally:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass

    if not updates:
        raise HTTPException(
            status_code=400,
            detail="Не удалось найти данные. Проверьте заголовки (Наименование, Кол-во).",
        )

    def to_decimal(val) -> Decimal:
        if val is None:
            return Decimal("0.0")
        s = str(val).strip().lower()
        if s in ("nan", "none", ""):
            return Decimal("0.0")
        try:
            return Decimal(s)
        except Exception:
            return Decimal("0.0")

    async with async_session_factory() as session:
        calc_db = await session.get(
            Calculation, calc_id, options=[selectinload(Calculation.items)]
        )

        updates_by_pos = {u["position"]: u for u in updates if u.get("position")}
        updates_by_name = {str(u["name"]).strip().lower(): u for u in updates}

        new_total = Decimal("0.0")
        updated_count = 0
        items_to_delete = []

        for i, item in enumerate(calc_db.items, 1):
            update_data = updates_by_pos.get(i)
            if not update_data:
                update_data = updates_by_name.get(str(item.name).strip().lower())

            if update_data:
                new_full_name = str(update_data.get("name", item.name)).strip()
                new_qty = to_decimal(update_data.get("quantity"))
                new_price_work = to_decimal(update_data.get("cost_per_unit"))
                new_price_mat = to_decimal(update_data.get("cost_material_per_unit", 0))
                new_unit = update_data.get("unit")

                if item.code and item.code.lower() not in new_full_name.lower():
                    item.code = ""

                item.name = new_full_name
                item.quantity = new_qty
                item.cost_per_unit = new_price_work
                item.cost_material_per_unit = new_price_mat
                if new_unit and new_unit != "-":
                    item.unit = new_unit
                item.source = "manual"
                item.total_cost = item.quantity * (item.cost_per_unit + item.cost_material_per_unit)
                updated_count += 1
            else:
                items_to_delete.append(item)

            if item not in items_to_delete:
                new_total += item.total_cost

        for item in items_to_delete:
            await session.delete(item)

        calc_db.total_cost = new_total
        await session.commit()

    return {
        "updated_count": updated_count,
        "deleted_count": len(items_to_delete),
        "new_total": float(new_total),
    }
