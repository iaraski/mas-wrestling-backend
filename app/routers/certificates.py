from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Header
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Optional, Any
from uuid import UUID
from app.core.db import get_db, tables
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, insert, update, delete
from app.core.minio import put_object
from app.core.local_auth import verify_access_token
from app.core.roles import get_role_codes
import uuid
import io
import os
import httpx
from PIL import Image, ImageDraw, ImageFont

router = APIRouter(prefix="/certificates", tags=["certificates"])

async def verify_admin(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid token")
    token = authorization.split(" ")[1]
    payload = verify_access_token(token)
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")
        
    roles = await get_role_codes(user_id)
    if "admin" not in roles and "founder" not in roles:
        raise HTTPException(status_code=403, detail="Forbidden")
    return user_id

class CertificateElement(BaseModel):
    id: str
    x: float
    y: float
    text: str
    fontSize: int
    color: str
    align: str
    fontWeight: str

class CertificateTemplateCreate(BaseModel):
    name: str
    format: str
    background_image_url: Optional[str] = None
    elements: List[CertificateElement] = []

class CertificateTemplateUpdate(BaseModel):
    name: Optional[str] = None
    format: Optional[str] = None
    background_image_url: Optional[str] = None
    elements: Optional[List[CertificateElement]] = None

@router.get("/")
async def get_templates(db: AsyncSession = Depends(get_db)):
    t = tables.get("certificate_templates")
    if t is None:
        return []
    result = await db.execute(select(t))
    rows = result.mappings().all()
    return [dict(r) for r in rows]

@router.post("/")
async def create_template(data: CertificateTemplateCreate, db: AsyncSession = Depends(get_db), admin=Depends(verify_admin)):
    t = tables.get("certificate_templates")
    if t is None:
        raise HTTPException(400, "Table not ready")
    
    elements_dict = [e.model_dump() for e in data.elements]
    
    stmt = insert(t).values(
        name=data.name,
        format=data.format,
        background_image_url=data.background_image_url,
        elements=elements_dict
    ).returning(t)
    
    result = await db.execute(stmt)
    await db.commit()
    return dict(result.mappings().first())

@router.put("/{template_id}")
async def update_template(template_id: UUID, data: CertificateTemplateUpdate, db: AsyncSession = Depends(get_db), admin=Depends(verify_admin)):
    t = tables.get("certificate_templates")
    if t is None:
        raise HTTPException(400, "Table not ready")
    
    update_data = {}
    if data.name is not None:
        update_data["name"] = data.name
    if data.format is not None:
        update_data["format"] = data.format
    if data.background_image_url is not None:
        update_data["background_image_url"] = data.background_image_url
    if data.elements is not None:
        update_data["elements"] = [e.model_dump() for e in data.elements]
        
    if not update_data:
        return {"status": "ok"}
        
    stmt = update(t).where(t.c.id == template_id).values(**update_data).returning(t)
    result = await db.execute(stmt)
    await db.commit()
    row = result.mappings().first()
    if not row:
        raise HTTPException(404, "Not found")
    return dict(row)

@router.delete("/{template_id}")
async def delete_template(template_id: UUID, db: AsyncSession = Depends(get_db), admin=Depends(verify_admin)):
    t = tables.get("certificate_templates")
    if t is None:
        raise HTTPException(400, "Table not ready")
    
    stmt = delete(t).where(t.c.id == template_id)
    await db.execute(stmt)
    await db.commit()
    return {"status": "ok"}

@router.post("/preview")
async def preview_template(data: CertificateTemplateCreate, admin=Depends(verify_admin)):
    is_portrait = data.format == 'A4_PORTRAIT'
    virtual_width = 794 if is_portrait else 1123
    virtual_height = 1123 if is_portrait else 794

    if not data.background_image_url:
        img = Image.new("RGB", (virtual_width, virtual_height), "white")
        bg_bytes = None
    else:
        try:
            async with httpx.AsyncClient() as client:
                bg_resp = await client.get(data.background_image_url)
                bg_resp.raise_for_status()
                bg_bytes = bg_resp.content
            img = Image.open(io.BytesIO(bg_bytes)).convert("RGB")
        except Exception as e:
            raise HTTPException(500, f"Не удалось загрузить фон: {e}")
        
    font_regular_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Roboto-Regular.ttf")
    font_bold_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Roboto-Bold.ttf")
    
    draw = ImageDraw.Draw(img)
    
    img_width, img_height = img.size
    
    # Фронтенд использует виртуальные координаты (A4_WIDTH=794, A4_HEIGHT=1123)
    # Нам нужно масштабировать координаты и размер шрифта под реальный размер картинки
    is_portrait = data.format == 'A4_PORTRAIT'
    virtual_width = 794 if is_portrait else 1123
    virtual_height = 1123 if is_portrait else 794
    
    scale_x = img_width / virtual_width
    scale_y = img_height / virtual_height
    
    variables = {
        "{{athlete_name}}": "Иванов Иван",
        "{{place}}": "1",
        "{{category}}": "Юноши 14-15 лет, До 70 кг",
        "{{weight_category}}": "До 70",
        "{{age_category}}": "Юноши 14-15 лет",
        "{{competition_name}}": "Тестовый турнир",
        "{{date}}": "01.01.2026",
        "{{team_name}}": "Сборная Москвы"
    }
    
    for el in data.elements:
        text = el.text
        for k, v in variables.items():
            text = text.replace(k, str(v))
            
        # Масштабируем координаты и размер шрифта
        x = el.x * scale_x
        y = el.y * scale_y
        
        # В браузере (cqi) шрифт задан в пикселях. В Pillow truetype ожидает размер в точках (pt) 
        # При 96 DPI: 1px = 0.75pt (или 1pt = 1.33px). 
        # Pillow по умолчанию считает DPI=72 (1pt=1px).
        # Поэтому чтобы получить точный визуальный размер, нужно умножить на коэффициент.
        font_size = int((el.fontSize * scale_x) * 1.33) 
        
        color = el.color
        align = el.align
        font_weight = el.fontWeight
        
        font_path = font_bold_path if font_weight == "bold" else font_regular_path
        try:
            font = ImageFont.truetype(font_path, font_size)
        except:
            font = ImageFont.load_default()
            
        try:
            # Pillow считает bbox от верхнего левого угла отрисовки текста
            bbox = draw.multiline_textbbox((0, 0), text, font=font, align=align)
            w = bbox[2] - bbox[0]
            # В CSS высота текста считается по line-height, в Pillow это bbox[3]-bbox[1].
            # Pillow часто дает чуть меньший bbox для текста без выносных элементов.
            # Будем опираться на bbox для Y.
            h = bbox[3] - bbox[1]
            
            # В CSS мы используем translate(..., -50%), поэтому по Y смещаем на половину высоты
            draw_y = y - h / 2
            
            draw_x = x
            if align == "center":
                draw_x = x - w / 2
            elif align == "right":
                draw_x = x - w
                
            # Отрисовываем
            draw.multiline_text((draw_x, draw_y), text, fill=color, font=font, align=align)
        except Exception as e:
            # Fallback
            draw.text((x, y), text, fill=color, font=font)
            
    pdf_bytes = io.BytesIO()
    img.save(pdf_bytes, "PDF", resolution=100.0)
    pdf_bytes.seek(0)
    
    return StreamingResponse(
        pdf_bytes, 
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=preview.pdf"}
    )

@router.post("/upload-background")
async def upload_background(file: UploadFile = File(...), admin=Depends(verify_admin)):
    file_bytes = await file.read()
    ext = file.filename.split('.')[-1].lower() if '.' in file.filename else 'png'

    filename = f"certificates/{uuid.uuid4()}.{ext}"
    url = await put_object(filename, file_bytes, content_type=file.content_type or 'image/png')
    if not url:
        raise HTTPException(500, "Failed to upload image")
    return {"url": url}
