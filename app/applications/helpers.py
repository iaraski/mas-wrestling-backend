from datetime import datetime, timedelta, timezone


_MSK_TZ = timezone(timedelta(hours=3))


def is_telegram_file_id(value: str) -> bool:
    normalized = (value or "").strip()
    if not normalized:
        return False
    if normalized.lower().startswith("http"):
        return False
    if normalized.startswith("documents/") or normalized.startswith("/"):
        return False
    return True


def is_minio_key(value: str) -> bool:
    normalized = (value or "").strip()
    if not normalized:
        return False
    return normalized.startswith("documents/") or normalized.startswith("/")


def parse_date(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except Exception:
        return None


def age_at_date(birth_date: str | None, at_date: str | None) -> int | None:
    if not birth_date:
        return None
    try:
        birth = datetime.fromisoformat(str(birth_date).replace("Z", "+00:00")).date()
        if at_date:
            at = datetime.fromisoformat(str(at_date).replace("Z", "+00:00")).date()
        else:
            at = datetime.now(_MSK_TZ).date()
        return int(at.year) - int(birth.year)
    except Exception:
        return None


def normalize_gender(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    if normalized in {"male", "m", "м"}:
        return "male"
    if normalized in {"female", "f", "ж"}:
        return "female"
    return normalized


def category_group(gender: str | None, age_min: int | None, age_max: int | None) -> str:
    normalized = normalize_gender(gender)
    is_male = normalized == "male"
    is_female = normalized == "female"
    if age_min == 18 and age_max == 21:
        return "Юниоры" if is_male else "Юниорки" if is_female else "Юниоры"
    if age_max is not None and age_max < 18:
        return "Юноши" if is_male else "Девушки" if is_female else "Юноши"
    return "Мужчины" if is_male else "Женщины" if is_female else "Мужчины"


def weight_label(weight_min: float | int | None, weight_max: float | int | None) -> str:
    try:
        if weight_max is None or float(weight_max) >= 999:
            if not weight_min:
                return "абсолютная"
            return f"{int(float(weight_min))}+ кг"
        return f"до {weight_max} кг"
    except Exception:
        return "—"


def birth_years_label(age_min: int | None, age_max: int | None, at_date: str | None) -> str | None:
    if age_min is None or age_max is None:
        return None
    try:
        year = datetime.fromisoformat(str(at_date).replace("Z", "+00:00")).year if at_date else datetime.now(_MSK_TZ).year
    except Exception:
        year = datetime.now(_MSK_TZ).year
    return f"{year - age_max}-{year - age_min} г.р."


def format_category_label(category: dict, at_date: str | None) -> str:
    try:
        group = category_group(category.get("gender"), category.get("age_min"), category.get("age_max"))
        years = birth_years_label(category.get("age_min"), category.get("age_max"), at_date)
        weight = weight_label(category.get("weight_min"), category.get("weight_max"))
        return f"{group} {years}, {weight}" if years else f"{group}, {weight}"
    except Exception:
        return "Неизвестная категория"


def normalize_passport_photo_url(photo_url: str | None) -> str | None:
    if not isinstance(photo_url, str):
        return photo_url
    if is_telegram_file_id(photo_url):
        return f"/applications/photo/{photo_url}"
    if is_minio_key(photo_url):
        return f"/applications/photo-key/{photo_url.lstrip('/')}"
    return photo_url
