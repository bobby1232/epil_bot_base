from __future__ import annotations

from datetime import datetime, date, timedelta
import pytz

from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import User, Service, Appointment, AppointmentStatus
from app.config import Config


# =========================
# SETTINGS
# =========================

async def get_settings(session: AsyncSession, timezone: str):
    """
    Загружает настройки из БД (таблица settings).
    """
    from app.models import Setting

    res = await session.execute(select(Setting))
    rows = res.scalars().all()

    data = {r.key: r.value for r in rows}

    class Settings:
        tz = pytz.timezone(timezone)
        hold_ttl_min = int(data.get("hold_ttl_min", 15))
        cancel_limit_hours = int(data.get("cancel_limit_hours", 24))

    return Settings()


# =========================
# USERS
# =========================

async def upsert_user(
    session: AsyncSession,
    tg_id: int,
    username: str | None,
    full_name: str | None,
) -> User:
    res = await session.execute(select(User).where(User.tg_id == tg_id))
    user = res.scalar_one_or_none()

    if user:
        user.username = username
        user.full_name = full_name
    else:
        user = User(
            tg_id=tg_id,
            username=username,
            full_name=full_name,
        )
        session.add(user)

    await session.flush()
    return user


async def set_user_phone(session: AsyncSession, tg_id: int, phone: str):
    res = await session.execute(select(User).where(User.tg_id == tg_id))
    user = res.scalar_one()
    user.phone = phone
    await session.flush()


# =========================
# SERVICES
# =========================

async def list_active_services(session: AsyncSession) -> list[Service]:
    res = await session.execute(
        select(Service).where(Service.is_active.is_(True))
    )
    return res.scalars().all()


# =========================
# CLIENT APPOINTMENTS
# =========================

async def get_user_appointments(
    session: AsyncSession,
    tg_id: int,
    limit: int = 10,
) -> list[Appointment]:
    """
    МОИ ЗАПИСИ:
    - только будущие
    - только Booked
    - Hold ТОЛЬКО если ещё не истёк
    - НИКАКИХ Rejected / Canceled
    """
    res = await session.execute(select(User).where(User.tg_id == tg_id))
    user = res.scalar_one()

    now_utc = datetime.now(tz=pytz.UTC)

    res = await session.execute(
        select(Appointment)
        .options(
            selectinload(Appointment.service),
            selectinload(Appointment.client),
        )
        .where(
            and_(
                Appointment.client_user_id == user.id,
                Appointment.start_dt >= now_utc,
                or_(
                    Appointment.status == AppointmentStatus.Booked,
                    and_(
                        Appointment.status == AppointmentStatus.Hold,
                        Appointment.hold_expires_at.is_not(None),
                        Appointment.hold_expires_at > now_utc,
                    ),
                ),
            )
        )
        .order_by(Appointment.start_dt.asc())
        .limit(limit)
    )

    return res.scalars().all()


async def get_user_appointments_history(
    session: AsyncSession,
    tg_id: int,
    limit: int = 10,
) -> list[Appointment]:
    """
    ИСТОРИЯ:
    - прошедшие записи
    - любые статусы КРОМЕ Hold
    """
    res = await session.execute(select(User).where(User.tg_id == tg_id))
    user = res.scalar_one()

    now_utc = datetime.now(tz=pytz.UTC)

    res = await session.execute(
        select(Appointment)
        .options(
            selectinload(Appointment.service),
            selectinload(Appointment.client),
        )
        .where(
            and_(
                Appointment.client_user_id == user.id,
                Appointment.start_dt < now_utc,
                Appointment.status != AppointmentStatus.Hold,
            )
        )
        .order_by(Appointment.start_dt.desc())
        .limit(limit)
    )

    return res.scalars().all()


# =========================
# CREATE HOLD
# =========================

async def create_hold_appointment(
    session: AsyncSession,
    settings,
    client: User,
    service: Service,
    start_local: datetime,
    comment: str | None,
) -> Appointment:
    """
    Создаёт HOLD-заявку
    """
    start_utc = start_local.astimezone(pytz.UTC)
    now_utc = datetime.now(tz=pytz.UTC)

    hold_expires_at = now_utc + timedelta(minutes=settings.hold_ttl_min)

    appt = Appointment(
        client_user_id=client.id,
        service_id=service.id,
        start_dt=start_utc,
        status=AppointmentStatus.Hold,
        hold_expires_at=hold_expires_at,
        client_comment=comment,
        created_at=now_utc,
        updated_at=now_utc,
    )

    session.add(appt)
    await session.flush()
    return appt


# =========================
# ADMIN
# =========================

async def get_appointment(session: AsyncSession, appt_id: int) -> Appointment:
    res = await session.execute(
        select(Appointment)
        .options(
            selectinload(Appointment.service),
            selectinload(Appointment.client),
        )
        .where(Appointment.id == appt_id)
    )
    return res.scalar_one()


async def admin_confirm(session: AsyncSession, appt: Appointment):
    appt.status = AppointmentStatus.Booked
    appt.updated_at = datetime.now(tz=pytz.UTC)


async def admin_reject(session: AsyncSession, appt: Appointment, reason: str | None = None):
    appt.status = AppointmentStatus.Rejected
    appt.updated_at = datetime.now(tz=pytz.UTC)


async def cancel_by_client(session: AsyncSession, settings, appt: Appointment) -> bool:
    now_utc = datetime.now(tz=pytz.UTC)
    limit = appt.start_dt - timedelta(hours=settings.cancel_limit_hours)
    if now_utc > limit:
        return False

    appt.status = AppointmentStatus.Canceled
    appt.updated_at = now_utc
    return True


# =========================
# ADMIN LISTS
# =========================

async def admin_list_holds(session: AsyncSession):
    res = await session.execute(
        select(Appointment)
        .options(
            selectinload(Appointment.service),
            selectinload(Appointment.client),
        )
        .where(Appointment.status == AppointmentStatus.Hold)
        .order_by(Appointment.start_dt.asc())
    )
    return res.scalars().all()


async def admin_list_appointments_for_day(session: AsyncSession, tz, day: date):
    start = datetime.combine(day, datetime.min.time()).astimezone(tz).astimezone(pytz.UTC)
    end = start + timedelta(days=1)

    res = await session.execute(
        select(Appointment)
        .options(
            selectinload(Appointment.service),
            selectinload(Appointment.client),
        )
        .where(
            and_(
                Appointment.start_dt >= start,
                Appointment.start_dt < end,
            )
        )
        .order_by(Appointment.start_dt.asc())
    )
    return res.scalars().all()
