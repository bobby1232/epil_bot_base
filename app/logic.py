from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, time, date
import hashlib
import pytz
from sqlalchemy.orm import selectinload


from sqlalchemy import select, text, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import User, Service, Setting, Appointment, AppointmentStatus, BlockedInterval

@dataclass(frozen=True)
class SettingsView:
    slot_step_min: int
    buffer_min: int
    min_lead_time_min: int
    booking_horizon_days: int
    hold_ttl_min: int
    cancel_limit_hours: int
    work_start: time
    work_end: time
    work_days: set[int]
    tz: pytz.BaseTzInfo

def _parse_hhmm(s: str) -> time:
    hh, mm = s.split(":")
    return time(int(hh), int(mm))

async def seed_defaults_if_needed(session: AsyncSession, *, defaults: dict[str, str]) -> None:
    existing = await session.execute(select(Setting.key).limit(1))
    if existing.first():
        return
    for k, v in defaults.items():
        session.add(Setting(key=k, value=str(v)))

async def get_settings(session: AsyncSession, tz_name: str) -> SettingsView:
    tz = pytz.timezone(tz_name)
    rows = (await session.execute(select(Setting))).scalars().all()
    m = {r.key: r.value for r in rows}

    return SettingsView(
        slot_step_min=int(m["slot_step_min"]),
        buffer_min=int(m["buffer_min"]),
        min_lead_time_min=int(m["min_lead_time_min"]),
        booking_horizon_days=int(m["booking_horizon_days"]),
        hold_ttl_min=int(m["hold_ttl_min"]),
        cancel_limit_hours=int(m["cancel_limit_hours"]),
        work_start=_parse_hhmm(m["work_start"]),
        work_end=_parse_hhmm(m["work_end"]),
        work_days=set(int(x) for x in m["work_days"].split(",") if x.strip() != ""),
        tz=tz,
    )

async def upsert_user(session: AsyncSession, tg_id: int, username: str | None, full_name: str | None) -> User:
    q = await session.execute(select(User).where(User.tg_id == tg_id))
    u = q.scalar_one_or_none()
    now = datetime.now(tz=pytz.UTC)
    if u:
        u.username = username
        u.full_name = full_name
        return u
    u = User(tg_id=tg_id, username=username, full_name=full_name, phone=None, created_at=now)
    session.add(u)
    await session.flush()
    return u

async def set_user_phone(session: AsyncSession, tg_id: int, phone: str) -> None:
    q = await session.execute(select(User).where(User.tg_id == tg_id))
    u = q.scalar_one()
    u.phone = phone

async def list_active_services(session: AsyncSession) -> list[Service]:
    return (await session.execute(
        select(Service).where(Service.is_active == True).order_by(Service.sort_order, Service.id)
    )).scalars().all()

async def ensure_default_services(session: AsyncSession) -> None:
    q = await session.execute(select(Service.id).limit(1))
    if q.first():
        return
    session.add_all([
        Service(name="Подмышки", price=25, duration_min=20, buffer_min=0, is_active=True, sort_order=10),
        Service(name="Голени", price=35, duration_min=30, buffer_min=0, is_active=True, sort_order=20),
        Service(name="Бикини классика", price=45, duration_min=40, buffer_min=0, is_active=True, sort_order=30),
        Service(name="Бикини глубокое", price=55, duration_min=50, buffer_min=0, is_active=True, sort_order=40),
    ])

def _to_tz(dt_utc: datetime, tz: pytz.BaseTzInfo) -> datetime:
    if dt_utc.tzinfo is None:
        dt_utc = pytz.UTC.localize(dt_utc)
    return dt_utc.astimezone(tz)

def _to_utc(dt_local: datetime, tz: pytz.BaseTzInfo) -> datetime:
    if dt_local.tzinfo is None:
        dt_local = tz.localize(dt_local)
    return dt_local.astimezone(pytz.UTC)

def _round_slot(dt_local: datetime, step_min: int) -> datetime:
    m = (dt_local.minute // step_min) * step_min
    return dt_local.replace(minute=m, second=0, microsecond=0)

def compute_slot_end(start_local: datetime, service: Service, settings: SettingsView) -> datetime:
    total_min = int(service.duration_min) + int(service.buffer_min) + int(settings.buffer_min)
    return start_local + timedelta(minutes=total_min)

async def list_available_dates(session: AsyncSession, settings: SettingsView) -> list[date]:
    now_local = _to_tz(datetime.now(tz=pytz.UTC), settings.tz)
    start_date = now_local.date()
    end_date = (now_local + timedelta(days=settings.booking_horizon_days)).date()
    out: list[date] = []
    d = start_date
    while d <= end_date:
        if d.weekday() in settings.work_days:
            out.append(d)
        d += timedelta(days=1)
    return out

async def list_available_slots_for_service(
    session: AsyncSession,
    settings: SettingsView,
    service: Service,
    day: date,
) -> list[datetime]:
    now_local = _to_tz(datetime.now(tz=pytz.UTC), settings.tz)
    earliest_local = now_local + timedelta(minutes=settings.min_lead_time_min)

    work_start_local = settings.tz.localize(datetime.combine(day, settings.work_start))
    work_end_local = settings.tz.localize(datetime.combine(day, settings.work_end))

    step = settings.slot_step_min
    cursor = _round_slot(work_start_local, step)
    slots: list[datetime] = []

    window_start_utc = _to_utc(work_start_local, settings.tz)
    window_end_utc = _to_utc(work_end_local + timedelta(hours=6), settings.tz)

    appts = (await session.execute(
        select(Appointment).where(
            and_(
                Appointment.start_dt < window_end_utc,
                Appointment.end_dt > window_start_utc,
                Appointment.status.in_([AppointmentStatus.Hold, AppointmentStatus.Booked])
            )
        )
    )).scalars().all()

    blocks = (await session.execute(
        select(BlockedInterval).where(
            and_(
                BlockedInterval.start_dt < window_end_utc,
                BlockedInterval.end_dt > window_start_utc
            )
        )
    )).scalars().all()

    def overlaps(start_utc: datetime, end_utc: datetime) -> bool:
        for a in appts:
            if a.start_dt < end_utc and a.end_dt > start_utc:
                return True
        for b in blocks:
            if b.start_dt < end_utc and b.end_dt > start_utc:
                return True
        return False

    while cursor < work_end_local:
        if cursor >= earliest_local:
            end_local = compute_slot_end(cursor, service, settings)
            if end_local <= work_end_local:
                s_utc = _to_utc(cursor, settings.tz)
                e_utc = _to_utc(end_local, settings.tz)
                if not overlaps(s_utc, e_utc):
                    slots.append(cursor)
        cursor += timedelta(minutes=step)

    return slots

def _advisory_key_for_slot(start_utc: datetime, service_id: int) -> int:
    base = f"{int(start_utc.timestamp())}:{service_id}".encode()
    h = hashlib.sha256(base).digest()
    key = int.from_bytes(h[:8], byteorder="big", signed=False)
    return key & ((1 << 63) - 1)

async def _ensure_slot_available(
    session: AsyncSession,
    start_utc: datetime,
    end_utc: datetime,
    service_id: int,
    *,
    exclude_appt_id: int | None = None,
) -> None:
    lock_key = _advisory_key_for_slot(start_utc, service_id)
    await session.execute(text("SELECT pg_advisory_xact_lock(:k)").bindparams(k=lock_key))

    overlap_filters = [
        Appointment.start_dt < end_utc,
        Appointment.end_dt > start_utc,
        Appointment.status.in_([AppointmentStatus.Hold, AppointmentStatus.Booked]),
    ]
    if exclude_appt_id is not None:
        overlap_filters.append(Appointment.id != exclude_appt_id)

    overlap = await session.execute(
        select(Appointment.id).where(and_(*overlap_filters)).limit(1)
    )
    if overlap.first():
        raise ValueError("SLOT_TAKEN")

    block_overlap = await session.execute(
        select(BlockedInterval.id).where(
            and_(
                BlockedInterval.start_dt < end_utc,
                BlockedInterval.end_dt > start_utc
            )
        ).limit(1)
    )
    if block_overlap.first():
        raise ValueError("SLOT_BLOCKED")

async def create_hold_appointment(
    session: AsyncSession,
    settings: SettingsView,
    client: User,
    service: Service,
    start_local: datetime,
    comment: str | None,
) -> Appointment:
    now_utc = datetime.now(tz=pytz.UTC)
    start_utc = _to_utc(start_local, settings.tz)
    end_local = compute_slot_end(start_local, service, settings)
    end_utc = _to_utc(end_local, settings.tz)

    await _ensure_slot_available(session, start_utc, end_utc, service.id)

    appt = Appointment(
        client_user_id=client.id,
        service_id=service.id,
        start_dt=start_utc,
        end_dt=end_utc,
        status=AppointmentStatus.Hold,
        hold_expires_at=now_utc + timedelta(minutes=settings.hold_ttl_min),
        client_comment=comment,
        admin_comment=None,
        proposed_alt_start_dt=None,
        reminder_24h_sent=False,
        reminder_2h_sent=False,
        visit_confirmed=False,
        created_at=now_utc,
        updated_at=now_utc,
    )
    session.add(appt)
    await session.flush()
    return appt

async def create_admin_appointment(
    session: AsyncSession,
    settings: SettingsView,
    client: User,
    service: Service,
    start_local: datetime,
    *,
    price_override: float | None = None,
    client_comment: str | None = None,
    admin_comment: str | None = None,
) -> Appointment:
    now_utc = datetime.now(tz=pytz.UTC)
    start_utc = _to_utc(start_local, settings.tz)
    end_local = compute_slot_end(start_local, service, settings)
    end_utc = _to_utc(end_local, settings.tz)

    await _ensure_slot_available(session, start_utc, end_utc, service.id)

    appt = Appointment(
        client_user_id=client.id,
        service_id=service.id,
        start_dt=start_utc,
        end_dt=end_utc,
        status=AppointmentStatus.Booked,
        hold_expires_at=None,
        client_comment=client_comment,
        admin_comment=admin_comment,
        price_override=price_override,
        proposed_alt_start_dt=None,
        reminder_24h_sent=False,
        reminder_2h_sent=False,
        visit_confirmed=False,
        created_at=now_utc,
        updated_at=now_utc,
    )
    session.add(appt)
    await session.flush()
    return appt

async def check_slot_available(
    session: AsyncSession,
    settings: SettingsView,
    service: Service,
    start_local: datetime,
) -> None:
    start_utc = _to_utc(start_local, settings.tz)
    end_local = compute_slot_end(start_local, service, settings)
    end_utc = _to_utc(end_local, settings.tz)
    await _ensure_slot_available(session, start_utc, end_utc, service.id)

async def request_reschedule(
    session: AsyncSession,
    settings: SettingsView,
    appt: Appointment,
    new_start_local: datetime,
) -> None:
    if appt.status != AppointmentStatus.Booked:
        raise ValueError("NOT_BOOKED")
    now_utc = datetime.now(tz=pytz.UTC)
    start_utc = _to_utc(new_start_local, settings.tz)
    end_local = compute_slot_end(new_start_local, appt.service, settings)
    end_utc = _to_utc(end_local, settings.tz)

    lock_key = _advisory_key_for_slot(start_utc, appt.service_id)
    await session.execute(text("SELECT pg_advisory_xact_lock(:k)").bindparams(k=lock_key))

    overlap = await session.execute(
        select(Appointment.id).where(
            and_(
                Appointment.id != appt.id,
                Appointment.start_dt < end_utc,
                Appointment.end_dt > start_utc,
                Appointment.status.in_([AppointmentStatus.Hold, AppointmentStatus.Booked])
            )
        ).limit(1)
    )
    if overlap.first():
        raise ValueError("SLOT_TAKEN")

    block_overlap = await session.execute(
        select(BlockedInterval.id).where(
            and_(
                BlockedInterval.start_dt < end_utc,
                BlockedInterval.end_dt > start_utc
            )
        ).limit(1)
    )
    if block_overlap.first():
        raise ValueError("SLOT_BLOCKED")

    appt.proposed_alt_start_dt = start_utc
    appt.updated_at = now_utc

async def confirm_reschedule(session: AsyncSession, settings: SettingsView, appt: Appointment) -> None:
    if appt.status != AppointmentStatus.Booked or not appt.proposed_alt_start_dt:
        return
    now_utc = datetime.now(tz=pytz.UTC)
    start_utc = appt.proposed_alt_start_dt
    start_local = _to_tz(start_utc, settings.tz)
    end_local = compute_slot_end(start_local, appt.service, settings)
    end_utc = _to_utc(end_local, settings.tz)

    lock_key = _advisory_key_for_slot(start_utc, appt.service_id)
    await session.execute(text("SELECT pg_advisory_xact_lock(:k)").bindparams(k=lock_key))

    overlap = await session.execute(
        select(Appointment.id).where(
            and_(
                Appointment.id != appt.id,
                Appointment.start_dt < end_utc,
                Appointment.end_dt > start_utc,
                Appointment.status.in_([AppointmentStatus.Hold, AppointmentStatus.Booked])
            )
        ).limit(1)
    )
    if overlap.first():
        raise ValueError("SLOT_TAKEN")

    block_overlap = await session.execute(
        select(BlockedInterval.id).where(
            and_(
                BlockedInterval.start_dt < end_utc,
                BlockedInterval.end_dt > start_utc
            )
        ).limit(1)
    )
    if block_overlap.first():
        raise ValueError("SLOT_BLOCKED")

    appt.start_dt = start_utc
    appt.end_dt = end_utc
    appt.proposed_alt_start_dt = None
    appt.reminder_24h_sent = False
    appt.reminder_2h_sent = False
    appt.visit_confirmed = False
    appt.updated_at = now_utc

async def reject_reschedule(session: AsyncSession, appt: Appointment) -> None:
    if not appt.proposed_alt_start_dt:
        return
    appt.proposed_alt_start_dt = None
    appt.updated_at = datetime.now(tz=pytz.UTC)

async def get_user_appointments(session: AsyncSession, tg_id: int, limit: int = 10) -> list[Appointment]:
    """
    Мои записи (актуальные):
    - только будущие
    - Booked + активные Hold
    - исключаем Rejected/Canceled/Completed
    """
    u = (await session.execute(select(User).where(User.tg_id == tg_id))).scalar_one()
    now_utc = datetime.now(tz=pytz.UTC)

    return (await session.execute(
        select(Appointment)
        .options(
            selectinload(Appointment.service),
            selectinload(Appointment.client),
        )
        .where(
            and_(
                Appointment.client_user_id == u.id,
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
    )).scalars().all()


async def get_user_appointments_history(session: AsyncSession, tg_id: int, limit: int = 10) -> list[Appointment]:
    """
    История:
    - прошедшие записи (start_dt < now)
    - без Hold (они либо сгорели/подтвердились, либо не нужны в истории)
    """
    u = (await session.execute(select(User).where(User.tg_id == tg_id))).scalar_one()
    now_utc = datetime.now(tz=pytz.UTC)

    return (await session.execute(
        select(Appointment)
        .options(
            selectinload(Appointment.service),
            selectinload(Appointment.client),
        )
        .where(
            and_(
                Appointment.client_user_id == u.id,
                Appointment.start_dt < now_utc,
                Appointment.status != AppointmentStatus.Hold,
            )
        )
        .order_by(Appointment.start_dt.desc())
        .limit(limit)
    )).scalars().all()

async def get_appointment(session: AsyncSession, appt_id: int) -> Appointment:
    return (await session.execute(
        select(Appointment)
        .options(
            selectinload(Appointment.service),
            selectinload(Appointment.client),
        )
        .where(Appointment.id == appt_id)
    )).scalar_one()

async def admin_confirm(session: AsyncSession, appt: Appointment) -> None:
    if appt.status != AppointmentStatus.Hold:
        return
    appt.status = AppointmentStatus.Booked
    appt.hold_expires_at = None
    appt.updated_at = datetime.now(tz=pytz.UTC)

async def admin_reject(session: AsyncSession, appt: Appointment, reason: str | None = None) -> None:
    if appt.status not in (AppointmentStatus.Hold, AppointmentStatus.Booked):
        return
    appt.status = AppointmentStatus.Rejected
    appt.admin_comment = reason
    appt.hold_expires_at = None
    appt.updated_at = datetime.now(tz=pytz.UTC)

async def cancel_by_client(session: AsyncSession, settings: SettingsView, appt: Appointment) -> bool:
    if appt.status != AppointmentStatus.Booked:
        return False
    now_utc = datetime.now(tz=pytz.UTC)
    limit = appt.start_dt - timedelta(hours=settings.cancel_limit_hours)
    if now_utc > limit:
        return False
    appt.status = AppointmentStatus.Canceled
    appt.updated_at = now_utc
    return True

async def admin_list_appointments_for_day(session: AsyncSession, tz: pytz.BaseTzInfo, day: date) -> list[Appointment]:
    start_local = tz.localize(datetime.combine(day, datetime.min.time()))
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(pytz.UTC)
    end_utc = end_local.astimezone(pytz.UTC)

    return (await session.execute(
        select(Appointment)
        .options(selectinload(Appointment.client), selectinload(Appointment.service))
        .where(and_(
            Appointment.start_dt >= start_utc,
            Appointment.start_dt < end_utc,
            Appointment.status != AppointmentStatus.Rejected,
        ))
        .order_by(Appointment.start_dt.asc())
    )).scalars().all()


async def admin_list_holds(session: AsyncSession) -> list[Appointment]:
    return (await session.execute(
        select(Appointment)
        .options(selectinload(Appointment.client), selectinload(Appointment.service))
        .where(Appointment.status == AppointmentStatus.Hold)
        .order_by(Appointment.hold_expires_at.asc())
    )).scalars().all()
