from __future__ import annotations

from datetime import datetime, time as dt_time, timedelta, timezone

from telegram.ext import ContextTypes
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

from app.models import Appointment, AppointmentStatus, User, Service
from app.utils import format_price
from texts import AFTERCARE_RECOMMENDATIONS_PARTS



WEEKDAY_RU_FULL = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]

def weekday_ru_full(dt: datetime) -> str:
    return WEEKDAY_RU_FULL[dt.weekday()]

REMINDER_48H_TEMPLATE = (
    "👋 Здравствуйте!\n\n"
    "Напоминаем о вашей записи:\n"
    "**{service}**\n"
    "📅 **{date}**\n"
    "⏰ **{time}**\n\n"
    "Если планы изменились — запись можно перенести или отменить заранее.\n"
    "Будем рады видеть вас 💛"
)

REMINDER_3H_TEMPLATE = (
    "⏰ Скоро встречаемся!\n\n"
    "Ваша запись сегодня:\n"
    "**{service}**\n"
    "🕒 **{time}**\n\n"
    "Пожалуйста, приходите за 5 минут.\n"
    "Оплата наличными.\n"
    "Если не успеваете, напишите, я постараюсь помочь 🤝"
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _fmt_date(dt: datetime, tz_name: str) -> tuple[str, str]:
    # dt в БД timezone-aware; переводим в tz бота (чтобы клиент видел локальное время)
    try:
        import pytz
        tz = pytz.timezone(tz_name)
        local = dt.astimezone(tz)
    except Exception:
        local = dt
    return f"{weekday_ru_full(local)}, {local.strftime('%d.%m.%Y')}", local.strftime('%H:%M')


async def check_and_send_reminders(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Запускается JobQueue раз в минуту.
    Шлём:
      - за 48 часов (флаг reminder_24h_sent используем как "первое напоминание")
      - за 3 часа   (флаг reminder_2h_sent используем как "второе напоминание")
    Только для AppointmentStatus.Booked.
    """
    app = context.application
    session_factory = app.bot_data.get("session_factory")
    if session_factory is None:
        # если у тебя session_factory хранится иначе — скажи, поменяю
        return

    tz_name = app.bot_data.get("tz", "Europe/Moscow")
    now = _utcnow()

    # Окна под отправку (чтобы не ловить погрешности по минутам)
    # 48 часов: попадаем в окно [48h, 48h+2min)
    # 3 часа:   попадаем в окно [3h, 3h+2min)
    win = timedelta(minutes=2)

    target_48_from = now + timedelta(hours=48)
    target_48_to = target_48_from + win

    target_3_from = now + timedelta(hours=3)
    target_3_to = target_3_from + win

    async with session_factory() as session:
        # --- 48h reminders ---
        q48 = (
            select(Appointment)
            .options(selectinload(Appointment.client), selectinload(Appointment.service))
            .where(Appointment.status == AppointmentStatus.Booked)
            .where(Appointment.reminder_24h_sent.is_(False))   # используем как "48h не отправляли"
            .where(Appointment.start_dt >= target_48_from)
            .where(Appointment.start_dt < target_48_to)
        )
        res48 = await session.execute(q48)
        appts48 = list(res48.scalars().all())

        for appt in appts48:
            if not appt.client or not appt.client.tg_id:
                continue

            d, t = _fmt_date(appt.start_dt, tz_name)
            text = REMINDER_48H_TEMPLATE.format(
                service=(appt.service.name if appt.service else "Услуга"),
                date=d,
                time=t,
            )

            try:
                await context.bot.send_message(
                    chat_id=appt.client.tg_id,
                    text=text,
                    parse_mode="Markdown",
                )
                # помечаем как отправленное
                await session.execute(
                    update(Appointment)
                    .where(Appointment.id == appt.id)
                    .values(reminder_24h_sent=True, updated_at=_utcnow())
                )
            except Exception:
                # не валим весь джоб из-за 1 ошибки
                continue

        # --- 3h reminders ---
        q3 = (
            select(Appointment)
            .options(selectinload(Appointment.client), selectinload(Appointment.service))
            .where(Appointment.status == AppointmentStatus.Booked)
            .where(Appointment.reminder_2h_sent.is_(False))   # используем как "3h не отправляли"
            .where(Appointment.start_dt >= target_3_from)
            .where(Appointment.start_dt < target_3_to)
        )
        res3 = await session.execute(q3)
        appts3 = list(res3.scalars().all())

        for appt in appts3:
            if not appt.client or not appt.client.tg_id:
                continue

            d, t = _fmt_date(appt.start_dt, tz_name)
            text = REMINDER_3H_TEMPLATE.format(
                service=(appt.service.name if appt.service else "Услуга"),
                time=t,
            )

            try:
                await context.bot.send_message(
                    chat_id=appt.client.tg_id,
                    text=text,
                    parse_mode="Markdown",
                )
                await session.execute(
                    update(Appointment)
                    .where(Appointment.id == appt.id)
                    .values(reminder_2h_sent=True, updated_at=_utcnow())
                )
            except Exception:
                continue

        await session.commit()

    # После commit можно отправить пост-уходовые рекомендации
    async with session_factory() as session:
        q_aftercare = (
            select(Appointment)
            .options(selectinload(Appointment.client), selectinload(Appointment.service))
            .where(Appointment.status == AppointmentStatus.Booked)
            .where(Appointment.end_dt <= now)
        )
        res_aftercare = await session.execute(q_aftercare)
        appts_aftercare = list(res_aftercare.scalars().all())

        for appt in appts_aftercare:
            if not appt.client or not appt.client.tg_id:
                continue

            try:
                for part in AFTERCARE_RECOMMENDATIONS_PARTS:
                    await context.bot.send_message(
                        chat_id=appt.client.tg_id,
                        text=part,
                    )
                await session.execute(
                    update(Appointment)
                    .where(Appointment.id == appt.id)
                    .values(status=AppointmentStatus.Completed, updated_at=_utcnow())
                )
            except Exception:
                continue

        await session.commit()


async def send_daily_admin_schedule(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Ежедневная сводка записей на сегодня для мастера (админа).
    """
    app = context.application
    session_factory = app.bot_data.get("session_factory")
    cfg = app.bot_data.get("cfg")
    if session_factory is None or cfg is None:
        return

    admin_id = getattr(cfg, "admin_telegram_id", None)
    if not admin_id:
        return

    tz_name = app.bot_data.get("tz", "Europe/Moscow")
    try:
        import pytz
        tz = pytz.timezone(tz_name)
    except Exception:
        tz = timezone.utc

    now_local = datetime.now(tz=tz)
    day = now_local.date()
    start_local = datetime.combine(day, dt_time.min)
    if hasattr(tz, "localize"):
        start_local = tz.localize(start_local)
    else:
        start_local = start_local.replace(tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    async with session_factory() as session:
        q = (
            select(Appointment)
            .options(selectinload(Appointment.client), selectinload(Appointment.service))
            .where(Appointment.status == AppointmentStatus.Booked)
            .where(Appointment.start_dt >= start_utc)
            .where(Appointment.start_dt < end_utc)
            .order_by(Appointment.start_dt.asc())
        )
        res = await session.execute(q)
        appts = list(res.scalars().all())

    if not appts:
        text = "На сегодня записей нет."
    else:
        day_label = f"{day.strftime('%d.%m.%Y')} ({weekday_ru_full(now_local)})"
        lines = [f"📅 Записи на сегодня: {day_label}"]
        for appt in appts:
            start_t = appt.start_dt.astimezone(tz).strftime("%H:%M")
            end_t = appt.end_dt.astimezone(tz).strftime("%H:%M")
            client = appt.client.full_name or (
                f"@{appt.client.username}" if appt.client.username else str(appt.client.tg_id)
            )
            phone = appt.client.phone or "—"
            price = format_price(
                appt.price_override if appt.price_override is not None else appt.service.price
            )
            lines.append(
                f"• {start_t}–{end_t} | {appt.service.name} | {price} | {client} | {phone}"
            )
        text = "\n".join(lines)

    try:
        await context.bot.send_message(chat_id=admin_id, text=text)
    except Exception:
        return
