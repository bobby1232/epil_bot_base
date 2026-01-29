from __future__ import annotations
from datetime import date, datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from app.models import Service, Appointment
from app.utils import format_price

STATUS_RU = {
    "Hold": "Ожидает подтверждения",
    "Booked": "Подтверждена",
    "Rejected": "Отклонена",
    "Canceled": "Отменена",
    "Completed": "Завершена",
}

RU_WEEKDAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

def status_ru(v: str) -> str:
    return STATUS_RU.get(v, v)

def _format_date_ru(d: date) -> str:
    return f"{d.strftime('%d.%m')} ({RU_WEEKDAYS[d.weekday()]})"

def main_menu_kb(is_admin: bool = False) -> ReplyKeyboardMarkup:
    kb = [
        ["Записаться", "Цены и услуги"],
        ["Адрес / Контакты", "Мои записи"],
        ["История"],
        ["Подготовка к процедуре", "Уход после процедуры"],
        ["Задать вопрос"],
    ]
    if is_admin:
        kb.append(["Админ-меню"])
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def admin_menu_kb() -> ReplyKeyboardMarkup:
    kb = [
        ["📅 Записи сегодня", "📅 Записи завтра"],
        ["🧾 Все заявки (Ожидание)", "🗓 Все заявки"],
        ["📝 Записать клиента"],
        ["⏸ Перерыв", "🗑 Отменить перерыв"],
        ["⬅️ В главное меню"],
    ]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def phone_request_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📞 Отправить телефон", request_contact=True)],
            ["⬅️ Назад"],
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

def services_kb(services: list[Service]) -> InlineKeyboardMarkup:
    rows = []
    for s in services:
        price = format_price(s.price)
        rows.append([InlineKeyboardButton(f"{s.name} • {int(s.duration_min)} мин • {price}", callback_data=f"svc:{s.id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back:main")])
    return InlineKeyboardMarkup(rows)

def admin_services_kb(services: list[Service]) -> InlineKeyboardMarkup:
    rows = []
    for s in services:
        price = format_price(s.price)
        rows.append([InlineKeyboardButton(f"{s.name} • {int(s.duration_min)} мин • {price}", callback_data=f"admsvc:{s.id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back:main")])
    return InlineKeyboardMarkup(rows)

def dates_kb(dates: list[date]) -> InlineKeyboardMarkup:
    rows = []
    for d in dates:
        rows.append([InlineKeyboardButton(_format_date_ru(d), callback_data=f"date:{d.isoformat()}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back:services")])
    return InlineKeyboardMarkup(rows)

def admin_dates_kb(dates: list[date]) -> InlineKeyboardMarkup:
    rows = []
    for d in dates:
        rows.append([InlineKeyboardButton(_format_date_ru(d), callback_data=f"admdate:{d.isoformat()}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="admback:services")])
    return InlineKeyboardMarkup(rows)

def break_dates_kb(dates: list[date]) -> InlineKeyboardMarkup:
    rows = []
    for d in dates:
        rows.append([InlineKeyboardButton(_format_date_ru(d), callback_data=f"breakdate:{d.isoformat()}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back:main")])
    return InlineKeyboardMarkup(rows)

def admin_slots_kb(slots_local: list[datetime]) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for dt in slots_local:
        row.append(InlineKeyboardButton(dt.strftime("%H:%M"), callback_data=f"admtime:{dt.isoformat()}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="admback:dates")])
    return InlineKeyboardMarkup(rows)

def break_slots_kb(slots_local: list[datetime]) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for dt in slots_local:
        row.append(InlineKeyboardButton(dt.strftime("%H:%M"), callback_data=f"breaktime:{dt.isoformat()}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="breakback:dates")])
    return InlineKeyboardMarkup(rows)

def slots_kb(slots_local: list[datetime]) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for dt in slots_local:
        row.append(InlineKeyboardButton(dt.strftime("%H:%M"), callback_data=f"slot:{dt.isoformat()}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back:dates")])
    return InlineKeyboardMarkup(rows)

def confirm_request_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Отправить заявку", callback_data="req:send")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back:dates")]
    ])

def admin_request_kb(appt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить", callback_data=f"adm:confirm:{appt_id}")],
        [InlineKeyboardButton("❌ Отклонить", callback_data=f"adm:reject:{appt_id}")],
        [InlineKeyboardButton("💬 Написать клиенту", callback_data=f"adm:msg:{appt_id}")],
    ])

def admin_manage_appt_kb(appt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Перенести", callback_data=f"admresched:start:{appt_id}")],
        [InlineKeyboardButton("🚫 Отменить", callback_data=f"adm:cancel:{appt_id}")],
    ])

def my_appts_kb(appts: list[Appointment], tz=None) -> InlineKeyboardMarkup:
    rows = []
    for a in appts:
        dt = a.start_dt.astimezone(tz) if tz else a.start_dt.astimezone()
        rows.append([
            InlineKeyboardButton(
                f"{dt.strftime('%d.%m %H:%M')} • {status_ru(a.status.value)}",
                callback_data=f"my:{a.id}",
            )
        ])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back:main")])
    return InlineKeyboardMarkup(rows)

def my_appt_actions_kb(appt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Перенести", callback_data=f"myresched:{appt_id}")],
        [InlineKeyboardButton("🚫 Отменить", callback_data=f"mycancel:{appt_id}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="myback:list")]
    ])

def reschedule_dates_kb(dates: list[date]) -> InlineKeyboardMarkup:
    rows = []
    for d in dates:
        rows.append([InlineKeyboardButton(_format_date_ru(d), callback_data=f"rdate:{d.isoformat()}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="myback:list")])
    return InlineKeyboardMarkup(rows)

def reschedule_slots_kb(slots_local: list[datetime]) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for dt in slots_local:
        row.append(InlineKeyboardButton(dt.strftime("%H:%M"), callback_data=f"rslot:{dt.isoformat()}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="rback:dates")])
    return InlineKeyboardMarkup(rows)

def reschedule_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Отправить запрос", callback_data="resched:send")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="rback:dates")]
    ])

def admin_reschedule_kb(appt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить перенос", callback_data=f"adm:resched:confirm:{appt_id}")],
        [InlineKeyboardButton("❌ Отклонить перенос", callback_data=f"adm:resched:reject:{appt_id}")],
    ])

def admin_reschedule_dates_kb(dates: list[date]) -> InlineKeyboardMarkup:
    rows = []
    for d in dates:
        rows.append([InlineKeyboardButton(_format_date_ru(d), callback_data=f"admresched:date:{d.isoformat()}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back:main")])
    return InlineKeyboardMarkup(rows)

def admin_reschedule_slots_kb(slots_local: list[datetime]) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for dt in slots_local:
        row.append(InlineKeyboardButton(dt.strftime("%H:%M"), callback_data=f"admresched:slot:{dt.isoformat()}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="admresched:back:dates")])
    return InlineKeyboardMarkup(rows)

def admin_reschedule_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить перенос", callback_data="admresched:send")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="admresched:back:dates")],
    ])

def reminder_kb(appt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить визит", callback_data=f"r:confirm:{appt_id}")],
        [InlineKeyboardButton("🚫 Отменить", callback_data=f"r:cancel:{appt_id}")],
    ])

def cancel_breaks_kb(blocks: list[tuple[int, datetime, datetime]]) -> InlineKeyboardMarkup:
    rows = []
    for block_id, start_local, end_local in blocks:
        weekday = RU_WEEKDAYS[start_local.weekday()]
        date_label = f"{start_local.strftime('%d.%m')} ({weekday})"
        if start_local.date() == end_local.date():
            time_label = f"{start_local.strftime('%H:%M')}–{end_local.strftime('%H:%M')}"
        else:
            time_label = (
                f"{start_local.strftime('%H:%M')}–{end_local.strftime('%d.%m %H:%M')}"
            )
        label = f"{date_label} {time_label}"
        rows.append([InlineKeyboardButton(f"🗑 Отменить {label}", callback_data=f"breakcancel:{block_id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back:main")])
    return InlineKeyboardMarkup(rows)
