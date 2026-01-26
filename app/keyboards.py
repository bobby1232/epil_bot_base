from __future__ import annotations
from datetime import date, datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from app.models import Service, Appointment

def main_menu_kb() -> ReplyKeyboardMarkup:
    kb = [
        ["Записаться", "Цены и услуги"],
        ["Адрес / Контакты", "Мои записи"],
        ["История"],
        ["Задать вопрос"],
    ]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def admin_menu_kb() -> ReplyKeyboardMarkup:
    kb = [
        ["📅 Записи сегодня", "📅 Записи завтра"],
        ["🧾 Все заявки (Ожидание)"],
        ["⬅️ В главное меню"],
    ]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def phone_request_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton("📞 Отправить телефон", request_contact=True)], ["⬅️ Назад"]],
        resize_keyboard=True,
        one_time_keyboard=True
    )

def services_kb(services: list[Service]) -> InlineKeyboardMarkup:
    rows = []
    for s in services:
        rows.append([InlineKeyboardButton(f"{s.name} • {int(s.duration_min)} мин • {s.price}", callback_data=f"svc:{s.id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back:main")])
    return InlineKeyboardMarkup(rows)

def dates_kb(dates: list[date]) -> InlineKeyboardMarkup:
    rows = []
    for d in dates:
        rows.append([InlineKeyboardButton(d.strftime("%d.%m (%a)"), callback_data=f"date:{d.isoformat()}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back:services")])
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

def my_appts_kb(appts: list[Appointment]) -> InlineKeyboardMarkup:
    rows = []
    for a in appts:
        rows.append([InlineKeyboardButton(f"#{a.id} • {a.start_dt.astimezone().strftime('%d.%m %H:%M')} • {a.status.value}", callback_data=f"my:{a.id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back:main")])
    return InlineKeyboardMarkup(rows)

def my_appt_actions_kb(appt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚫 Отменить", callback_data=f"mycancel:{appt_id}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="myback:list")]
    ])

def reminder_kb(appt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить визит", callback_data=f"r:confirm:{appt_id}")],
        [InlineKeyboardButton("🚫 Отменить", callback_data=f"r:cancel:{appt_id}")],
    ])
