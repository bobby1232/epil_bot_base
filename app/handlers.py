from __future__ import annotations
from datetime import datetime, date, timedelta, time
from urllib.parse import quote
import asyncio
import logging
import pytz

from telegram import Update
from telegram.ext import ContextTypes

from app.config import Config
from app.logic import (
    get_settings, upsert_user, set_user_phone, list_active_services, list_available_dates,
    list_available_slots_for_service, list_available_slots_for_duration,
    create_hold_appointment, get_user_appointments,
    get_user_appointments_history, get_appointment, admin_confirm, admin_reject,
    cancel_by_client, request_reschedule, confirm_reschedule, reject_reschedule,
    admin_list_appointments_for_day, admin_list_holds, create_admin_appointment,
    create_admin_appointment_with_duration, check_slot_available,
    check_slot_available_for_duration, compute_slot_end, compute_slot_end_for_duration,
    admin_cancel_appointment, list_available_break_slots, create_blocked_interval,
    admin_reschedule_appointment, admin_list_booked_range, list_future_breaks,
    delete_blocked_interval, SettingsView
)
from app.keyboards import (
    main_menu_kb, phone_request_kb, services_kb, dates_kb, slots_kb, confirm_request_kb,
    admin_request_kb, my_appts_kb, my_appt_actions_kb, admin_menu_kb,
    reschedule_dates_kb, reschedule_slots_kb, reschedule_confirm_kb, admin_reschedule_kb,
    admin_services_kb, admin_dates_kb, admin_slots_kb, admin_manage_appt_kb,
    admin_reschedule_dates_kb, admin_reschedule_slots_kb, admin_reschedule_confirm_kb,
    break_dates_kb, break_slots_kb, status_ru, RU_WEEKDAYS, cancel_breaks_kb,
    contacts_kb,
)
from app.models import AppointmentStatus
from app.utils import format_price
from texts import (
    CONTACTS,
    PRECARE_RECOMMENDATIONS,
    AFTERCARE_RECOMMENDATIONS,
    PRECARE_RECOMMENDATIONS_PARTS,
)

logger = logging.getLogger(__name__)

K_SVC = "svc_id"
K_DATE = "date"
K_SLOT = "slot_iso"
K_COMMENT = "comment"
K_PHONE = "phone"
K_RESCHED_APPT = "resched_appt_id"
K_RESCHED_SVC = "resched_svc_id"
K_RESCHED_DATE = "resched_date"
K_RESCHED_SLOT = "resched_slot_iso"
K_ADMIN_SVC = "admin_svc_id"
K_ADMIN_DATE = "admin_date"
K_ADMIN_TIME = "admin_time_iso"
K_ADMIN_DURATION = "admin_duration_min"
K_ADMIN_CLIENT_NAME = "admin_client_name"
K_ADMIN_CLIENT_PHONE = "admin_client_phone"
K_ADMIN_CLIENT_TGID = "admin_client_tg_id"
K_ADMIN_PRICE = "admin_price_override"
K_ADMIN_TIME_ERRORS = "admin_time_errors"
K_ADMIN_RESCHED_APPT = "admin_resched_appt_id"
K_ADMIN_RESCHED_SVC = "admin_resched_svc_id"
K_ADMIN_RESCHED_DATE = "admin_resched_date"
K_ADMIN_RESCHED_SLOT = "admin_resched_slot_iso"
K_BREAK_DATE = "break_date"
K_BREAK_DURATION = "break_duration_min"
K_BREAK_TIME_ERRORS = "break_time_errors"

ADDRESS_LINE = "Мусы Джалиля 30 к1, квартира 123"

def admin_ids(cfg: Config) -> tuple[int, ...]:
    ids = getattr(cfg, "admin_telegram_ids", None)
    if ids:
        return tuple(ids)
    admin_id = getattr(cfg, "admin_telegram_id", None)
    if admin_id:
        return (int(admin_id),)
    return tuple()

def is_admin(cfg: Config, user_id: int) -> bool:
    return user_id in admin_ids(cfg)

async def notify_admins(
    context: ContextTypes.DEFAULT_TYPE,
    cfg: Config,
    text: str,
    reply_markup=None,
) -> None:
    for admin_id in admin_ids(cfg):
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=text,
                reply_markup=reply_markup,
            )
        except Exception:
            logger.exception("Failed to notify admin %s", admin_id)

def main_menu_for(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config | None = context.bot_data.get("cfg")
    if cfg and update.effective_user:
        return main_menu_kb(is_admin(cfg, update.effective_user.id))
    return main_menu_kb()

def _clear_admin_booking(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in (
        K_ADMIN_SVC,
        K_ADMIN_DATE,
        K_ADMIN_TIME,
        K_ADMIN_DURATION,
        K_ADMIN_CLIENT_NAME,
        K_ADMIN_CLIENT_PHONE,
        K_ADMIN_CLIENT_TGID,
        K_ADMIN_PRICE,
        K_ADMIN_TIME_ERRORS,
    ):
        context.user_data.pop(key, None)
    for flag in (
        "awaiting_admin_time",
        "awaiting_admin_duration",
        "awaiting_admin_client_name",
        "awaiting_admin_client_phone",
        "awaiting_admin_client_tg",
        "awaiting_admin_price",
    ):
        context.user_data.pop(flag, None)

def _clear_admin_reschedule(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in (
        K_ADMIN_RESCHED_APPT,
        K_ADMIN_RESCHED_SVC,
        K_ADMIN_RESCHED_DATE,
        K_ADMIN_RESCHED_SLOT,
    ):
        context.user_data.pop(key, None)

def _clear_break(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in (K_BREAK_DATE, K_BREAK_DURATION, K_BREAK_TIME_ERRORS):
        context.user_data.pop(key, None)
    for flag in ("awaiting_break_duration", "awaiting_break_time"):
        context.user_data.pop(flag, None)

def _normalize_phone(value: str) -> str:
    phone = (value or "").strip()
    for ch in [" ", "-", "(", ")", "\u00A0"]:
        phone = phone.replace(ch, "")
    return phone

def _generate_offline_tg_id() -> int:
    return -int(datetime.now(tz=pytz.UTC).timestamp() * 1_000_000)

def _increment_admin_time_errors(context: ContextTypes.DEFAULT_TYPE) -> int:
    errors = int(context.user_data.get(K_ADMIN_TIME_ERRORS, 0)) + 1
    context.user_data[K_ADMIN_TIME_ERRORS] = errors
    return errors

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        async with s.begin():
            await upsert_user(s, update.effective_user.id, update.effective_user.username, update.effective_user.full_name)
    await update.message.reply_text(
        "Привет! \n\n"
        "👋 Я — бот Адрианы по записи на депиляцию ✨\n\n"
        "С моей помощью ты можешь: \n"
        "• быстро записаться на процедуру в удобное время \n"
        "• узнать стоимость услуг и адрес студии \n"
        "• посмотреть историю своих записей \n"
        "• получать напоминания, чтобы ничего не забыть 🗓️\n"
        "Я здесь, чтобы сделать процесс записи простым и комфортным \n\n"
        "💛 Если понадобится помощь — я рядом.\n"
        "Приятного пользования и до встречи на процедуре 🤗",
        reply_markup=main_menu_for(update, context)
    )
    if is_admin(cfg, update.effective_user.id):
        await update.message.reply_text("Админ-панель 👇", reply_markup=admin_menu_kb())

async def unified_text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("awaiting_break_duration"):
        return await handle_break_duration(update, context)
    if context.user_data.get("awaiting_break_time"):
        return await handle_break_time(update, context)
    if context.user_data.get("awaiting_admin_duration"):
        return await handle_admin_duration(update, context)
    if context.user_data.get("awaiting_admin_time"):
        return await handle_admin_time(update, context)
    if context.user_data.get("awaiting_admin_client_name"):
        return await handle_admin_client_name(update, context)
    if context.user_data.get("awaiting_admin_client_phone"):
        return await handle_admin_client_phone(update, context)
    if context.user_data.get("awaiting_admin_client_tg"):
        return await handle_admin_client_tg(update, context)
    if context.user_data.get("awaiting_admin_price"):
        return await handle_admin_price(update, context)
    if context.user_data.get("awaiting_question"):
        return await handle_question(update, context)
    if context.user_data.get("awaiting_comment"):
        return await handle_comment(update, context)
    if context.user_data.get("awaiting_phone"):
        return await handle_contact(update, context)
    return await text_router(update, context)

async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (update.message.text or "").strip()
    if txt == "Записаться":
        return await flow_services(update, context)
    if txt == "Цены и услуги":
        return await show_prices(update, context)
    if txt == "Адрес / Контакты":
        return await show_contacts(update, context)
    if txt == "Мои записи":
        return await show_my_appointments(update, context)
    if txt == "История":
        return await show_my_history(update, context)
    if txt == "Подготовка к процедуре":
        return await show_precare(update, context)
    if txt == "Уход после процедуры":
        return await show_aftercare(update, context)
    if txt == "Задать вопрос":
        return await ask_question(update, context)

    # Admin menu (только для ADMIN_TELEGRAM_ID)
    cfg: Config = context.bot_data.get("cfg")
    if cfg and is_admin(cfg, update.effective_user.id):
        if txt == "📅 Записи сегодня":
            return await admin_day_view(update, context, offset_days=0)
        if txt == "📅 Записи завтра":
            return await admin_day_view(update, context, offset_days=1)
        if txt == "🧾 Все заявки (Ожидание)":
            return await admin_holds_view(update, context)
        if txt == "🗓 Все заявки":
            return await admin_booked_month_view(update, context)
        if txt == "📝 Записать клиента":
            return await admin_start_booking(update, context)
        if txt == "⏸ Перерыв":
            return await admin_start_break(update, context)
        if txt == "🗑 Отменить перерыв":
            return await admin_cancel_break_view(update, context)
        if txt == "⬅️ В главное меню":
            await update.message.reply_text("Главное меню 👇", reply_markup=main_menu_for(update, context))
            return
        if txt == "Админ-меню":
            await update.message.reply_text("Админ-панель 👇", reply_markup=admin_menu_kb())
            return

    await update.message.reply_text("Используй кнопки меню 👇", reply_markup=main_menu_for(update, context))

async def show_prices(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        services = await list_active_services(s)
    if not services:
        await update.message.reply_text("Пока нет услуг. Напиши мастеру.", reply_markup=main_menu_for(update, context))
        return
    lines = ["Прайс-лист:"]
    for sv in services:
        lines.append(f"• {sv.name}: {format_price(sv.price)} / {int(sv.duration_min)} мин")
    await update.message.reply_text("\n".join(lines), reply_markup=main_menu_for(update, context))

async def show_contacts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    address_query = quote(ADDRESS_LINE)
    yandex_maps_url = f"https://yandex.ru/maps/?text={address_query}"
    await update.message.reply_text(
        CONTACTS,
        reply_markup=contacts_kb(yandex_maps_url=yandex_maps_url),
    )

async def send_address_copy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.message.reply_text(
        f"Адрес для копирования:\n{ADDRESS_LINE}",
        reply_markup=main_menu_for(update, context),
    )

async def show_precare(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        PRECARE_RECOMMENDATIONS,
        reply_markup=main_menu_for(update, context),
    )

async def show_aftercare(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        AFTERCARE_RECOMMENDATIONS,
        reply_markup=main_menu_for(update, context),
    )

async def ask_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Напиши вопрос одним сообщением — я перешлю мастеру.", reply_markup=main_menu_for(update, context))
    context.user_data["awaiting_question"] = True

async def handle_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    if not context.user_data.get("awaiting_question"):
        return
    context.user_data["awaiting_question"] = False
    q = update.message.text.strip()
    user = update.effective_user
    await notify_admins(
        context,
        cfg,
        text=(
            "❓ Вопрос от клиента:\n"
            f"Имя: {user.full_name}\n@{user.username}\nTG ID: {user.id}\n\n{q}"
        ),
    )
    await update.message.reply_text("Отправлено ✅ Мастер ответит вам в Telegram.", reply_markup=main_menu_for(update, context))

async def flow_services(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        services = await list_active_services(s)
    if not services:
        await update.message.reply_text("Услуги пока не настроены. Напишите мастеру.", reply_markup=main_menu_for(update, context))
        return
    await update.message.reply_text("Выбери услугу:", reply_markup=services_kb(services))

async def admin_start_booking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        if update.message:
            return await update.message.reply_text("Нет доступа.")
        if update.callback_query:
            return await update.callback_query.message.edit_text("Нет доступа.")
        return
    _clear_admin_booking(context)
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        services = await list_active_services(s)
    if not services:
        if update.message:
            await update.message.reply_text("Услуги пока не настроены.", reply_markup=admin_menu_kb())
        elif update.callback_query:
            await update.callback_query.message.edit_text("Услуги пока не настроены.")
        return
    if update.message:
        await update.message.reply_text("Выбери услугу для записи:", reply_markup=admin_services_kb(services))
    elif update.callback_query:
        await update.callback_query.message.edit_text("Выбери услугу для записи:", reply_markup=admin_services_kb(services))

async def cb_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    if data.startswith("svc:"):
        context.user_data[K_SVC] = int(data.split(":")[1])
        return await flow_dates(update, context)

    if data.startswith("admsvc:"):
        context.user_data[K_ADMIN_SVC] = int(data.split(":")[1])
        return await admin_flow_dates(update, context)

    if data.startswith("date:"):
        context.user_data[K_DATE] = data.split(":")[1]
        if context.user_data.get(K_RESCHED_APPT):
            return await flow_reschedule_slots(update, context)
        return await flow_slots(update, context)

    if data.startswith("admdate:"):
        context.user_data[K_ADMIN_DATE] = data.split(":")[1]
        return await admin_prompt_duration(update, context)

    if data.startswith("breakdate:"):
        context.user_data[K_BREAK_DATE] = data.split(":")[1]
        return await admin_break_prompt_duration(update, context)

    if data.startswith("slot:"):
        context.user_data[K_SLOT] = data.split("slot:")[1]
        if context.user_data.get(K_RESCHED_APPT):
            context.user_data[K_RESCHED_SLOT] = context.user_data[K_SLOT]
            return await confirm_reschedule_request(update, context)
        return await flow_comment(update, context)

    if data == "req:send":
        return await finalize_request(update, context)

    if data.startswith("myresched:"):
        appt_id = int(data.split(":")[1])
        return await start_reschedule(update, context, appt_id)

    if data.startswith("adm:confirm:"):
        appt_id = int(data.split(":")[2])
        return await admin_action_confirm(update, context, appt_id)

    if data.startswith("adm:reject:"):
        appt_id = int(data.split(":")[2])
        return await admin_action_reject(update, context, appt_id)

    if data.startswith("adm:msg:"):
        appt_id = int(data.split(":")[2])
        return await admin_action_msg(update, context, appt_id)

    if data.startswith("adm:cancel:"):
        appt_id = int(data.split(":")[2])
        return await admin_cancel(update, context, appt_id)

    if data.startswith("admresched:start:"):
        appt_id = int(data.split(":")[2])
        return await admin_start_reschedule(update, context, appt_id)

    if data.startswith("admtime:"):
        slot_iso = data.split(":", 1)[1]
        return await admin_pick_time_from_slots(update, context, slot_iso)

    if data.startswith("breaktime:"):
        slot_iso = data.split(":", 1)[1]
        return await admin_pick_break_time(update, context, slot_iso)

    if data.startswith("breakcancel:"):
        block_id = int(data.split(":", 1)[1])
        return await admin_cancel_break(update, context, block_id)

    if data == "back:main":
        await query.message.reply_text("Главное меню 👇", reply_markup=main_menu_for(update, context))
        return

    if data == "back:services":
        return await flow_services_from_callback(update, context)

    if data == "back:dates":
        return await flow_dates(update, context)

    if data == "back:phone":
        context.user_data.pop(K_PHONE, None)
        return await prompt_phone(update, context)

    if data == "admback:services":
        return await admin_start_booking(update, context)

    if data == "admback:dates":
        return await admin_flow_dates(update, context)

    if data == "breakback:dates":
        return await admin_start_break(update, context)

    if data == "myback:list":
        return await show_my_appointments_from_cb(update, context)

    if data.startswith("my:"):
        appt_id = int(data.split(":")[1])
        return await show_my_appointment_detail(update, context, appt_id)

    if data.startswith("mycancel:"):
        appt_id = int(data.split(":")[1])
        return await client_cancel(update, context, appt_id)

    if data.startswith("r:confirm:"):
        appt_id = int(data.split(":")[2])
        return await reminder_confirm(update, context, appt_id)

    if data.startswith("r:cancel:"):
        appt_id = int(data.split(":")[2])
        return await reminder_cancel(update, context, appt_id)

    if data.startswith("r:resched:"):
        appt_id = int(data.split(":")[2])
        return await start_reschedule(update, context, appt_id)

    if data.startswith("rdate:"):
        context.user_data[K_RESCHED_DATE] = data.split(":")[1]
        return await flow_reschedule_slots(update, context)

    if data.startswith("rslot:"):
        context.user_data[K_RESCHED_SLOT] = data.split(":")[1]
        return await confirm_reschedule_request(update, context)

    if data == "resched:send":
        return await finalize_reschedule_request(update, context)

    if data == "rback:dates":
        return await flow_reschedule_dates(update, context)

    if data.startswith("admresched:date:"):
        context.user_data[K_ADMIN_RESCHED_DATE] = data.split(":")[2]
        return await admin_flow_reschedule_slots(update, context)

    if data.startswith("admresched:slot:"):
        context.user_data[K_ADMIN_RESCHED_SLOT] = data.split(":")[2]
        return await admin_confirm_reschedule(update, context)

    if data == "admresched:send":
        return await admin_finalize_reschedule(update, context)

    if data == "admresched:back:dates":
        return await admin_flow_reschedule_dates(update, context)

    if data.startswith("adm:resched:confirm:"):
        appt_id = int(data.split(":")[3])
        return await admin_reschedule_confirm(update, context, appt_id)

    if data.startswith("adm:resched:reject:"):
        appt_id = int(data.split(":")[3])
        return await admin_reschedule_reject(update, context, appt_id)

    if data == "contact:copy":
        return await send_address_copy(update, context)

async def flow_services_from_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.callback_query.message
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        services = await list_active_services(s)
    await msg.edit_text("Выбери услугу:", reply_markup=services_kb(services))

async def flow_dates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_factory = context.bot_data["session_factory"]
    cfg: Config = context.bot_data["cfg"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        dates = await list_available_dates(s, settings)
    await update.callback_query.message.edit_text("Выбери дату:", reply_markup=dates_kb(dates))

async def admin_flow_dates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_factory = context.bot_data["session_factory"]
    cfg: Config = context.bot_data["cfg"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        dates = await list_available_dates(s, settings)
    await update.callback_query.message.edit_text("Выбери дату для записи:", reply_markup=admin_dates_kb(dates))

async def admin_start_break(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        return await update.effective_message.reply_text("Нет доступа.")
    _clear_break(context)
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        dates = await list_available_dates(s, settings)
    await update.effective_message.reply_text("Выбери день перерыва:", reply_markup=break_dates_kb(dates))

async def admin_break_prompt_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["awaiting_break_duration"] = True
    await update.callback_query.message.edit_text(
        "Укажи длительность перерыва в минутах (например, 30)."
    )

async def admin_prompt_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["awaiting_admin_duration"] = True
    await update.callback_query.message.edit_text(
        "Введи длительность услуги в минутах (например, 45).\n"
        "Можно отправить «-», чтобы взять стандартную длительность услуги."
    )

async def _admin_send_time_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.bot_data["cfg"]
    svc_id = context.user_data.get(K_ADMIN_SVC)
    day_iso = context.user_data.get(K_ADMIN_DATE)
    if not svc_id or not day_iso:
        _clear_admin_booking(context)
        await update.effective_message.reply_text("Сессия сброшена. Начни заново.", reply_markup=admin_menu_kb())
        return

    day = date.fromisoformat(day_iso)
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        services = await list_active_services(s)
        service = next((x for x in services if x.id == int(svc_id)), None)
        if not service:
            _clear_admin_booking(context)
            await update.effective_message.reply_text("Услуга недоступна.", reply_markup=admin_menu_kb())
            return
        duration_min = int(context.user_data.get(K_ADMIN_DURATION) or service.duration_min)
        slots = await list_available_slots_for_duration(s, settings, service, day, duration_min)

    context.user_data["awaiting_admin_time"] = True
    slots_hint = "Свободных слотов нет."
    if slots:
        slots_hint = "Свободные слоты: " + ", ".join(st.strftime("%H:%M") for st in slots[:12])
        if len(slots) > 12:
            slots_hint += " и ещё…"

    await update.effective_message.reply_text(
        "Введи время визита в формате HH:MM (например, 14:30).\n"
        f"Длительность: {duration_min} мин.\n"
        f"{slots_hint}\n"
        "Можно выбрать время кнопкой ниже.",
        reply_markup=admin_slots_kb(slots),
    )

async def _send_break_time_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.bot_data["cfg"]
    day_iso = context.user_data.get(K_BREAK_DATE)
    duration_min = context.user_data.get(K_BREAK_DURATION)
    if not day_iso or not duration_min:
        _clear_break(context)
        await update.effective_message.reply_text("Сессия сброшена. Начни заново.", reply_markup=admin_menu_kb())
        return

    day = date.fromisoformat(day_iso)
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        slots = await list_available_break_slots(s, settings, day, int(duration_min))

    context.user_data["awaiting_break_time"] = True
    slots_hint = "Свободных слотов нет."
    if slots:
        slots_hint = "Свободные слоты: " + ", ".join(st.strftime("%H:%M") for st in slots[:12])
        if len(slots) > 12:
            slots_hint += " и ещё…"

    await update.effective_message.reply_text(
        "Выбери время начала перерыва в формате HH:MM (например, 14:30).\n"
        f"Длительность: {int(duration_min)} мин.\n"
        f"{slots_hint}\n"
        "Можно выбрать время кнопкой ниже.",
        reply_markup=break_slots_kb(slots),
    )

async def flow_slots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_factory = context.bot_data["session_factory"]
    cfg: Config = context.bot_data["cfg"]
    svc_id = context.user_data.get(K_SVC)
    day_iso = context.user_data.get(K_DATE)
    if not svc_id or not day_iso:
        return await update.callback_query.message.edit_text("Сессия сброшена. Нажми «Записаться» заново.")
    day = date.fromisoformat(day_iso)

    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        services = await list_active_services(s)
        service = next((x for x in services if x.id == svc_id), None)
        if not service:
            return await update.callback_query.message.edit_text("Услуга недоступна.")
        slots = await list_available_slots_for_service(s, settings, service, day)

    if not slots:
        return await update.callback_query.message.edit_text("На эту дату нет свободных слотов. Выбери другую дату.")

    await update.callback_query.message.edit_text("Выбери время:", reply_markup=slots_kb(slots))

async def flow_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.message.edit_text(
        "Комментарий (при желании). Отправь «-», если без комментария."
    )
    context.user_data["awaiting_comment"] = True

async def prompt_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["awaiting_phone"] = True
    await update.effective_message.reply_text(
        "Теперь отправь телефон кнопкой 👇\n"
        "Если кнопки нет — нажми /start и снова «Записаться».",
        reply_markup=phone_request_kb(),
    )

async def handle_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_comment"):
        return

    context.user_data["awaiting_comment"] = False
    c = (update.message.text or "").strip()
    context.user_data[K_COMMENT] = None if c == "-" else c

    await prompt_phone(update, context)
    return


async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Получает телефон (через contact или текстом), сохраняет его и показывает подтверждение заявки.

    ВАЖНО: берём выбранную услугу/слот из тех же ключей user_data, которые заполняются
    на шагах выбора услуги/даты/времени: K_SVC ("svc_id") и K_SLOT ("slot_iso").
    """
    if not context.user_data.get("awaiting_phone"):
        return

    msg = update.message
    if not msg:
        return

    # 1) достаём телефон: контакт или текст (или пропуск)
    phone = None
    if msg.contact and msg.contact.phone_number:
        phone = msg.contact.phone_number
    else:
        txt = (msg.text or "").strip()
        ok = all(ch.isdigit() or ch in "+-() " for ch in txt) and any(ch.isdigit() for ch in txt)
        if ok:
            phone = txt

    if not phone:
        await msg.reply_text(
            "Не вижу номер телефона. Нажми кнопку «Отправить телефон» 👇"
        )
        return

    # нормализация
    if phone:
        phone = (phone or "").strip()
        for ch in [" ", "-", "(", ")", "\u00A0"]:
            phone = phone.replace(ch, "")

    cfg: Config = context.bot_data["cfg"]
    session_factory = context.bot_data["session_factory"]

    # 2) читаем данные флоу (услуга/слот/коммент)
    svc_id = context.user_data.get(K_SVC)
    slot_iso = context.user_data.get(K_SLOT)
    context.user_data[K_PHONE] = phone

    # 3) сохраняем телефон (если есть) + собираем сводку
    async with session_factory() as s:
        await upsert_user(
            s,
            tg_id=update.effective_user.id,
            username=update.effective_user.username,
            full_name=update.effective_user.full_name,
        )
        if phone:
            await set_user_phone(s, update.effective_user.id, phone)

        settings = await get_settings(s, cfg.timezone)

        # валидация: обязательно должны быть услуга и слот
        if not svc_id or not slot_iso:
            context.user_data["awaiting_phone"] = False
            await s.commit()
            prefix = "Телефон сохранён ✅\n"
            await msg.reply_text(
                f"{prefix}Но я не вижу выбранную услугу/время. Начни запись заново: /start → «Записаться».",
                reply_markup=main_menu_for(update, context),
            )
            return

        start_local = datetime.fromisoformat(slot_iso)

        services = await list_active_services(s)
        service = next((x for x in services if x.id == int(svc_id)), None)
        if not service:
            context.user_data["awaiting_phone"] = False
            await s.commit()
            prefix = "Телефон сохранён ✅\n"
            await msg.reply_text(
                f"{prefix}Выбранная услуга недоступна. Начни запись заново: /start → «Записаться».",
                reply_markup=main_menu_for(update, context),
            )
            return
        await s.commit()

    context.user_data["awaiting_phone"] = False
    price_label = format_price(service.price)
    local_dt = start_local.astimezone(settings.tz) if start_local.tzinfo else settings.tz.localize(start_local)
    await msg.reply_text(
        "Проверь, всё ли верно перед отправкой заявки:\n"
        f"Услуга: {service.name}\n"
        f"Дата/время: {local_dt.strftime('%d.%m %H:%M')}\n"
        f"Цена: {price_label}",
        reply_markup=confirm_request_kb(),
    )

async def handle_admin_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_admin_duration"):
        return
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        _clear_admin_booking(context)
        return await update.message.reply_text("Нет доступа.")

    txt = (update.message.text or "").strip().lower()
    if txt in {"отмена", "cancel", "/cancel"}:
        _clear_admin_booking(context)
        return await update.message.reply_text("Запись отменена.", reply_markup=admin_menu_kb())

    svc_id = context.user_data.get(K_ADMIN_SVC)
    day_iso = context.user_data.get(K_ADMIN_DATE)
    if not svc_id or not day_iso:
        _clear_admin_booking(context)
        return await update.message.reply_text("Сессия сброшена. Начни заново.", reply_markup=admin_menu_kb())

    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        services = await list_active_services(s)
        service = next((x for x in services if x.id == int(svc_id)), None)
        if not service:
            _clear_admin_booking(context)
            return await update.message.reply_text("Услуга недоступна.", reply_markup=admin_menu_kb())

    if txt in {"-", "стандарт", "стандартная"}:
        duration_min = int(service.duration_min)
    else:
        try:
            duration_min = int(txt)
        except ValueError:
            return await update.message.reply_text("Длительность должна быть числом. Введи количество минут.")
        if duration_min <= 0:
            return await update.message.reply_text("Длительность должна быть больше нуля. Введи количество минут.")

    context.user_data[K_ADMIN_DURATION] = duration_min
    context.user_data["awaiting_admin_duration"] = False
    await _admin_send_time_prompt(update, context)

async def handle_break_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_break_duration"):
        return
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        _clear_break(context)
        return await update.message.reply_text("Нет доступа.")

    text = (update.message.text or "").strip()
    if not text.isdigit():
        return await update.message.reply_text("Нужно число минут, например 30.")

    duration = int(text)
    if duration <= 0:
        return await update.message.reply_text("Длительность должна быть больше 0.")

    context.user_data[K_BREAK_DURATION] = duration
    context.user_data["awaiting_break_duration"] = False
    await _send_break_time_prompt(update, context)

async def admin_pick_time_from_slots(update: Update, context: ContextTypes.DEFAULT_TYPE, slot_iso: str):
    query = update.callback_query
    if not query:
        return
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        _clear_admin_booking(context)
        return await query.message.reply_text("Нет доступа.")

    try:
        start_local = datetime.fromisoformat(slot_iso)
    except ValueError:
        return await query.message.reply_text("Не удалось распознать время. Попробуй ещё раз.")

    svc_id = context.user_data.get(K_ADMIN_SVC)
    day_iso = context.user_data.get(K_ADMIN_DATE)
    if not svc_id or not day_iso:
        _clear_admin_booking(context)
        return await query.message.reply_text("Сессия сброшена. Начни заново.", reply_markup=admin_menu_kb())

    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        services = await list_active_services(s)
        service = next((x for x in services if x.id == int(svc_id)), None)
        if not service:
            _clear_admin_booking(context)
            return await query.message.reply_text("Услуга недоступна.", reply_markup=admin_menu_kb())

        if start_local.tzinfo is None:
            start_local = settings.tz.localize(start_local)
        duration_min = int(context.user_data.get(K_ADMIN_DURATION) or service.duration_min)
        end_local = compute_slot_end_for_duration(start_local, duration_min, service, settings)
        work_start_local = settings.tz.localize(datetime.combine(start_local.date(), settings.work_start))
        work_end_local = settings.tz.localize(datetime.combine(start_local.date(), settings.work_end))
        if start_local < work_start_local or end_local > work_end_local:
            return await query.message.reply_text(
                f"Время вне рабочего диапазона ({settings.work_start.strftime('%H:%M')}–{settings.work_end.strftime('%H:%M')})."
            )
        try:
            await check_slot_available_for_duration(s, settings, service, start_local, duration_min)
        except ValueError as e:
            code = str(e)
            if code == "SLOT_TAKEN":
                return await query.message.reply_text("Этот слот уже занят. Выбери другое время.")
            if code == "SLOT_BLOCKED":
                return await query.message.reply_text("Это время заблокировано. Выбери другое время.")
            raise

    context.user_data["awaiting_admin_time"] = False
    context.user_data[K_ADMIN_TIME] = start_local.isoformat()
    context.user_data.pop(K_ADMIN_TIME_ERRORS, None)
    context.user_data["awaiting_admin_client_name"] = True
    await query.message.reply_text("Введи имя клиента.")

async def handle_admin_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_admin_time"):
        return
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        _clear_admin_booking(context)
        return await update.message.reply_text("Нет доступа.")

    txt = (update.message.text or "").strip().lower()
    if txt in {"отмена", "cancel", "/cancel"}:
        _clear_admin_booking(context)
        return await update.message.reply_text("Запись отменена.", reply_markup=admin_menu_kb())

    svc_id = context.user_data.get(K_ADMIN_SVC)
    day_iso = context.user_data.get(K_ADMIN_DATE)
    if not svc_id or not day_iso:
        _clear_admin_booking(context)
        return await update.message.reply_text("Сессия сброшена. Начни заново.", reply_markup=admin_menu_kb())

    async def _maybe_abort_after_errors() -> bool:
        if _increment_admin_time_errors(context) >= 3:
            _clear_admin_booking(context)
            await update.message.reply_text(
                "Слишком много ошибок. Процесс записи сброшен.",
                reply_markup=main_menu_for(update, context),
            )
            return True
        return False

    try:
        hh, mm = txt.split(":")
        hh_i = int(hh)
        mm_i = int(mm)
        if not (0 <= hh_i <= 23 and 0 <= mm_i <= 59):
            raise ValueError
    except ValueError:
        if await _maybe_abort_after_errors():
            return
        return await update.message.reply_text("Неверный формат времени. Введи HH:MM, например 14:30.")

    day = date.fromisoformat(day_iso)
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        services = await list_active_services(s)
        service = next((x for x in services if x.id == int(svc_id)), None)
        if not service:
            _clear_admin_booking(context)
            return await update.message.reply_text("Услуга недоступна.", reply_markup=admin_menu_kb())

        start_local = settings.tz.localize(datetime.combine(day, time(hh_i, mm_i)))
        now_local = datetime.now(tz=settings.tz)
        if start_local < now_local:
            if await _maybe_abort_after_errors():
                return
            return await update.message.reply_text("Нельзя выбрать время в прошлом. Введи другое время.")

        work_start_local = settings.tz.localize(datetime.combine(day, settings.work_start))
        work_end_local = settings.tz.localize(datetime.combine(day, settings.work_end))
        duration_min = int(context.user_data.get(K_ADMIN_DURATION) or service.duration_min)
        end_local = compute_slot_end_for_duration(start_local, duration_min, service, settings)
        if start_local < work_start_local or end_local > work_end_local:
            if await _maybe_abort_after_errors():
                return
            return await update.message.reply_text(
                f"Время вне рабочего диапазона ({settings.work_start.strftime('%H:%M')}–{settings.work_end.strftime('%H:%M')})."
            )

        try:
            await check_slot_available_for_duration(s, settings, service, start_local, duration_min)
        except ValueError as e:
            code = str(e)
            if code == "SLOT_TAKEN":
                if await _maybe_abort_after_errors():
                    return
                return await update.message.reply_text("Этот слот уже занят. Введи другое время.")
            if code == "SLOT_BLOCKED":
                if await _maybe_abort_after_errors():
                    return
                return await update.message.reply_text("Это время заблокировано. Введи другое время.")
            raise

    context.user_data["awaiting_admin_time"] = False
    context.user_data[K_ADMIN_TIME] = start_local.isoformat()
    context.user_data.pop(K_ADMIN_TIME_ERRORS, None)
    context.user_data["awaiting_admin_client_name"] = True
    await update.message.reply_text("Введи имя клиента.")

async def admin_pick_break_time(update: Update, context: ContextTypes.DEFAULT_TYPE, slot_iso: str):
    query = update.callback_query
    if not query:
        return
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        _clear_break(context)
        return await query.message.reply_text("Нет доступа.")

    try:
        start_local = datetime.fromisoformat(slot_iso)
    except ValueError:
        return await query.message.reply_text("Не удалось распознать время. Попробуй ещё раз.")

    await _finalize_break(query.message, context, start_local)

async def handle_break_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_break_time"):
        return
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        _clear_break(context)
        return await update.message.reply_text("Нет доступа.")

    text = (update.message.text or "").strip()
    try:
        hh, mm = text.split(":")
        t = time(int(hh), int(mm))
    except ValueError:
        return await update.message.reply_text("Нужно время в формате HH:MM, например 14:30.")

    day_iso = context.user_data.get(K_BREAK_DATE)
    if not day_iso:
        _clear_break(context)
        return await update.message.reply_text("Сессия сброшена. Начни заново.", reply_markup=admin_menu_kb())

    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        day = date.fromisoformat(day_iso)
        start_local = settings.tz.localize(datetime.combine(day, t))
        duration_min = int(context.user_data.get(K_BREAK_DURATION, 0))
        if duration_min <= 0:
            _clear_break(context)
            return await update.message.reply_text("Сессия сброшена. Начни заново.", reply_markup=admin_menu_kb())
        slots = await list_available_break_slots(s, settings, day, duration_min)

    if start_local not in slots:
        errors = int(context.user_data.get(K_BREAK_TIME_ERRORS, 0)) + 1
        context.user_data[K_BREAK_TIME_ERRORS] = errors
        if errors >= 3:
            _clear_break(context)
            return await update.message.reply_text(
                "Слишком много ошибок. Начни заново.", reply_markup=admin_menu_kb()
            )
        return await update.message.reply_text("Этот слот недоступен. Выбери другое время.")

    await _finalize_break(update.message, context, start_local)

async def _finalize_break(message, context: ContextTypes.DEFAULT_TYPE, start_local: datetime) -> None:
    cfg: Config = context.bot_data["cfg"]
    day_iso = context.user_data.get(K_BREAK_DATE)
    duration_min = int(context.user_data.get(K_BREAK_DURATION, 0))
    if not day_iso or duration_min <= 0:
        _clear_break(context)
        await message.reply_text("Сессия сброшена. Начни заново.", reply_markup=admin_menu_kb())
        return

    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        async with s.begin():
            settings = await get_settings(s, cfg.timezone)
            try:
                await create_blocked_interval(
                    s,
                    settings,
                    start_local,
                    duration_min,
                    created_by_admin=message.from_user.id if message.from_user else admin_ids(cfg)[0],
                )
            except ValueError as e:
                code = str(e)
                if code == "SLOT_TAKEN":
                    await message.reply_text("Этот слот уже занят. Выбери другое время.")
                    return
                if code == "SLOT_BLOCKED":
                    await message.reply_text("Этот слот уже заблокирован. Выбери другое время.")
                    return
                raise

    _clear_break(context)
    end_local = start_local + timedelta(minutes=duration_min)
    await message.reply_text(
        f"Перерыв добавлен ✅\n"
        f"Дата: {start_local.strftime('%d.%m')}\n"
        f"Время: {start_local.strftime('%H:%M')}–{end_local.strftime('%H:%M')}",
        reply_markup=admin_menu_kb(),
    )

async def handle_admin_client_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_admin_client_name"):
        return
    name = (update.message.text or "").strip()
    if not name:
        return await update.message.reply_text("Имя не может быть пустым. Введи имя клиента.")
    context.user_data["awaiting_admin_client_name"] = False
    context.user_data[K_ADMIN_CLIENT_NAME] = name
    context.user_data["awaiting_admin_client_phone"] = True
    await update.message.reply_text("Введи телефон клиента или «-», если без телефона.")

async def handle_admin_client_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_admin_client_phone"):
        return
    txt = (update.message.text or "").strip()
    phone = None
    if txt not in {"-", "без телефона"}:
        cleaned = _normalize_phone(txt)
        if not cleaned or not any(ch.isdigit() for ch in cleaned):
            return await update.message.reply_text("Не вижу телефон. Введи номер или «-» для пропуска.")
        phone = cleaned
    context.user_data["awaiting_admin_client_phone"] = False
    context.user_data[K_ADMIN_CLIENT_PHONE] = phone
    context.user_data["awaiting_admin_client_tg"] = True
    await update.message.reply_text("Введи Telegram ID клиента или «-», если запись без Telegram.")

async def handle_admin_client_tg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_admin_client_tg"):
        return
    txt = (update.message.text or "").strip()
    tg_id = None
    if txt not in {"-", "нет", "без", "без telegram", "без телеграм"}:
        try:
            tg_id = int(txt)
        except ValueError:
            return await update.message.reply_text("Telegram ID должен быть числом. Введи число или «-».")
    if tg_id is None:
        tg_id = _generate_offline_tg_id()
    context.user_data["awaiting_admin_client_tg"] = False
    context.user_data[K_ADMIN_CLIENT_TGID] = tg_id
    context.user_data["awaiting_admin_price"] = True
    await update.message.reply_text("Введи цену услуги или «-», чтобы оставить стандартную.")

async def handle_admin_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_admin_price"):
        return
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        _clear_admin_booking(context)
        return await update.message.reply_text("Нет доступа.")
    txt = (update.message.text or "").strip()
    price_override = None
    if txt not in {"-", "стандарт", "стандартная"}:
        try:
            price_override = float(txt.replace(",", "."))
        except ValueError:
            return await update.message.reply_text("Цена должна быть числом. Введи цену или «-».")
        if price_override < 0:
            return await update.message.reply_text("Цена не может быть отрицательной. Введи цену или «-».")

    svc_id = context.user_data.get(K_ADMIN_SVC)
    day_iso = context.user_data.get(K_ADMIN_DATE)
    time_iso = context.user_data.get(K_ADMIN_TIME)
    duration_min = context.user_data.get(K_ADMIN_DURATION)
    client_name = context.user_data.get(K_ADMIN_CLIENT_NAME)
    client_phone = context.user_data.get(K_ADMIN_CLIENT_PHONE)
    client_tg_id = context.user_data.get(K_ADMIN_CLIENT_TGID)

    if not all([svc_id, day_iso, time_iso, client_name, client_tg_id]):
        _clear_admin_booking(context)
        return await update.message.reply_text("Сессия сброшена. Начни заново.", reply_markup=admin_menu_kb())

    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        async with s.begin():
            settings = await get_settings(s, cfg.timezone)
            services = await list_active_services(s)
            service = next((x for x in services if x.id == int(svc_id)), None)
            if not service:
                _clear_admin_booking(context)
                return await update.message.reply_text("Услуга недоступна.", reply_markup=admin_menu_kb())

            client = await upsert_user(s, client_tg_id, None, client_name)
            if client_phone:
                await set_user_phone(s, client_tg_id, client_phone)

            start_local = datetime.fromisoformat(time_iso)
            try:
                appt = await create_admin_appointment_with_duration(
                    s,
                    settings=settings,
                    client=client,
                    service=service,
                    start_local=start_local,
                    duration_min=int(duration_min or service.duration_min),
                    price_override=price_override,
                    admin_comment="Создано мастером",
                )
            except ValueError as e:
                code = str(e)
                if code == "SLOT_TAKEN":
                    return await update.message.reply_text("Этот слот уже занят. Начни запись заново.", reply_markup=admin_menu_kb())
                if code == "SLOT_BLOCKED":
                    return await update.message.reply_text("Этот слот заблокирован. Начни запись заново.", reply_markup=admin_menu_kb())
                raise
            except Exception as exc:
                logger.exception("Failed to create admin appointment: %s", exc)
                _clear_admin_booking(context)
                return await update.message.reply_text(
                    "Не удалось создать запись. Проверьте базу и попробуйте ещё раз.",
                    reply_markup=admin_menu_kb(),
                )

    _clear_admin_booking(context)
    price_label = format_price(price_override if price_override is not None else service.price)
    local_dt = appt.start_dt.astimezone(settings.tz)
    await update.message.reply_text(
        "Запись создана ✅\n"
        f"Клиент: {client_name}\n"
        f"Услуга: {service.name}\n"
        f"Дата/время: {local_dt.strftime('%d.%m %H:%M')}\n"
        f"Цена: {price_label}",
        reply_markup=admin_manage_appt_kb(appt.id),
    )

    if client_tg_id > 0:
        try:
            await context.bot.send_message(
                chat_id=client_tg_id,
                text=(
                    "✅ Мастер записал вас на услугу.\n"
                    f"{local_dt.strftime('%d.%m %H:%M')}\n"
                    f"Услуга: {service.name}\n"
                    f"Цена: {price_label}"
                )
            )
        except Exception:
            pass
    await update.message.reply_text("Админ-панель 👇", reply_markup=admin_menu_kb())

async def finalize_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    session_factory = context.bot_data["session_factory"]

    svc_id = context.user_data.get(K_SVC)
    slot_iso = context.user_data.get(K_SLOT)
    phone = context.user_data.get(K_PHONE)
    if not svc_id or not slot_iso:
        return await update.callback_query.message.edit_text("Сессия сброшена. Нажми «Записаться» заново.")

    start_local = datetime.fromisoformat(slot_iso)

    async with session_factory() as s:
        async with s.begin():
            settings = await get_settings(s, cfg.timezone)
            client = await upsert_user(s, update.effective_user.id, update.effective_user.username, update.effective_user.full_name)
            if phone:
                await set_user_phone(s, update.effective_user.id, phone)
            services = await list_active_services(s)
            service = next((x for x in services if x.id == svc_id), None)
            if not service:
                return await update.callback_query.message.edit_text("Услуга недоступна.")
            try:
                appt = await create_hold_appointment(s, settings, client, service, start_local, context.user_data.get(K_COMMENT))
            except ValueError as e:
                code = str(e)
                if code == "SLOT_TAKEN":
                    return await update.callback_query.message.edit_text("Этот слот уже занят. Выбери другое время.")
                if code == "SLOT_BLOCKED":
                    return await update.callback_query.message.edit_text("Этот слот заблокирован. Выбери другое время.")
                raise

            await notify_admins(
                context,
                cfg,
                text=(
                    f"🆕 Новая заявка (HOLD #{appt.id})\n"
                    f"Услуга: {service.name}\n"
                    f"Дата/время: {appt.start_dt.astimezone(settings.tz).strftime('%d.%m %H:%M')}\n"
                    f"Длительность: {int(service.duration_min)} мин (+буфер)\n"
                    f"Цена: {format_price(service.price)}\n\n"
                    f"Клиент: {update.effective_user.full_name} (@{update.effective_user.username})\n"
                    f"Телефон: {client.phone or '—'}\n"
                    f"Комментарий: {context.user_data.get(K_COMMENT) or '—'}\n\n"
                    f"Hold истекает: {appt.hold_expires_at.astimezone(settings.tz).strftime('%H:%M')}"
                ),
                reply_markup=admin_request_kb(appt.id),
            )

    for k in (K_SVC, K_DATE, K_SLOT, K_COMMENT, K_PHONE):
        context.user_data.pop(k, None)

    await update.callback_query.message.edit_text(
        "Заявка создана ✅\nСтатус: Ожидает подтверждения.\nЯ сообщу, когда мастер подтвердит запись."
    )

async def show_my_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        appts = await get_user_appointments(s, update.effective_user.id, limit=10)
    if not appts:
        await update.message.reply_text("У вас пока нет записей.", reply_markup=main_menu_for(update, context))
        return
    await update.message.reply_text("Ваши записи:", reply_markup=my_appts_kb(appts))

async def show_my_appointments_from_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        appts = await get_user_appointments(s, update.effective_user.id, limit=10)
    if not appts:
        return await update.callback_query.message.edit_text("У вас пока нет записей.")
    await update.callback_query.message.edit_text("Ваши записи:", reply_markup=my_appts_kb(appts))


async def show_my_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        appts = await get_user_appointments_history(s, update.effective_user.id, limit=10)
    if not appts:
        await update.message.reply_text("История пустая.", reply_markup=main_menu_for(update, context))
        return
    await update.message.reply_text("История:", reply_markup=my_appts_kb(appts))

async def show_my_history_from_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        appts = await get_user_appointments_history(s, update.effective_user.id, limit=10)
    if not appts:
        return await update.callback_query.message.edit_text("История пустая.")
    await update.callback_query.message.edit_text("История:", reply_markup=my_appts_kb(appts))

async def show_my_appointment_detail(update: Update, context: ContextTypes.DEFAULT_TYPE, appt_id: int):
    cfg: Config = context.bot_data["cfg"]
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        appt = await get_appointment(s, appt_id)

    proposed = ""
    if appt.proposed_alt_start_dt:
        proposed_dt = appt.proposed_alt_start_dt.astimezone(settings.tz).strftime('%d.%m %H:%M')
        proposed = f"\nЗапрос на перенос: {proposed_dt} (ожидает подтверждения)"

    price = format_price(appt.price_override if appt.price_override is not None else appt.service.price)
    txt = (
        "Запись\n"
        f"Статус: {status_ru(appt.status.value)}\n"
        f"Дата/время: {appt.start_dt.astimezone(settings.tz).strftime('%d.%m %H:%M')}\n"
        f"Услуга: {appt.service.name}\n"
        f"Цена: {price}\n"
        f"Комментарий: {appt.client_comment or '—'}"
        f"{proposed}"
    )
    kb = my_appt_actions_kb(appt.id) if appt.status == AppointmentStatus.Booked else None
    await update.callback_query.message.edit_text(txt, reply_markup=kb)

async def client_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE, appt_id: int):
    cfg: Config = context.bot_data["cfg"]
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        async with s.begin():
            settings = await get_settings(s, cfg.timezone)
            appt = await get_appointment(s, appt_id)
            ok = await cancel_by_client(s, settings, appt)
            if not ok:
                return await update.callback_query.message.edit_text(
                    f"Отмена недоступна менее чем за {settings.cancel_limit_hours} часов. Напишите мастеру напрямую."
                )
            await notify_admins(
                context,
                cfg,
                text=(
                    "🚫 Клиент отменил запись "
                    f"#{appt.id} на {appt.start_dt.astimezone(settings.tz).strftime('%d.%m %H:%M')}"
                ),
            )
    await update.callback_query.message.edit_text("Запись отменена ✅")

async def start_reschedule(update: Update, context: ContextTypes.DEFAULT_TYPE, appt_id: int):
    cfg: Config = context.bot_data["cfg"]
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        async with s.begin():
            settings = await get_settings(s, cfg.timezone)
            appt = await get_appointment(s, appt_id)
            if appt.client.tg_id != update.effective_user.id:
                return await update.callback_query.message.edit_text("Нет доступа.")
            if appt.status != AppointmentStatus.Booked:
                return await update.callback_query.message.edit_text("Перенос доступен только для подтверждённых записей.")
            now_utc = datetime.now(tz=pytz.UTC)
            if now_utc > (appt.start_dt - timedelta(hours=settings.cancel_limit_hours)):
                return await update.callback_query.message.edit_text("До визита осталось слишком мало времени. Для переноса свяжитесь напрямую.")

    context.user_data[K_RESCHED_APPT] = appt_id
    context.user_data[K_RESCHED_SVC] = appt.service_id
    return await flow_reschedule_dates(update, context)

async def flow_reschedule_dates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_factory = context.bot_data["session_factory"]
    cfg: Config = context.bot_data["cfg"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        dates = await list_available_dates(s, settings)
    await update.callback_query.message.edit_text("Выбери новую дату для переноса:", reply_markup=reschedule_dates_kb(dates))

async def flow_reschedule_slots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_factory = context.bot_data["session_factory"]
    cfg: Config = context.bot_data["cfg"]
    svc_id = context.user_data.get(K_RESCHED_SVC)
    day_iso = context.user_data.get(K_RESCHED_DATE) or context.user_data.get(K_DATE)
    if not svc_id or not day_iso:
        return await update.callback_query.message.edit_text("Сессия сброшена. Нажми «Мои записи» и начни перенос заново.")
    day = date.fromisoformat(day_iso)

    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        services = await list_active_services(s)
        service = next((x for x in services if x.id == svc_id), None)
        if not service:
            return await update.callback_query.message.edit_text("Услуга недоступна.")
        slots = await list_available_slots_for_service(s, settings, service, day)

    if not slots:
        return await update.callback_query.message.edit_text("На эту дату нет свободных слотов. Выбери другую дату.")

    await update.callback_query.message.edit_text("Выбери новое время:", reply_markup=reschedule_slots_kb(slots))

async def confirm_reschedule_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_factory = context.bot_data["session_factory"]
    cfg: Config = context.bot_data["cfg"]
    appt_id = context.user_data.get(K_RESCHED_APPT)
    slot_iso = context.user_data.get(K_RESCHED_SLOT)
    if not appt_id or not slot_iso:
        return await update.callback_query.message.edit_text("Сессия сброшена. Нажми «Мои записи» и начни перенос заново.")

    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        appt = await get_appointment(s, appt_id)

    new_dt = datetime.fromisoformat(slot_iso).astimezone(settings.tz).strftime('%d.%m %H:%M')
    old_dt = appt.start_dt.astimezone(settings.tz).strftime('%d.%m %H:%M')
    await update.callback_query.message.edit_text(
        f"Запросить перенос записи?\nТекущее время: {old_dt}\nНовое время: {new_dt}",
        reply_markup=reschedule_confirm_kb()
    )

async def finalize_reschedule_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    session_factory = context.bot_data["session_factory"]
    appt_id = context.user_data.get(K_RESCHED_APPT)
    slot_iso = context.user_data.get(K_RESCHED_SLOT)
    if not appt_id or not slot_iso:
        return await update.callback_query.message.edit_text("Сессия сброшена. Нажми «Мои записи» и начни перенос заново.")

    async with session_factory() as s:
        async with s.begin():
            settings = await get_settings(s, cfg.timezone)
            appt = await get_appointment(s, appt_id)
            if appt.client.tg_id != update.effective_user.id:
                return await update.callback_query.message.edit_text("Нет доступа.")
            try:
                await request_reschedule(s, settings, appt, datetime.fromisoformat(slot_iso))
            except ValueError as e:
                code = str(e)
                if code == "SLOT_TAKEN":
                    return await update.callback_query.message.edit_text("Этот слот уже занят. Выбери другое время.")
                if code == "SLOT_BLOCKED":
                    return await update.callback_query.message.edit_text("Это время заблокировано. Выбери другое.")
                return await update.callback_query.message.edit_text("Не удалось отправить запрос на перенос. Попробуй ещё раз.")

            new_local = appt.proposed_alt_start_dt.astimezone(settings.tz)
            old_local = appt.start_dt.astimezone(settings.tz)

            await notify_admins(
                context,
                cfg,
                text=(
                    "🔄 Запрос на перенос записи\n"
                    f"#{appt.id}\n"
                    f"Услуга: {appt.service.name}\n"
                    f"Текущее время: {old_local.strftime('%d.%m %H:%M')}\n"
                    f"Новое время: {new_local.strftime('%d.%m %H:%M')}\n"
                    f"Клиент: {appt.client.full_name or appt.client.tg_id}\n"
                    f"Телефон: {appt.client.phone or '—'}"
                ),
                reply_markup=admin_reschedule_kb(appt.id),
            )

    for k in (K_RESCHED_APPT, K_RESCHED_SVC, K_RESCHED_DATE, K_RESCHED_SLOT, K_DATE, K_SLOT):
        context.user_data.pop(k, None)

    await update.callback_query.message.edit_text(
        "Запрос на перенос отправлен ✅\nОжидай подтверждения мастера."
    )

async def admin_action_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, appt_id: int):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        return await update.callback_query.message.edit_text("Нет доступа.")
    session_factory = context.bot_data["session_factory"]

    async with session_factory() as s:
        async with s.begin():
            settings = await get_settings(s, cfg.timezone)
            appt = await get_appointment(s, appt_id)
            await admin_confirm(s, appt)

            await context.bot.send_message(
                chat_id=appt.client.tg_id,
                text=(
                    f"✅ Запись подтверждена!\n"
                    f"{appt.start_dt.astimezone(settings.tz).strftime('%d.%m %H:%M')}\n"
                    f"Услуга: {appt.service.name}\n"
                    f"Адриана ждет Вас!\n\n"
                ),
            )
            await asyncio.sleep(5)
            for part in PRECARE_RECOMMENDATIONS_PARTS:
                await context.bot.send_message(
                    chat_id=appt.client.tg_id,
                    text=part,
                )
    await update.callback_query.message.edit_text("Подтверждено ✅")

async def admin_action_reject(update: Update, context: ContextTypes.DEFAULT_TYPE, appt_id: int):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        return await update.callback_query.message.edit_text("Нет доступа.")
    session_factory = context.bot_data["session_factory"]

    async with session_factory() as s:
        async with s.begin():
            settings = await get_settings(s, cfg.timezone)
            appt = await get_appointment(s, appt_id)
            await admin_reject(s, appt, reason="Отклонено мастером")

            await context.bot.send_message(
                chat_id=appt.client.tg_id,
                text=(
                    f"❌ Запись отклонена.\n"
                    f"Слот: {appt.start_dt.astimezone(settings.tz).strftime('%d.%m %H:%M')}\n"
                    f"Попробуйте выбрать другое время."
                )
            )
    await update.callback_query.message.edit_text("Отклонено ❌")

def _is_admin_created(appt) -> bool:
    return (appt.admin_comment or "").strip().lower() == "создано мастером"

async def admin_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE, appt_id: int):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        return await update.callback_query.message.edit_text("Нет доступа.")
    session_factory = context.bot_data["session_factory"]

    async with session_factory() as s:
        async with s.begin():
            settings = await get_settings(s, cfg.timezone)
            appt = await get_appointment(s, appt_id)
            ok = await admin_cancel_appointment(s, appt)
            if not ok:
                return await update.callback_query.message.edit_text("Отменить можно только подтверждённую запись.")
            appt_local = appt.start_dt.astimezone(settings.tz).strftime('%d.%m %H:%M')
            if appt.client.tg_id > 0:
                try:
                    await context.bot.send_message(
                        chat_id=appt.client.tg_id,
                        text=(
                            "🚫 Мастер отменил вашу запись.\n"
                            f"Дата/время: {appt_local}\n"
                            f"Услуга: {appt.service.name}"
                        )
                    )
                except Exception:
                    pass
    await update.callback_query.message.edit_text("Запись отменена ✅")

async def admin_start_reschedule(update: Update, context: ContextTypes.DEFAULT_TYPE, appt_id: int):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        return await update.callback_query.message.edit_text("Нет доступа.")
    session_factory = context.bot_data["session_factory"]

    async with session_factory() as s:
        async with s.begin():
            appt = await get_appointment(s, appt_id)
            if not _is_admin_created(appt):
                return await update.callback_query.message.edit_text(
                    "Перенос доступен только для записей, созданных мастером."
                )
            if appt.status != AppointmentStatus.Booked:
                return await update.callback_query.message.edit_text("Перенос доступен только для подтверждённых записей.")

    _clear_admin_reschedule(context)
    context.user_data[K_ADMIN_RESCHED_APPT] = appt_id
    context.user_data[K_ADMIN_RESCHED_SVC] = appt.service_id
    return await admin_flow_reschedule_dates(update, context)

async def admin_flow_reschedule_dates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_factory = context.bot_data["session_factory"]
    cfg: Config = context.bot_data["cfg"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        dates = await list_available_dates(s, settings)
    await update.callback_query.message.edit_text(
        "Выбери новую дату для переноса:",
        reply_markup=admin_reschedule_dates_kb(dates),
    )

async def admin_flow_reschedule_slots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_factory = context.bot_data["session_factory"]
    cfg: Config = context.bot_data["cfg"]
    svc_id = context.user_data.get(K_ADMIN_RESCHED_SVC)
    day_iso = context.user_data.get(K_ADMIN_RESCHED_DATE)
    if not svc_id or not day_iso:
        return await update.callback_query.message.edit_text("Сессия сброшена. Начни перенос заново.")
    day = date.fromisoformat(day_iso)

    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        services = await list_active_services(s)
        service = next((x for x in services if x.id == svc_id), None)
        if not service:
            return await update.callback_query.message.edit_text("Услуга недоступна.")
        slots = await list_available_slots_for_service(s, settings, service, day)

    if not slots:
        return await update.callback_query.message.edit_text("На эту дату нет свободных слотов. Выбери другую дату.")

    await update.callback_query.message.edit_text(
        "Выбери новое время:",
        reply_markup=admin_reschedule_slots_kb(slots),
    )

async def admin_confirm_reschedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_factory = context.bot_data["session_factory"]
    cfg: Config = context.bot_data["cfg"]
    appt_id = context.user_data.get(K_ADMIN_RESCHED_APPT)
    slot_iso = context.user_data.get(K_ADMIN_RESCHED_SLOT)
    if not appt_id or not slot_iso:
        return await update.callback_query.message.edit_text("Сессия сброшена. Начни перенос заново.")

    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        appt = await get_appointment(s, appt_id)

    new_dt = datetime.fromisoformat(slot_iso).astimezone(settings.tz).strftime('%d.%m %H:%M')
    old_dt = appt.start_dt.astimezone(settings.tz).strftime('%d.%m %H:%M')
    await update.callback_query.message.edit_text(
        f"Перенести запись?\nТекущее время: {old_dt}\nНовое время: {new_dt}",
        reply_markup=admin_reschedule_confirm_kb(),
    )

async def admin_finalize_reschedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        return await update.callback_query.message.edit_text("Нет доступа.")
    session_factory = context.bot_data["session_factory"]
    appt_id = context.user_data.get(K_ADMIN_RESCHED_APPT)
    slot_iso = context.user_data.get(K_ADMIN_RESCHED_SLOT)
    if not appt_id or not slot_iso:
        return await update.callback_query.message.edit_text("Сессия сброшена. Начни перенос заново.")

    async with session_factory() as s:
        async with s.begin():
            settings = await get_settings(s, cfg.timezone)
            appt = await get_appointment(s, appt_id)
            if not _is_admin_created(appt):
                return await update.callback_query.message.edit_text(
                    "Перенос доступен только для записей, созданных мастером."
                )
            if appt.status != AppointmentStatus.Booked:
                return await update.callback_query.message.edit_text("Перенос доступен только для подтверждённых записей.")
            new_start = datetime.fromisoformat(slot_iso)
            now_local = datetime.now(tz=settings.tz)
            if new_start < now_local:
                return await update.callback_query.message.edit_text("Нельзя перенести запись на время в прошлом.")
            try:
                await admin_reschedule_appointment(s, settings, appt, new_start)
            except ValueError as e:
                code = str(e)
                if code == "SLOT_TAKEN":
                    return await update.callback_query.message.edit_text("Слот уже занят. Выбери другое время.")
                if code == "SLOT_BLOCKED":
                    return await update.callback_query.message.edit_text("Слот заблокирован. Выбери другое время.")
                return await update.callback_query.message.edit_text("Не удалось перенести запись.")

            new_local = appt.start_dt.astimezone(settings.tz).strftime('%d.%m %H:%M')
            if appt.client.tg_id > 0:
                try:
                    await context.bot.send_message(
                        chat_id=appt.client.tg_id,
                        text=(
                            "🔄 Мастер перенёс вашу запись.\n"
                            f"Новая дата/время: {new_local}\n"
                            f"Услуга: {appt.service.name}"
                        )
                    )
                except Exception:
                    pass

    _clear_admin_reschedule(context)
    await update.callback_query.message.edit_text("Запись перенесена ✅")

async def admin_reschedule_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, appt_id: int):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        return await update.callback_query.message.edit_text("Нет доступа.")
    session_factory = context.bot_data["session_factory"]

    async with session_factory() as s:
        async with s.begin():
            settings = await get_settings(s, cfg.timezone)
            appt = await get_appointment(s, appt_id)
            if not appt.proposed_alt_start_dt:
                return await update.callback_query.message.edit_text("Запрос на перенос не найден.")
            try:
                await confirm_reschedule(s, settings, appt)
            except ValueError as e:
                code = str(e)
                if code == "SLOT_TAKEN":
                    return await update.callback_query.message.edit_text("Слот уже занят. Запрос не подтверждён.")
                if code == "SLOT_BLOCKED":
                    return await update.callback_query.message.edit_text("Слот заблокирован. Запрос не подтверждён.")
                return await update.callback_query.message.edit_text("Не удалось подтвердить перенос.")

            new_local = appt.start_dt.astimezone(settings.tz).strftime('%d.%m %H:%M')
            await context.bot.send_message(
                chat_id=appt.client.tg_id,
                text=(
                    "✅ Перенос подтверждён!\n"
                    f"Новая дата/время: {new_local}\n"
                    f"Услуга: {appt.service.name}"
                )
            )
    await update.callback_query.message.edit_text("Перенос подтверждён ✅")

async def admin_reschedule_reject(update: Update, context: ContextTypes.DEFAULT_TYPE, appt_id: int):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        return await update.callback_query.message.edit_text("Нет доступа.")
    session_factory = context.bot_data["session_factory"]

    async with session_factory() as s:
        async with s.begin():
            appt = await get_appointment(s, appt_id)
            if not appt.proposed_alt_start_dt:
                return await update.callback_query.message.edit_text("Запрос на перенос не найден.")
            await reject_reschedule(s, appt)
            await context.bot.send_message(
                chat_id=appt.client.tg_id,
                text=(
                    "❌ Перенос отклонён мастером.\n"
                    "Запись остаётся в исходное время."
                )
            )
    await update.callback_query.message.edit_text("Перенос отклонён ❌")

async def admin_action_msg(update: Update, context: ContextTypes.DEFAULT_TYPE, appt_id: int):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        return await update.callback_query.message.edit_text("Нет доступа.")
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        appt = await get_appointment(s, appt_id)
    await update.callback_query.message.edit_text(
        f"TG ID клиента: {appt.client.tg_id}\n@{appt.client.username or '—'}",
        reply_markup=admin_request_kb(appt_id)
    )

async def reminder_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, appt_id: int):
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        async with s.begin():
            appt = await get_appointment(s, appt_id)
            appt.visit_confirmed = True
            appt.updated_at = datetime.now(tz=pytz.UTC)
    await update.callback_query.message.edit_text("Отлично, визит подтверждён ✅")

async def reminder_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE, appt_id: int):
    return await client_cancel(update, context, appt_id)

def _slot_status_for_time(
    slot_start_local: datetime,
    spans: list[tuple[datetime, datetime, AppointmentStatus]],
) -> AppointmentStatus | None:
    has_hold = False
    for start_local, end_local, status in spans:
        if start_local <= slot_start_local < end_local:
            if status == AppointmentStatus.Booked:
                return AppointmentStatus.Booked
            if status == AppointmentStatus.Hold:
                has_hold = True
    return AppointmentStatus.Hold if has_hold else None

def _build_day_timeline(
    day: date,
    settings: SettingsView,
    appts: list,
    slots_per_line: int = 6,
) -> str:
    work_start_local = settings.tz.localize(datetime.combine(day, settings.work_start))
    work_end_local = settings.tz.localize(datetime.combine(day, settings.work_end))
    step = timedelta(minutes=settings.slot_step_min)
    spans = [
        (a.start_dt.astimezone(settings.tz), a.end_dt.astimezone(settings.tz), a.status)
        for a in appts
    ]

    def slot_symbol(status: AppointmentStatus | None) -> str:
        if status == AppointmentStatus.Booked:
            return "🟥"
        if status == AppointmentStatus.Hold:
            return "🟨"
        return "🟩"

    slots: list[str] = []
    cursor = work_start_local
    while cursor < work_end_local:
        status = _slot_status_for_time(cursor, spans)
        slots.append(f"{cursor.strftime('%H:%M')}")
        cursor += step

    status_symbols = []
    cursor = work_start_local
    while cursor < work_end_local:
        status = _slot_status_for_time(cursor, spans)
        status_symbols.append(slot_symbol(status))
        cursor += step

    lines = ["🧭 График слотов:"]
    time_row: list[str] = []
    symbol_row: list[str] = []
    for time_label, symbol in zip(slots, status_symbols):
        time_row.append(time_label)
        symbol_row.append(symbol.center(5))
        if len(time_row) >= slots_per_line:
            lines.append(" ".join(time_row))
            lines.append(" ".join(symbol_row))
            time_row = []
            symbol_row = []
    if time_row:
        lines.append(" ".join(time_row))
        lines.append(" ".join(symbol_row))
    lines.append("Легенда: 🟩 свободно • 🟥 подтверждено • 🟨 ожидает подтверждения")
    return "\n".join(lines)

async def admin_day_view(update: Update, context: ContextTypes.DEFAULT_TYPE, offset_days: int):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        return await update.message.reply_text("Нет доступа.")

    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        day = (datetime.now(tz=settings.tz) + timedelta(days=offset_days)).date()
        appts = await admin_list_appointments_for_day(s, settings.tz, day)
        start_local = settings.tz.localize(datetime.combine(day, datetime.min.time()))
        end_local = start_local + timedelta(days=1)
        breaks = await list_future_breaks(
            s,
            start_local.astimezone(pytz.UTC),
            end_local.astimezone(pytz.UTC),
        )

    lines = [f"📅 Записи на {day.strftime('%d.%m')} ({RU_WEEKDAYS[day.weekday()]}):"]
    if not appts:
        lines.append("• Записей нет.")
    else:
        for a in appts:
            start_t = a.start_dt.astimezone(settings.tz).strftime("%H:%M")
            end_t = a.end_dt.astimezone(settings.tz).strftime("%H:%M")
            client = a.client.full_name or (f"@{a.client.username}" if a.client.username else str(a.client.tg_id))
            phone = a.client.phone or "—"
            price = format_price(a.price_override if a.price_override is not None else a.service.price)
            lines.append(
                f"• {start_t}–{end_t} | {status_ru(a.status.value)} | {a.service.name} | {price} | {client} | {phone}"
            )

    if breaks:
        lines.append("• Перерывы:")
        for b in breaks:
            start_t = b.start_dt.astimezone(settings.tz).strftime("%H:%M")
            end_t = b.end_dt.astimezone(settings.tz).strftime("%H:%M")
            reason = b.reason or "Перерыв"
            lines.append(f"  - {start_t}–{end_t} | {reason}")

    await update.message.reply_text("\n".join(lines), reply_markup=admin_menu_kb())
    timeline = _build_day_timeline(day, settings, appts)
    await update.message.reply_text(f"<code>{timeline}</code>", reply_markup=admin_menu_kb(), parse_mode="HTML")
    for a in appts:
        if a.status == AppointmentStatus.Booked:
            start_t = a.start_dt.astimezone(settings.tz).strftime("%H:%M")
            await update.message.reply_text(
                f"Запись • {start_t} • {a.service.name}",
                reply_markup=admin_manage_appt_kb(a.id, allow_reschedule=_is_admin_created(a)),
            )

async def admin_booked_month_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        return await update.message.reply_text("Нет доступа.")

    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        now_local = datetime.now(tz=settings.tz)
        end_local = now_local + timedelta(days=30)
        appts = await admin_list_booked_range(
            s,
            now_local.astimezone(pytz.UTC),
            end_local.astimezone(pytz.UTC),
        )

    if not appts:
        return await update.message.reply_text(
            "На ближайший месяц подтверждённых записей нет.",
            reply_markup=admin_menu_kb()
        )

    lines = ["🗓 Все подтверждённые записи на месяц вперёд:"]
    for a in appts:
        local_dt = a.start_dt.astimezone(settings.tz)
        end_dt = a.end_dt.astimezone(settings.tz)
        day_label = f"{local_dt.strftime('%d.%m')} ({RU_WEEKDAYS[local_dt.weekday()]})"
        client = a.client.full_name or (f"@{a.client.username}" if a.client.username else str(a.client.tg_id))
        phone = a.client.phone or "—"
        price = format_price(a.price_override if a.price_override is not None else a.service.price)
        lines.append(
            f"• {day_label} {local_dt.strftime('%H:%M')}–{end_dt.strftime('%H:%M')} | {a.service.name} | {price} | {client} | {phone}"
        )

    await update.message.reply_text("\n".join(lines), reply_markup=admin_menu_kb())

async def admin_cancel_break_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        return await update.message.reply_text("Нет доступа.")

    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        now_local = datetime.now(tz=settings.tz)
        end_local = now_local + timedelta(days=30)
        blocks = await list_future_breaks(
            s,
            now_local.astimezone(pytz.UTC),
            end_local.astimezone(pytz.UTC),
        )

    if not blocks:
        return await update.message.reply_text("Перерывы не найдены.", reply_markup=admin_menu_kb())

    items = [
        (b.id, b.start_dt.astimezone(settings.tz), b.end_dt.astimezone(settings.tz))
        for b in blocks
    ]
    await update.message.reply_text(
        "Выберите перерыв для отмены:",
        reply_markup=cancel_breaks_kb(items),
    )

async def admin_cancel_break(update: Update, context: ContextTypes.DEFAULT_TYPE, block_id: int):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        return await update.callback_query.message.edit_text("Нет доступа.")

    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        async with s.begin():
            ok = await delete_blocked_interval(s, block_id)

    if not ok:
        return await update.callback_query.message.edit_text("Перерыв уже отменён или не найден.")

    await update.callback_query.message.edit_text("Перерыв отменён ✅")
    await update.callback_query.message.reply_text("Админ-панель 👇", reply_markup=admin_menu_kb())


async def admin_holds_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        return await update.message.reply_text("Нет доступа.")

    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        holds = await admin_list_holds(s)

    if not holds:
        return await update.message.reply_text("HOLD-заявок нет.", reply_markup=admin_menu_kb())

    lines = ["🧾 HOLD-заявки:"]
    for a in holds:
        t = a.start_dt.astimezone(settings.tz).strftime("%d.%m %H:%M")
        exp = a.hold_expires_at.astimezone(settings.tz).strftime("%H:%M") if a.hold_expires_at else "—"
        client = a.client.full_name or (f"@{a.client.username}" if a.client.username else str(a.client.tg_id))
        lines.append(f"• {t} | #{a.id} | {a.service.name} | {client} | hold до {exp}")

    await update.message.reply_text("\n".join(lines), reply_markup=admin_menu_kb())
