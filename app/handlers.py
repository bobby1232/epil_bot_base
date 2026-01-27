from __future__ import annotations
from datetime import datetime, date, timedelta
import pytz

from telegram import Update
from telegram.ext import ContextTypes

from app.config import Config
from app.logic import (
    get_settings, upsert_user, set_user_phone, list_active_services, list_available_dates,
    list_available_slots_for_service, create_hold_appointment, get_user_appointments,
    get_user_appointments_history, get_appointment, admin_confirm, admin_reject,
    cancel_by_client,
    admin_list_appointments_for_day, admin_list_holds
)
from app.keyboards import (
    main_menu_kb, phone_request_kb, services_kb, dates_kb, slots_kb, confirm_request_kb,
    admin_request_kb, my_appts_kb, my_appt_actions_kb, reminder_kb, admin_menu_kb
)
from app.models import AppointmentStatus

K_SVC = "svc_id"
K_DATE = "date"
K_SLOT = "slot_iso"
K_COMMENT = "comment"

def is_admin(cfg: Config, user_id: int) -> bool:
    return user_id == cfg.admin_telegram_id

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        async with s.begin():
            await upsert_user(s, update.effective_user.id, update.effective_user.username, update.effective_user.full_name)
    await update.message.reply_text(
        "Привет! Я помогу записаться на эпиляцию. Выбирай действие в меню 👇",
        reply_markup=main_menu_kb()
    )
    if is_admin(cfg, update.effective_user.id):
        await update.message.reply_text("Админ-панель 👇", reply_markup=admin_menu_kb())

async def unified_text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        if txt == "⬅️ В главное меню":
            await update.message.reply_text("Главное меню 👇", reply_markup=main_menu_kb())
            return

    await update.message.reply_text("Используй кнопки меню 👇", reply_markup=main_menu_kb())

async def show_prices(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        services = await list_active_services(s)
    if not services:
        await update.message.reply_text("Пока нет услуг. Напиши мастеру.", reply_markup=main_menu_kb())
        return
    lines = ["Прайс-лист:"]
    for sv in services:
        lines.append(f"• {sv.name}: {sv.price} / {int(sv.duration_min)} мин")
    await update.message.reply_text("\n".join(lines), reply_markup=main_menu_kb())

async def show_contacts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Адрес / Контакты:\n— (заполни текстом позже)\n"
        "Если нужно — нажми «Задать вопрос».",
        reply_markup=main_menu_kb()
    )

async def ask_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Напиши вопрос одним сообщением — я перешлю мастеру.", reply_markup=main_menu_kb())
    context.user_data["awaiting_question"] = True

async def handle_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    if not context.user_data.get("awaiting_question"):
        return
    context.user_data["awaiting_question"] = False
    q = update.message.text.strip()
    user = update.effective_user
    await context.bot.send_message(
        chat_id=cfg.admin_telegram_id,
        text=f"❓ Вопрос от клиента:\nИмя: {user.full_name}\n@{user.username}\nTG ID: {user.id}\n\n{q}"
    )
    await update.message.reply_text("Отправлено ✅ Мастер ответит вам в Telegram.", reply_markup=main_menu_kb())

async def flow_services(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        services = await list_active_services(s)
    if not services:
        await update.message.reply_text("Услуги пока не настроены. Напишите мастеру.", reply_markup=main_menu_kb())
        return
    await update.message.reply_text("Выбери услугу:", reply_markup=services_kb(services))

async def cb_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    if data.startswith("svc:"):
        context.user_data[K_SVC] = int(data.split(":")[1])
        return await flow_dates(update, context)

    if data.startswith("date:"):
        context.user_data[K_DATE] = data.split(":")[1]
        return await flow_slots(update, context)

    if data.startswith("slot:"):
        context.user_data[K_SLOT] = data.split("slot:")[1]
        return await flow_comment(update, context)

    if data == "req:send":
        return await finalize_request(update, context)

    if data.startswith("adm:confirm:"):
        appt_id = int(data.split(":")[2])
        return await admin_action_confirm(update, context, appt_id)

    if data.startswith("adm:reject:"):
        appt_id = int(data.split(":")[2])
        return await admin_action_reject(update, context, appt_id)

    if data.startswith("adm:msg:"):
        appt_id = int(data.split(":")[2])
        return await admin_action_msg(update, context, appt_id)

    if data == "back:main":
        await query.message.reply_text("Главное меню 👇", reply_markup=main_menu_kb())
        return

    if data == "back:services":
        return await flow_services_from_callback(update, context)

    if data == "back:dates":
        return await flow_dates(update, context)

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
        "Комментарий (опционально). Отправь «-», если без комментария."
    )
    context.user_data["awaiting_comment"] = True

async def handle_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_comment"):
        return

    context.user_data["awaiting_comment"] = False
    c = (update.message.text or "").strip()
    context.user_data[K_COMMENT] = None if c == "-" else c

    # Переключаемся в ожидание телефона
    context.user_data["awaiting_phone"] = True

    await update.message.reply_text(
        "Теперь отправь телефон кнопкой 👇\n"
        "Или нажми «Пропустить телефон», если не хочешь оставлять номер.\n"
        "Если кнопки нет — нажми /start и снова «Записаться».",
        reply_markup=phone_request_kb()
    )
    return


async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Получает телефон (через contact или текстом), сохраняет его и создаёт HOLD-заявку.

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
    skip_phone = False
    if msg.contact and msg.contact.phone_number:
        phone = msg.contact.phone_number
    else:
        txt = (msg.text or "").strip()
        normalized = txt.lower()
        if normalized in {"-", "пропустить", "пропустить телефон", "без телефона", "⏭️ пропустить телефон"}:
            skip_phone = True
        else:
            ok = all(ch.isdigit() or ch in "+-() " for ch in txt) and any(ch.isdigit() for ch in txt)
            if ok:
                phone = txt

    if not phone and not skip_phone:
        await msg.reply_text(
            "Не вижу номер телефона. Нажми кнопку «Отправить телефон» или «Пропустить телефон» 👇"
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
    comment = context.user_data.get(K_COMMENT)

    # 3) сохраняем телефон (если есть) + создаём заявку
    async with session_factory() as s:
        # гарантируем пользователя
        client = await upsert_user(
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
            prefix = "Телефон сохранён ✅\n" if phone else ""
            await msg.reply_text(
                f"{prefix}Но я не вижу выбранную услугу/время. Начни запись заново: /start → «Записаться».",
                reply_markup=main_menu_kb(),
            )
            return

        start_local = datetime.fromisoformat(slot_iso)

        services = await list_active_services(s)
        service = next((x for x in services if x.id == int(svc_id)), None)
        if not service:
            context.user_data["awaiting_phone"] = False
            await s.commit()
            prefix = "Телефон сохранён ✅\n" if phone else ""
            await msg.reply_text(
                f"{prefix}Выбранная услуга недоступна. Начни запись заново: /start → «Записаться».",
                reply_markup=main_menu_kb(),
            )
            return

        try:
            appt = await create_hold_appointment(
                s,
                settings=settings,
                client=client,
                service=service,
                start_local=start_local,
                comment=comment,
            )
            await s.commit()
        except ValueError as e:
            await s.rollback()
            context.user_data["awaiting_phone"] = False
            code = str(e)
            if code == "SLOT_TAKEN":
                await msg.reply_text(
                    "Этот слот уже заняли. Пожалуйста выбери другое время: /start → «Записаться».",
                    reply_markup=main_menu_kb(),
                )
            elif code == "SLOT_BLOCKED":
                await msg.reply_text(
                    "Это время заблокировано. Пожалуйста выбери другое: /start → «Записаться».",
                    reply_markup=main_menu_kb(),
                )
            else:
                await msg.reply_text("Не удалось создать запись. Попробуй ещё раз: /start", reply_markup=main_menu_kb())
            return

    # 4) флоу завершён: снимаем флаг и чистим временные поля
    context.user_data["awaiting_phone"] = False
    for k in [K_SVC, K_DATE, K_SLOT, K_COMMENT]:
        context.user_data.pop(k, None)

    # 5) уведомляем клиента
    local_dt = appt.start_dt.astimezone(settings.tz)
    await msg.reply_text(
        "Заявка отправлена ✅\n"
        f"Услуга: {service.name}\n"
        f"Дата/время: {local_dt.strftime('%d.%m %H:%M')}\n"
        "Статус: Ожидает подтверждения\n"
        "Ожидай подтверждения мастера.",
        reply_markup=main_menu_kb(),
    )

    # 6) уведомляем админа с кнопками
    try:
        admin_id = int(cfg.admin_telegram_id)
        client_name = (
            update.effective_user.full_name
            or (f"@{update.effective_user.username}" if update.effective_user.username else str(update.effective_user.id))
        )
        await context.bot.send_message(
            chat_id=admin_id,
            text=(
                "🆕 Новая заявка (ожидает подтверждения)\n"
                f"#{appt.id}\n"
                f"{service.name}\n"
                f"{local_dt.strftime('%d.%m %H:%M')}\n"
                f"Клиент: {client_name}\n"
                f"Телефон: {phone or '—'}\n"
                f"Комментарий: {comment or '—'}"
            ),
            reply_markup=admin_request_kb(appt.id),
        )
    except Exception:
        pass

async def finalize_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    session_factory = context.bot_data["session_factory"]

    svc_id = context.user_data.get(K_SVC)
    slot_iso = context.user_data.get(K_SLOT)
    if not svc_id or not slot_iso:
        return await update.callback_query.message.edit_text("Сессия сброшена. Нажми «Записаться» заново.")

    start_local = datetime.fromisoformat(slot_iso)

    async with session_factory() as s:
        async with s.begin():
            settings = await get_settings(s, cfg.timezone)
            client = await upsert_user(s, update.effective_user.id, update.effective_user.username, update.effective_user.full_name)
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

            await context.bot.send_message(
                chat_id=cfg.admin_telegram_id,
                text=(
                    f"🆕 Новая заявка (HOLD #{appt.id})\n"
                    f"Услуга: {service.name}\n"
                    f"Дата/время: {appt.start_dt.astimezone(settings.tz).strftime('%d.%m %H:%M')}\n"
                    f"Длительность: {int(service.duration_min)} мин (+буфер)\n"
                    f"Цена: {service.price}\n\n"
                    f"Клиент: {update.effective_user.full_name} (@{update.effective_user.username})\n"
                    f"Телефон: {client.phone or '—'}\n"
                    f"Комментарий: {context.user_data.get(K_COMMENT) or '—'}\n\n"
                    f"Hold истекает: {appt.hold_expires_at.astimezone(settings.tz).strftime('%H:%M')}"
                ),
                reply_markup=admin_request_kb(appt.id)
            )

    for k in (K_SVC, K_DATE, K_SLOT, K_COMMENT):
        context.user_data.pop(k, None)

    await update.callback_query.message.edit_text(
        "Заявка создана ✅\nСтатус: Ожидает подтверждения.\nЯ сообщу, когда мастер подтвердит запись."
    )

async def show_my_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        appts = await get_user_appointments(s, update.effective_user.id, limit=10)
    if not appts:
        await update.message.reply_text("У вас пока нет записей.", reply_markup=main_menu_kb())
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
        await update.message.reply_text("История пустая.", reply_markup=main_menu_kb())
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

    txt = (
        f"Запись #{appt.id}\n"
        f"Статус: {appt.status.value}\n"
        f"Дата/время: {appt.start_dt.astimezone(settings.tz).strftime('%d.%m %H:%M')}\n"
        f"Услуга: {appt.service.name}\n"
        f"Комментарий: {appt.client_comment or '—'}"
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
            await context.bot.send_message(
                chat_id=cfg.admin_telegram_id,
                text=f"🚫 Клиент отменил запись #{appt.id} на {appt.start_dt.astimezone(settings.tz).strftime('%d.%m %H:%M')}"
            )
    await update.callback_query.message.edit_text("Запись отменена ✅")

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
                    f"Адриана ждет Вас!"
                )
            )
    await update.callback_query.message.edit_text(f"Подтверждено ✅ (#{appt_id})")

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
    await update.callback_query.message.edit_text(f"Отклонено ❌ (#{appt_id})")

async def admin_action_msg(update: Update, context: ContextTypes.DEFAULT_TYPE, appt_id: int):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        return await update.callback_query.message.edit_text("Нет доступа.")
    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        appt = await get_appointment(s, appt_id)
    await update.callback_query.message.edit_text(
        f"TG ID клиента: {appt.client.tg_id}\n@{appt.client.username or '—'}"
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

async def admin_day_view(update: Update, context: ContextTypes.DEFAULT_TYPE, offset_days: int):
    cfg: Config = context.bot_data["cfg"]
    if not is_admin(cfg, update.effective_user.id):
        return await update.message.reply_text("Нет доступа.")

    session_factory = context.bot_data["session_factory"]
    async with session_factory() as s:
        settings = await get_settings(s, cfg.timezone)
        day = (datetime.now(tz=settings.tz) + timedelta(days=offset_days)).date()
        appts = await admin_list_appointments_for_day(s, settings.tz, day)

    if not appts:
        return await update.message.reply_text(
            f"На {day.strftime('%d.%m')} записей нет.",
            reply_markup=admin_menu_kb()
        )

    lines = [f"📅 Записи на {day.strftime('%d.%m')}:" ]
    for a in appts:
        t = a.start_dt.astimezone(settings.tz).strftime("%H:%M")
        client = a.client.full_name or (f"@{a.client.username}" if a.client.username else str(a.client.tg_id))
        phone = a.client.phone or "—"
        lines.append(f"• {t} | #{a.id} | {a.status.value} | {a.service.name} | {client} | {phone}")

    await update.message.reply_text("\n".join(lines), reply_markup=admin_menu_kb())


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
