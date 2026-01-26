from datetime import datetime
import pytz
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Appointment, AppointmentStatus
from app.logic import get_settings


async def tick(application):
    """
    Сжигает истёкшие HOLD-заявки.
    Идемпотентен: одна заявка обрабатывается ровно один раз.
    """
    session_factory = application.bot_data["session_factory"]
    cfg = application.bot_data["cfg"]

    now_utc = datetime.now(tz=pytz.UTC)

    async with session_factory() as s:  # type: AsyncSession
        # 1) выбираем ТОЛЬКО истёкшие HOLD
        res = await s.execute(
            select(Appointment)
            .where(
                and_(
                    Appointment.status == AppointmentStatus.Hold,
                    Appointment.hold_expires_at.is_not(None),
                    Appointment.hold_expires_at <= now_utc,
                )
            )
        )
        expired = res.scalars().all()

        if not expired:
            return  # нечего делать — важно, чтобы не было спама

        # 2) обновляем статус
        for appt in expired:
            appt.status = AppointmentStatus.Rejected
            appt.updated_at = now_utc

        await s.commit()  # ФИКСИРУЕМ ОДИН РАЗ

    # 3) уведомляем клиентов УЖЕ ПОСЛЕ commit
    #    (если бот упадёт — статус уже сохранён, повторов не будет)
    for appt in expired:
        try:
            await application.bot.send_message(
                chat_id=appt.client.tg_id,
                text=(
                    "⏳ Заявка не была подтверждена мастером и автоматически отменена.\n\n"
                    f"Дата/время: {appt.start_dt.astimezone(pytz.UTC).strftime('%d.%m %H:%M')}\n"
                    "Вы можете выбрать другое время в меню «Записаться»."
                ),
            )
        except Exception:
            # не валим тик из-за проблем с одним клиентом
            pass
