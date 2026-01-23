from dotenv import load_dotenv
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters

from app.config import load_config
from app.db import make_engine, make_session_factory
from app.models import Base
from app.logic import seed_defaults_if_needed, ensure_default_services
from app.handlers import cmd_start, cb_router, handle_contact, unified_text_router
from app.scheduler import tick
from app.reminders import check_and_send_reminders  # booking reminders


async def init_db(engine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def seed_db(session_factory, cfg):
    defaults = {
        "slot_step_min": str(cfg.slot_step_min),
        "buffer_min": str(cfg.buffer_min),
        "min_lead_time_min": str(cfg.min_lead_time_min),
        "booking_horizon_days": str(cfg.booking_horizon_days),
        "hold_ttl_min": str(cfg.hold_ttl_min),
        "cancel_limit_hours": str(cfg.cancel_limit_hours),
        "work_start": cfg.work_start,
        "work_end": cfg.work_end,
        "work_days": cfg.work_days,
    }
    async with session_factory() as s:
        async with s.begin():
            await seed_defaults_if_needed(s, defaults=defaults)
            await ensure_default_services(s)


def main():
    load_dotenv()
    cfg = load_config()

    engine = make_engine(cfg)
    session_factory = make_session_factory(engine)

    async def post_init(app: Application):
        await init_db(engine)
        await seed_db(session_factory, cfg)

    app = Application.builder().token(cfg.bot_token).post_init(post_init).build()

    # shared objects for handlers/jobs
    app.bot_data["cfg"] = cfg
    app.bot_data["session_factory"] = session_factory
    # timezone for displaying appointment times in reminders
    app.bot_data["tz"] = getattr(cfg, "tz", None) or getattr(cfg, "timezone", None) or "Europe/Moscow"

    # handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(cb_router))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unified_text_router))

    # periodic jobs (every 60s)
    async def tick_job(ctx):
        await tick(ctx.application)

    app.job_queue.run_repeating(tick_job, interval=60, first=10)

    # reminders: 48h and 3h before appointment (checked every 60s)
    app.job_queue.run_repeating(check_and_send_reminders, interval=60, first=20)

    # LOCAL: polling if webhook not configured
    if cfg.webhook_url:
        app.run_webhook(
            listen="0.0.0.0",
            port=cfg.port,
            url_path="telegram",
            webhook_url=f"{cfg.webhook_url}/telegram",
            allowed_updates=["message", "callback_query"],
            drop_pending_updates=True,
        )
    else:
        app.run_polling(
            allowed_updates=["message", "callback_query"],
            drop_pending_updates=True,
        )


if __name__ == "__main__":
    main()
