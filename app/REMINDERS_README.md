# app/reminders.py

Этот файл нужно положить в проект по пути **app/reminders.py**.

## Что делает
- Раз в минуту проверяет записи со статусом `BOOKED`.
- Шлёт напоминание:
  - за **48 часов** до `start_dt` (один раз)
  - за **3 часа** до `start_dt` (один раз)
- Ставит флаги `reminded_48h` / `reminded_3h`, чтобы не было дублей.

## Требования к схеме БД (минимум)
Таблица `appointments` должна иметь:
- `id` (int)
- `user_id` (telegram user id, int)
- `start_dt` (timestamp, желательно timezone-aware, UTC)
- `status` (строка, ожидается `BOOKED`)
- `reminded_48h` (bool)
- `reminded_3h` (bool)

Опционально:
- `service_name` (строка)
- `service_id` (int) + таблица `services` с `id`, `name`

Если у вас другие названия — поправьте SQL в функциях `_select_due` и `_mark_sent`.
