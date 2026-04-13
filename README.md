# polymarket-sports-movers

Read-only Python Telegram bot для мониторинга спортивных рынков Polymarket через публичные API без wallet auth, private auth и API keys.

Что делает:
- обновляет universe каждые 5 минут;
- берёт только будущие события в горизонте `HORIZON_DAYS`;
- отфильтровывает live / ended / closed / started;
- отслеживает только `BEST ASK`;
- считает изменение за `LOOKBACK_MINUTES`;
- шлёт alert, если движение `>= MIN_MOVE_PP` percentage points;
- пишет alert-события в CSV и сохраняет rolling state в `SNAPSHOT_DIR`.

## Запуск

1. Скопировать `.env.example` в `.env`.
2. Заполнить `TG_TOKEN` и `TG_CHAT_ID`.
3. Установить зависимости: `pip install -r requirements.txt`
4. Проверить сборку: `npm run build`
5. Запустить: `python3 polymarket_movers.py`

Если `TG_TOKEN` или `TG_CHAT_ID` пустые, бот работает в dry-run режиме и печатает alerts в stdout.
Также можно принудительно включить dry-run через `DRY_RUN=true`.
