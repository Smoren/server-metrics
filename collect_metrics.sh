#!/bin/bash
# Скрипт сбора метрик

LOG_FILE="/var/log/system-metrics.csv"
METRICS_SCRIPT="/opt/system_monitor/system_monitor.py"

# Если файл не существует, создаем заголовок
if [ ! -f "$LOG_FILE" ] || [ ! -s "$LOG_FILE" ]; then
    python3 "$METRICS_SCRIPT" --format header > "$LOG_FILE"
fi

# Добавляем новую строку с метриками
python3 "$METRICS_SCRIPT" --format csv >> "$LOG_FILE"
