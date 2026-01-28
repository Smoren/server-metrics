#!/usr/bin/env python3
"""
Системный монитор для сбора метрик нагрузки
Формат вывода оптимизирован для pandas.read_csv()
"""

import psutil
import json
import time
from datetime import datetime
import socket
import argparse
import sys
import os

def get_disk_metrics():
    """Получение детальной информации о дисках"""
    disk_metrics = {}
    
    # Получаем список всех разделов
    partitions = psutil.disk_partitions(all=False)  # all=False исключает специальные файловые системы
    
    for partition in partitions:
        # Пропускаем специальные файловые системы
        if partition.fstype in ['tmpfs', 'devtmpfs', 'squashfs', 'overlay', 'proc', 'sysfs', 'cgroup']:
            continue
        
        try:
            usage = psutil.disk_usage(partition.mountpoint)
            
            # Создаем безопасное имя для CSV колонки
            mount_name = partition.mountpoint.replace('/', '_').replace('.', '_').strip('_')
            if not mount_name:  # для корневого раздела
                mount_name = 'root'
            
            # Получаем имя устройства без пути
            device_name = os.path.basename(partition.device)
            
            disk_metrics[mount_name] = {
                'mountpoint': partition.mountpoint,
                'device': device_name,
                'fstype': partition.fstype,
                'total_gb': round(usage.total / (1024**3), 2),
                'used_gb': round(usage.used / (1024**3), 2),
                'free_gb': round(usage.free / (1024**3), 2),
                'percent': round(usage.percent, 2),
                'total_bytes': usage.total,
                'used_bytes': usage.used,
                'free_bytes': usage.free
            }
            
        except (PermissionError, FileNotFoundError):
            # Пропускаем разделы без доступа
            continue
        except Exception as e:
            print(f"Error reading {partition.mountpoint}: {e}", file=sys.stderr)
            continue
    
    return disk_metrics

def collect_metrics():
    """Сбор всех системных метрик"""
    
    # Время сбора
    timestamp = datetime.now().isoformat()
    
    # CPU метрики
    cpu_percent = psutil.cpu_percent(interval=1, percpu=False)
    cpu_percent_per_core = psutil.cpu_percent(interval=1, percpu=True)
    cpu_count = psutil.cpu_count()
    cpu_freq = psutil.cpu_freq()
    
    # RAM метрики
    memory = psutil.virtual_memory()
    swap = psutil.swap_memory()
    
    # Дисковые метрики
    disk_metrics = get_disk_metrics()
    disk_io = psutil.disk_io_counters()
    
    # Сетевая статистика
    net_io = psutil.net_io_counters()
    
    # Загрузка системы
    load_avg = psutil.getloadavg()
    
    # Процессы
    process_count = len(psutil.pids())
    
    # Собираем все метрики в словарь
    metrics = {
        'timestamp': timestamp,
        'hostname': socket.gethostname(),
        
        # CPU
        'cpu_percent': round(cpu_percent, 2),
        'cpu_count': cpu_count,
        'cpu_freq_current': round(cpu_freq.current, 2) if cpu_freq else None,
        'load_1min': round(load_avg[0], 2),
        'load_5min': round(load_avg[1], 2),
        'load_15min': round(load_avg[2], 2),
        
        # Память (в гигабайтах для удобства)
        'memory_total_gb': round(memory.total / (1024**3), 2),
        'memory_available_gb': round(memory.available / (1024**3), 2),
        'memory_used_gb': round(memory.used / (1024**3), 2),
        'memory_percent': round(memory.percent, 2),
        'swap_total_gb': round(swap.total / (1024**3), 2) if swap.total > 0 else 0,
        'swap_used_gb': round(swap.used / (1024**3), 2) if swap.total > 0 else 0,
        'swap_percent': round(swap.percent, 2) if swap.total > 0 else 0,
        
        # Дисковая статистика IO
        'disk_io_read_mb': round(disk_io.read_bytes / (1024**2), 2),
        'disk_io_write_mb': round(disk_io.write_bytes / (1024**2), 2),
        
        # Сетевая статистика
        'net_mb_sent': round(net_io.bytes_sent / (1024**2), 2),
        'net_mb_recv': round(net_io.bytes_recv / (1024**2), 2),
        
        # Процессы
        'process_count': process_count,
    }
    
    # Добавляем метрики по каждому диску
    total_disk_used = 0
    total_disk_size = 0
    
    for mount_name, disk_info in disk_metrics.items():
        # Основные метрики в гигабайтах
        metrics[f'disk_{mount_name}_total_gb'] = disk_info['total_gb']
        metrics[f'disk_{mount_name}_used_gb'] = disk_info['used_gb']
        metrics[f'disk_{mount_name}_free_gb'] = disk_info['free_gb']
        metrics[f'disk_{mount_name}_percent'] = disk_info['percent']
        
        # Дополнительные метрики для детального анализа
        metrics[f'disk_{mount_name}_device'] = disk_info['device']
        metrics[f'disk_{mount_name}_fstype'] = disk_info['fstype']
        
        # Суммируем для общего объема (если это физические диски, а не overlay/loop)
        if not any(x in disk_info['fstype'] for x in ['overlay', 'squashfs']):
            total_disk_used += disk_info['used_bytes']
            total_disk_size += disk_info['total_bytes']
    
    # Общая статистика по всем дискам
    if total_disk_size > 0:
        metrics['disk_total_all_gb'] = round(total_disk_size / (1024**3), 2)
        metrics['disk_used_all_gb'] = round(total_disk_used / (1024**3), 2)
        metrics['disk_percent_all'] = round((total_disk_used / total_disk_size) * 100, 2)
    
    # Количество обнаруженных дисков
    metrics['disk_count'] = len(disk_metrics)
    
    return metrics

def print_csv_header():
    """Вывод заголовка CSV для pandas"""
    metrics = collect_metrics()
    print(','.join(metrics.keys()))

def print_csv_row():
    """Вывод строки CSV с метриками"""
    metrics = collect_metrics()
    
    # Форматируем значения для CSV
    row_values = []
    for key, value in metrics.items():
        if value is None:
            row_values.append('')
        elif isinstance(value, str) and ',' in value:
            # Экранируем запятые в строках
            row_values.append(f'"{value}"')
        elif isinstance(value, (int, float)):
            row_values.append(str(value))
        else:
            row_values.append(str(value))
    
    print(','.join(row_values))

def print_human_readable():
    """Вывод в удобочитаемом формате"""
    metrics = collect_metrics()
    disk_metrics = get_disk_metrics()
    
    print(f"\n{'='*60}")
    print(f"System Metrics Report - {metrics['timestamp']}")
    print(f"{'='*60}")
    
    print(f"\n📊 CPU Usage:")
    print(f"  Overall: {metrics['cpu_percent']}%")
    print(f"  Load Average: {metrics['load_1min']:.2f}, {metrics['load_5min']:.2f}, {metrics['load_15min']:.2f}")
    
    print(f"\n🧠 Memory:")
    print(f"  Used: {metrics['memory_used_gb']:.1f} GB / {metrics['memory_total_gb']:.1f} GB ({metrics['memory_percent']}%)")
    print(f"  Available: {metrics['memory_available_gb']:.1f} GB")
    
    if metrics['swap_total_gb'] > 0:
        print(f"  Swap: {metrics['swap_used_gb']:.1f} GB / {metrics['swap_total_gb']:.1f} GB ({metrics['swap_percent']}%)")
    
    print(f"\n💾 Disk Usage:")
    for mount_name, disk_info in disk_metrics.items():
        mountpoint = disk_info['mountpoint']
        print(f"  {mountpoint}:")
        print(f"    Used: {disk_info['used_gb']:.1f} GB / {disk_info['total_gb']:.1f} GB ({disk_info['percent']}%)")
        print(f"    Free: {disk_info['free_gb']:.1f} GB")
        print(f"    Type: {disk_info['device']} ({disk_info['fstype']})")
    
    print(f"\n📈 Disk IO:")
    print(f"  Read: {metrics['disk_io_read_mb']:.1f} MB")
    print(f"  Write: {metrics['disk_io_write_mb']:.1f} MB")
    
    print(f"\n🌐 Network:")
    print(f"  Sent: {metrics['net_mb_sent']:.1f} MB")
    print(f"  Received: {metrics['net_mb_recv']:.1f} MB")
    
    print(f"\n🔢 System:")
    print(f"  Processes: {metrics['process_count']}")
    print(f"  Disks: {metrics['disk_count']}")
    
    print(f"\n{'='*60}")

def print_json():
    """Вывод в формате JSON"""
    metrics = collect_metrics()
    print(json.dumps(metrics, indent=2))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='System Metrics Collector')
    parser.add_argument('--format', choices=['csv', 'json', 'header', 'human'], 
                       default='human', help='Output format')
    parser.add_argument('--once', action='store_true', 
                       help='Collect once and exit')
    
    args = parser.parse_args()
    
    try:
        if args.format == 'header':
            print_csv_header()
        elif args.format == 'json':
            print_json()
        elif args.format == 'human':
            print_human_readable()
        else:
            print_csv_row()
        
        if not args.once:
            sys.exit(0)
            
    except Exception as e:
        print(f"Error collecting metrics: {e}", file=sys.stderr)
        sys.exit(1)