"""
Модуль для настройки логирования
"""
import logging
import sys
from pathlib import Path
from typing import Optional

def setup_logger(
    name: str,
    log_file: Optional[str] = None,
    level: str = "INFO",
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    Настройка логгера с выводом в файл и консоль
    
    Args:
        name: Имя логгера
        log_file: Путь к файлу логов
        level: Уровень логирования
        format_string: Формат сообщений
    
    Returns:
        Настроенный логгер
    """
    logger = logging.getLogger(name)
    
    # Очищаем существующие обработчики
    logger.handlers.clear()
    
    # Устанавливаем уровень
    logger.setLevel(getattr(logging, level.upper()))
    
    # Формат по умолчанию
    if not format_string:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    formatter = logging.Formatter(format_string)
    
    # Консольный обработчик
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Файловый обработчик
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

# Основной логгер проекта
main_logger = setup_logger("btl_scraper", "logs/scraper.log")
