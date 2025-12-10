"""
Вспомогательные функции для работы с данными
"""
import re
import time
import random
from typing import Optional, Union, List, Dict, Any
from urllib.parse import urljoin, urlparse
import requests
from config import USER_AGENTS

def clean_text(text: str) -> str:
    """
    Очистка текста от лишних символов и пробелов
    
    Args:
        text: Исходный текст
        
    Returns:
        Очищенный текст
    """
    if not text:
        return ""
    
    # Удаляем HTML теги
    text = re.sub(r'<[^>]+>', '', text)
    
    # Удаляем лишние пробелы и переносы
    text = re.sub(r'\s+', ' ', text)
    
    # Убираем пробелы в начале и конце
    text = text.strip()
    
    return text

def extract_inn(text: str) -> Optional[str]:
    """
    Извлечение ИНН из текста
    
    Args:
        text: Текст для поиска ИНН
        
    Returns:
        ИНН или None
    """
    if not text:
        return None
    
    # Паттерн для ИНН (10 или 12 цифр)
    inn_pattern = r'\b\d{10,12}\b'
    match = re.search(inn_pattern, text)
    
    if match:
        inn = match.group()
        # Проверяем длину (ИНН может быть 10 или 12 цифр)
        if len(inn) in [10, 12]:
            return inn
    
    return None

def parse_revenue(revenue_str: str) -> Optional[float]:
    """
    Парсинг выручки из строки
    
    Args:
        revenue_str: Строка с выручкой
        
    Returns:
        Выручка в рублях или None
    """
    if not revenue_str:
        return None
    
    # Удаляем лишние символы
    revenue_str = re.sub(r'[^\d,.\s]', '', revenue_str.lower())
    
    # Ищем числа
    numbers = re.findall(r'\d+[,.]?\d*', revenue_str)
    if not numbers:
        return None
    
    try:
        # Берем первое число
        num_str = numbers[0].replace(',', '.')
        revenue = float(num_str)

  # Определяем единицы измерения
        original_str = revenue_str.lower()
        
        if 'млрд' in original_str or 'billion' in original_str:
            revenue *= 1_000_000_000
        elif 'млн' in original_str or 'million' in original_str:
            revenue *= 1_000_000
        elif 'тыс' in original_str or 'thousand' in original_str:
            revenue *= 1_000
        
        return revenue
        
    except ValueError:
        return None

def extract_phone(text: str) -> Optional[str]:
    """
    Извлечение телефона из текста
    
    Args:
        text: Текст для поиска телефона
        
    Returns:
        Телефон или None
    """
    if not text:
        return None
    
    # Паттерны для российских телефонов
    phone_patterns = [
        r'\+7[\s\-\(\)]?\d{3}[\s\-\(\)]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}',
        r'8[\s\-\(\)]?\d{3}[\s\-\(\)]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}',
        r'\(\d{3}\)[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}'
    ]
    
    for pattern in phone_patterns:
        match = re.search(pattern, text)
        if match:
            return match.group().strip()
    
    return None

def extract_email(text: str) -> Optional[str]:
    """
    Извлечение email из текста
    
    Args:
        text: Текст для поиска email
        
    Returns:
        Email или None
    """
    if not text:
        return None
    
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    match = re.search(email_pattern, text)
    
    return match.group() if match else None
  def normalize_company_name(name: str) -> str:
    """
    Нормализация названия компании
    
    Args:
        name: Исходное название
        
    Returns:
        Нормализованное название
    """
    if not name:
        return ""
    
    name = clean_text(name)
    
    # Удаляем организационно-правовые формы в начале
    legal_forms = [
        r'^ООО\s*["\']?',
        r'^ЗАО\s*["\']?',
        r'^ОАО\s*["\']?',
        r'^АО\s*["\']?',
        r'^ИП\s*["\']?',
        r'^Общество с ограниченной ответственностью\s*["\']?'
    ]
    
    for pattern in legal_forms:
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    
    # Удаляем кавычки
    name = re.sub(r'^["\']|["\']$', '', name)
    
    return name.strip()

def get_random_user_agent() -> str:
    """
    Получение случайного User-Agent
    
    Returns:
        User-Agent строка
    """
    return random.choice(USER_AGENTS)

def safe_request(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    delay: float = 1.0,
    max_retries: int = 3,
    timeout: int = 30
) -> Optional[requests.Response]:
    """
    Безопасный HTTP запрос с повторными попытками
    
    Args:
        url: URL для запроса
        headers: Заголовки
        delay: Задержка между запросами
        max_retries: Максимальное количество попыток
        timeout: Таймаут запроса
        
    Returns:
        Response объект или None
    """
    if not headers:
        headers = {'User-Agent': get_random_user_agent()}
    
    for attempt in range(max_retries):
        try:
            time.sleep(delay + random.uniform(0, 1))
            
            response = requests.get(url, headers=headers, timeout=timeout)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:  # Too Many Requests
                wait_time = delay * (2 ** attempt)  # Exponential backoff
                time.sleep(wait_time)
            else:
                response.raise_for_status()
                
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                print(f"Ошибка запроса {url}: {e}")
                return None
            
            wait_time = delay * (2 ** attempt)
            time.sleep(wait_time)
    
    return None
  def validate_inn(inn: str) -> bool:
    """
    Валидация ИНН
    
    Args:
        inn: ИНН для проверки
        
    Returns:
        True если ИНН валиден
    """
    if not inn or not inn.isdigit():
        return False
    
    if len(inn) not in [10, 12]:
        return False
    
    # Упрощенная проверка (без контрольных сумм)
    return True

def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Разбивка списка на чанки
    
    Args:
        lst: Исходный список
        chunk_size: Размер чанка
        
    Returns:
        Список чанков
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
