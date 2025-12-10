"""
Модуль для очистки и нормализации данных
"""
import re
from typing import List, Dict, Any, Optional
import pandas as pd

from ..utils import main_logger, clean_text, normalize_company_name, parse_revenue, extract_inn, extract_phone, extract_email
from config import config, BTL_KEYWORDS

class DataCleaner:
    """Класс для очистки и нормализации данных о компаниях"""
    
    def __init__(self):
        self.logger = main_logger
        self.min_revenue = config.min_revenue
        
    def clean_companies_data(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Очистка списка компаний
        
        Args:
            companies: Список сырых данных компаний
            
        Returns:
            Список очищенных данных компаний
        """
        cleaned_companies = []
        
        self.logger.info(f"Начинаем очистку {len(companies)} компаний")
        
        for company in companies:
            cleaned_company = self.clean_single_company(company)
            if cleaned_company:
                cleaned_companies.append(cleaned_company)
        
        self.logger.info(f"Очищено {len(cleaned_companies)} компаний")
        return cleaned_companies
    
    def clean_single_company(self, company: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Очистка данных одной компании
        
        Args:
            company: Сырые данные компании
            
        Returns:
            Очищенные данные компании или None
        """
        try:
            cleaned = {}
            
            # Обязательные поля
            cleaned['name'] = self._clean_company_name(company.get('name', ''))
            if not cleaned['name']:
                return None
            
            cleaned['inn'] = self._clean_inn(company.get('inn', ''))
            cleaned['revenue'] = self._clean_revenue(company.get('revenue', 0))
            cleaned['revenue_year'] = self._clean_revenue_year(company.get('revenue_year', 2024))
            cleaned['segment_tag'] = self._clean_segment_tag(company.get('segment_tag', ''))
            cleaned['source'] = self._clean_source(company.get('source', ''))
            
            # Дополнительные поля
            cleaned['okved_main'] = self._clean_okved(company.get('okved_main', ''))
            cleaned['employees'] = self._clean_employees(company.get('employees', 0))
            cleaned['site'] = self._clean_url(company.get('site', ''))
            cleaned['description'] = self._clean_description(company.get('description', ''))
            cleaned['region'] = self._clean_region(company.get('region', ''))
            cleaned['contacts'] = self._clean_contacts(company.get('contacts', ''))
            cleaned['rating_ref'] = self._clean_url(company.get('rating_ref', ''))
            
            # Проверяем валидность данных
            if not self._is_valid_company(cleaned):
                return None
            
            return cleaned
            
        except Exception as e:
            self.logger.error(f"Ошибка очистки компании: {e}")
            return None
    
    def _clean_company_name(self, name: str) -> str:
        """Очистка названия компании"""
        if not name:
            return ""
        
        name = normalize_company_name(name)
        
        # Удаляем лишние символы
        name = re.sub(r'[^\w\s\-\&\.\(\)\"\']+', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name
    
    def _clean_inn(self, inn: Any) -> str:
        """Очистка ИНН"""
        if not inn:
            return ""
        
        # Приводим к строке и оставляем только цифры
        inn_str = str(inn).strip()
        inn_digits = re.sub(r'[^\d]', '', inn_str)
        
        # Проверяем длину
        if len(inn_digits) in [10, 12]:
            return inn_digits
        
        return ""
    
    def _clean_revenue(self, revenue: Any) -> float:
        """Очистка выручки"""
        if isinstance(revenue, (int, float)):
            return float(revenue)
        
        if isinstance(revenue, str):
            parsed = parse_revenue(revenue)
            return parsed if parsed else 0.0
        
        return 0.0
    
    def _clean_revenue_year(self, year: Any) -> int:
        """Очистка года выручки"""
        try:
            year_int = int(year)
            if 2000 <= year_int <= 2025:
                return year_int
        except (ValueError, TypeError):
            pass
        
        return 2024  # По умолчанию текущий год
    
    def _clean_segment_tag(self, segment: str) -> str:
        """Очистка тега сегмента"""
        if not segment:
            return ""
        
        segment_upper = segment.upper()
        
        # Проверяем валидные теги
        valid_tags = ["BTL", "SOUVENIR", "FULL_CYCLE", "COMM_GROUP", "EVENT", "PROMO"]
        
        for tag in valid_tags:
            if tag in segment_upper:
                return tag
        
        return "BTL"  # По умолчанию BTL
    
    def _clean_source(self, source: str) -> str:
        """Очистка источника данных"""
        if not source:
            return "unknown"
        
        source_lower = source.lower()
        
        # Стандартизируем источники
        if 'rrar' in source_lower or 'alladvertising' in source_lower:
            return 'rrar_2025'
        elif 'marketing-tech' in source_lower or 'marketing_tech' in source_lower:
            return 'marketing_tech'
        elif 'fns' in source_lower:
            return 'fns_open_data'
        elif 'rusprofile' in source_lower:
            return 'rusprofile'
        elif 'list-org' in source_lower or 'list_org' in source_lower:
            return 'list_org'
        
        return source.lower()
    
    def _clean_okved(self, okved: str) -> str:
        """Очистка ОКВЭД кода"""
        if not okved:
            return ""
        
        # Извлекаем код ОКВЭД (обычно формат XX.XX.X)
        okved_match = re.search(r'\d{2}\.\d{1,2}(?:\.\d{1,2})?', str(okved))
        return okved_match.group() if okved_match else ""
    
    def _clean_employees(self, employees: Any) -> int:
        """Очистка количества сотрудников"""
        try:
            if isinstance(employees, str):
                # Извлекаем число из строки
                numbers = re.findall(r'\d+', employees)
                if numbers:
                    return int(numbers[0])
            return int(employees) if employees else 0
        except (ValueError, TypeError):
            return 0
    
    def _clean_url(self, url: str) -> str:
        """Очистка URL"""
        if not url:
            return ""
        
        url = url.strip()
        
        # Проверяем валидность URL
        url_pattern = re.compile(
            r'^https?://'  # протокол
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # домен
            r'localhost|'  # localhost
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # IP
            r'(?::\d+)?'  # порт
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        return url if url_pattern.match(url) else ""
    
    def _clean_description(self, description: str) -> str:
        """Очистка описания"""
        if not description:
            return ""
        
        desc = clean_text(description)
        
        # Ограничиваем длину
        if len(desc) > 300:
            desc = desc[:300] + "..."
        
        return desc
    
    def _clean_region(self, region: str) -> str:
        """Очистка региона"""
        if not region:
            return ""
        
        region = clean_text(region)
        
        # Стандартизируем названия регионов
        region_mapping = {
            'москва': 'Москва',
            'moscow': 'Москва',
            'спб': 'Санкт-Петербург',
            'санкт-петербург': 'Санкт-Петербург',
            'питер': 'Санкт-Петербург',
            'petersburg': 'Санкт-Петербург',
            'екатеринбург': 'Екатеринбург',
            'новосибирск': 'Новосибирск',
            'казань': 'Казань',
            'нижний новгород': 'Нижний Новгород',
            'ростов-на-дону': 'Ростов-на-Дону'
        }
        
        region_lower = region.lower()
        for key, value in region_mapping.items():
            if key in region_lower:
                return value
        
        return region.title()
    
    def _clean_contacts(self, contacts: str) -> str:
        """Очистка контактов"""
        if not contacts:
            return ""
        
        contacts = clean_text(contacts)
        
        # Проверяем, что это телефон или email
        phone = extract_phone(contacts)
        email = extract_email(contacts)
        
        return phone or email or contacts[:50]
    
    def _is_valid_company(self, company: Dict[str, Any]) -> bool:
        """
        Проверка валидности данных компании
        
        Args:
            company: Очищенные данные компании
            
        Returns:
            True если компания валидна
        """
        # Обязательные поля
        if not company.get('name'):
            return False
        
        # Проверяем минимальную выручку (если есть данные)
        revenue = company.get('revenue', 0)
        if revenue > 0 and revenue < self.min_revenue:
            self.logger.debug(f"Компания {company['name']} отфильтрована по выручке: {revenue}")
            return False
        
        return True
    
    def filter_by_relevance(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Фильтрация компаний по релевантности
        
        Args:
            companies: Список компаний
            
        Returns:
            Список релевантных компаний
        """
        relevant_companies = []
        
        for company in companies:
            if self._is_relevant_company(company):
                relevant_companies.append(company)
        
        self.logger.info(f"Отфильтровано {len(relevant_companies)} релевантных компаний из {len(companies)}")
        return relevant_companies
    
    def _is_relevant_company(self, company: Dict[str, Any]) -> bool:
        """
        Проверка релевантности компании
        
        Args:
            company: Данные компании
            
        Returns:
            True если компания релевантна
        """
        # 1. Проверяем сегмент
        segment = company.get('segment_tag', '')
        if segment in ['BTL', 'SOUVENIR', 'FULL_CYCLE', 'COMM_GROUP', 'EVENT', 'PROMO']:
            return True
        
        # 2. Проверяем ОКВЭД
        okved = company.get('okved_main', '')
        relevant_okved = ['73.11', '82.30', '47.78.3', '73.20', '82.99']
        if any(code in okved for code in relevant_okved):
            return True
        
        # 3. Проверяем описание на ключевые слова
        description = (company.get('description', '') + ' ' + company.get('name', '')).lower()
        
        if any(keyword in description for keyword in BTL_KEYWORDS):
            return True
        
        return False
    
    def to_dataframe(self, companies: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Преобразование в DataFrame
        
        Args:
            companies: Список компаний
            
        Returns:
            DataFrame с данными компаний
        """
        if not companies:
            return pd.DataFrame()
        
        df = pd.DataFrame(companies)
        
        # Обеспечиваем наличие всех необходимых колонок
        required_columns = [
            'inn', 'name', 'revenue_year', 'revenue', 'segment_tag', 'source',
            'okved_main', 'employees', 'site', 'description', 'region', 'contacts', 'rating_ref'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = ''
        
        # Устанавливаем правильные типы данных
        df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)
        df['employees'] = pd.to_numeric(df['employees'], errors='coerce').fillna(0)
        df['revenue_year'] = pd.to_numeric(df['revenue_year'], errors='coerce').fillna(2024)
        
        return df[required_columns]
    
    def save_cleaned_data(self, companies: List[Dict[str, Any]], filename: str = "cleaned_data.csv") -> None:
        """
        Сохранение очищенных данных
        
        Args:
            companies: Список компаний
            filename: Имя файла
        """
        try:
            df = self.to_dataframe(companies)
            df.to_csv(f"data/interim/{filename}", index=False, encoding='utf-8')
            
            self.logger.info(f"Очищенные данные сохранены в {filename}")
            
        except Exception as e:
            self.logger.error(f"Ошибка сохранения очищенных данных: {e}")
