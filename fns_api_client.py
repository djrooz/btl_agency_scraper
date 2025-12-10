"""
Клиент для работы с API ФНС и получения финансовых данных
"""
import json
import time
from typing import List, Dict, Any, Optional
import requests

from ..utils import main_logger, validate_inn
from config import config

class FNSAPIClient:
    """Клиент для работы с API ФНС и получения данных о компаниях"""
    
    def __init__(self):
        self.logger = main_logger
        self.api_endpoints = {
            'dadata': 'https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party',
            'zachestnyibiznes': 'https://zachestnyibiznes.ru/api/v1/company/',
            'ofdata': 'https://api.ofdata.ru/v2/company/',
            'rusprofile_search': 'https://www.rusprofile.ru/search'
        }
        
    def get_company_by_inn(self, inn: str) -> Optional[Dict[str, Any]]:
        """
        Получение данных компании по ИНН
        
        Args:
            inn: ИНН компании
            
        Returns:
            Данные компании или None
        """
        if not validate_inn(inn):
            self.logger.error(f"Некорректный ИНН: {inn}")
            return None
        
        # Пробуем разные источники
        company_data = None
        
        # 1. Пробуем получить через открытые API
        company_data = self._get_from_dadata(inn)
        if company_data:
            return company_data
            
        # 2. Пробуем zachestnyibiznes
        company_data = self._get_from_zachestnyibiznes(inn)
        if company_data:
            return company_data
        
        # 3. Пробуем поиск через rusprofile
        company_data = self._get_from_rusprofile(inn)
        if company_data:
            return company_data
            
        self.logger.warning(f"Не удалось получить данные для ИНН: {inn}")
        return None
    
    def _get_from_dadata(self, inn: str) -> Optional[Dict[str, Any]]:
        """
        Получение данных через DaData API
        
        Args:
            inn: ИНН компании
            
        Returns:
            Данные компании или None
        """
        try:
            # Для демонстрации используем публичный эндпоинт (требует API ключ)
            # В реальном проекте нужен API ключ DaData
            
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                # 'Authorization': f'Token {api_key}'  # Требуется API ключ
            }
            
            data = {
                'query': inn,
                'type': 'LEGAL'
            }
            
            # Пока не делаем реальный запрос без API ключа
            self.logger.info(f"DaData API требует ключ для ИНН: {inn}")
            return None
            
        except Exception as e:
            self.logger.error(f"Ошибка получения данных из DaData для {inn}: {e}")
            return None
    
    def _get_from_zachestnyibiznes(self, inn: str) -> Optional[Dict[str, Any]]:
        """
        Получение данных через zachestnyibiznes API
        
        Args:
            inn: ИНН компании
            
        Returns:
            Данные компании или None
        """
        try:
            url = f"https://zachestnyibiznes.ru/api/v1/company/{inn}"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data and 'success' in data and data['success']:
                    company_info = data.get('data', {})
                    
                    return {
                        'inn': inn,
                        'name': company_info.get('name', ''),
                        'full_name': company_info.get('full_name', ''),
                        'okved_main': company_info.get('okved', {}).get('main', {}).get('code', ''),
                        'okved_description': company_info.get('okved', {}).get('main', {}).get('name', ''),
                        'region': company_info.get('address', {}).get('region', ''),
                        'status': company_info.get('status', ''),
                        'revenue': 0,  # Требуется отдельный запрос за финансы
                        'revenue_year': 0,
                        'employees': company_info.get('employees', 0),
                        'registration_date': company_info.get('registration_date', ''),
                        'source': 'zachestnyibiznes'
                    }
            else:
                self.logger.warning(f"Ошибка API zachestnyibiznes для {inn}: {response.status_code}")
                
        except Exception as e:
            self.logger.error(f"Ошибка получения данных из zachestnyibiznes для {inn}: {e}")
        
        return None
    
    def _get_from_rusprofile(self, inn: str) -> Optional[Dict[str, Any]]:
        """
        Получение данных через поиск на rusprofile
        
        Args:
            inn: ИНН компании
            
        Returns:
            Данные компании или None
        """
        try:
            # Формируем URL для поиска по ИНН
            search_url = f"https://www.rusprofile.ru/search?query={inn}"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            
            time.sleep(2)  # Ограничиваем скорость запросов
            response = requests.get(search_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                # Здесь должен быть парсинг HTML для извлечения данных
                # Для демонстрации возвращаем базовую структуру
                
                return {
                    'inn': inn,
                    'name': '',
                    'okved_main': '',
                    'region': '',
                    'revenue': 0,
                    'revenue_year': 0,
                    'employees': 0,
                    'source': 'rusprofile'
                }
            
        except Exception as e:
            self.logger.error(f"Ошибка получения данных из rusprofile для {inn}: {e}")
        
        return None
    
    def get_financial_data(self, inn: str) -> Optional[Dict[str, Any]]:
        """
        Получение финансовых данных компании
        
        Args:
            inn: ИНН компании
            
        Returns:
            Финансовые данные или None
        """
        try:
            # Пытаемся получить данные из открытых источников
            
            # 1. Росстат БФО (бухгалтерская отчетность)
            bfo_data = self._get_bfo_data(inn)
            if bfo_data:
                return bfo_data
            
            # 2. Альтернативные источники
            alt_data = self._get_alternative_financial_data(inn)
            if alt_data:
                return alt_data
                
        except Exception as e:
            self.logger.error(f"Ошибка получения финансовых данных для {inn}: {e}")
        
        return None
    
    def _get_bfo_data(self, inn: str) -> Optional[Dict[str, Any]]:
        """
        Получение данных из БФО Росстат
        
        Args:
            inn: ИНН компании
            
        Returns:
            Финансовые данные или None
        """
        try:
            # URL сервиса БФО ФНС
            bfo_url = "https://bo.nalog.gov.ru/"
            
            # Для получения данных БФО требуется более сложная логика
            # с обработкой форм и сессий
            self.logger.info(f"БФО данные требуют сложной интеграции для {inn}")
            return None
            
        except Exception as e:
            self.logger.error(f"Ошибка получения БФО данных для {inn}: {e}")
            return None
    
    def _get_alternative_financial_data(self, inn: str) -> Optional[Dict[str, Any]]:
        """
        Получение финансовых данных из альтернативных источников
        
        Args:
            inn: ИНН компании
            
        Returns:
            Финансовые данные или None
        """
        try:
            # Можно использовать коммерческие API или другие источники
            # Для демонстрации возвращаем заглушку
            
            return {
                'inn': inn,
                'revenue_2023': 0,
                'revenue_2022': 0,
                'revenue_2021': 0,
                'profit_2023': 0,
                'employees_2023': 0,
                'source': 'alternative_api'
            }
            
        except Exception as e:
            self.logger.error(f"Ошибка получения альтернативных финансовых данных для {inn}: {e}")
            return None
    
    def batch_get_companies(self, inn_list: List[str]) -> List[Dict[str, Any]]:
        """
        Пакетное получение данных по списку ИНН
        
        Args:
            inn_list: Список ИНН
            
        Returns:
            Список данных компаний
        """
        companies = []
        
        self.logger.info(f"Получаем данные для {len(inn_list)} компаний")
        
        for i, inn in enumerate(inn_list):
            try:
                self.logger.info(f"Обрабатываем ИНН {i+1}/{len(inn_list)}: {inn}")
                
                company_data = self.get_company_by_inn(inn)
                if company_data:
                    # Добавляем финансовые данные
                    financial_data = self.get_financial_data(inn)
                    if financial_data:
                        company_data.update(financial_data)
                    
                    companies.append(company_data)
                
                # Пауза между запросами
                time.sleep(2)
                
            except Exception as e:
                self.logger.error(f"Ошибка обработки ИНН {inn}: {e}")
                continue
        
        self.logger.info(f"Получено данных для {len(companies)} компаний")
        return companies
    
    def save_raw_data(self, companies: List[Dict[str, Any]], filename: str = "fns_data.json") -> None:
        """
        Сохранение данных в JSON файл
        
        Args:
            companies: Список компаний
            filename: Имя файла
        """
        try:
            with open(f"data/raw/{filename}", 'w', encoding='utf-8') as f:
                json.dump(companies, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Данные ФНС сохранены в {filename}")
            
        except Exception as e:
            self.logger.error(f"Ошибка сохранения данных ФНС: {e}")
    
    def enrich_company_data(self, company: Dict[str, Any]) -> Dict[str, Any]:
        """
        Обогащение данных компании через API ФНС
        
        Args:
            company: Данные компании
            
        Returns:
            Обогащенные данные компании
        """
        inn = company.get('inn')
        if not inn:
            return company
        
        try:
            # Получаем дополнительные данные
            fns_data = self.get_company_by_inn(inn)
            if fns_data:
                # Обновляем данные, не перезаписывая существующие
                for key, value in fns_data.items():
                    if key not in company or not company[key]:
                        company[key] = value
            
            # Получаем финансовые данные
            financial_data = self.get_financial_data(inn)
            if financial_data:
                for key, value in financial_data.items():
                    if key not in company or not company[key]:
                        company[key] = value
                        
        except Exception as e:
            self.logger.error(f"Ошибка обогащения данных компании {inn}: {e}")
        
        return company
