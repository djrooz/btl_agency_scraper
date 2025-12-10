"""
Парсер данных с marketing-tech.ru
"""
import json
import re
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from ..utils import main_logger, safe_request, clean_text, normalize_company_name, parse_revenue
from config import SEGMENT_TAGS

class MarketingTechScraper:
    """Парсер сайта marketing-tech.ru для получения данных о маркетинговых агентствах"""
    
    def __init__(self, base_url: str = "https://marketing-tech.ru/"):
        self.base_url = base_url
        self.logger = main_logger
        
    def scrape_btl_agencies(self) -> List[Dict[str, Any]]:
        """Парсинг BTL агентств с данными о выручке"""
        agencies = []
        
        try:
            url = urljoin(self.base_url, "company_tags/btl/")
            self.logger.info(f"Парсинг BTL агентств marketing-tech: {url}")
            
            response = safe_request(url)
            if not response:
                return agencies
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ищем таблицу с рейтингом
            companies = self._extract_companies_from_table(soup, SEGMENT_TAGS["BTL"])
            agencies.extend(companies)
            
            self.logger.info(f"Найдено BTL агентств в marketing-tech: {len(companies)}")
            
        except Exception as e:
            self.logger.error(f"Ошибка парсинга BTL агентств marketing-tech: {e}")
            
        return agencies
    
    def scrape_marketing_agencies(self) -> List[Dict[str, Any]]:
        """Парсинг маркетинговых агентств"""
        agencies = []
        
        try:
            url = urljoin(self.base_url, "company_tags/marketing/")
            self.logger.info(f"Парсинг маркетинговых агентств: {url}")
            
            response = safe_request(url)
            if not response:
                return agencies
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            companies = self._extract_companies_from_table(soup, SEGMENT_TAGS["FULL_CYCLE"])
            agencies.extend(companies)
            
            self.logger.info(f"Найдено маркетинговых агентств: {len(companies)}")
            
        except Exception as e:
            self.logger.error(f"Ошибка парсинга маркетинговых агентств: {e}")
            
        return agencies
    
    def scrape_advertising_agencies(self) -> List[Dict[str, Any]]:
        """Парсинг рекламных агентств"""
        agencies = []
        
        try:
            url = urljoin(self.base_url, "company_tags/advertising/")
            self.logger.info(f"Парсинг рекламных агентств: {url}")
            
            response = safe_request(url)
            if not response:
                return agencies
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            companies = self._extract_companies_from_table(soup, SEGMENT_TAGS["FULL_CYCLE"])
            agencies.extend(companies)
            
            self.logger.info(f"Найдено рекламных агентств: {len(companies)}")
            
        except Exception as e:
            self.logger.error(f"Ошибка парсинга рекламных агентств: {e}")
            
        return agencies
    
    def _extract_companies_from_table(self, soup: BeautifulSoup, segment_tag: str) -> List[Dict[str, Any]]:
        """
        Извлечение компаний из таблицы рейтинга
        
        Args:
            soup: BeautifulSoup объект страницы
            segment_tag: Тег сегмента компании
            
        Returns:
            Список компаний
        """
        companies = []
        
        try:
            # Ищем таблицу с рейтингом
            table = soup.find('table') or soup.find('div', class_=re.compile(r'table|rating'))
            
            if not table:
                self.logger.warning("Таблица рейтинга не найдена")
                return companies
            
            # Ищем строки таблицы
            rows = table.find_all('tr')
            
            for row in rows[1:]:  # Пропускаем заголовок
                company_data = self._extract_company_from_row(row, segment_tag)
                if company_data:
                    companies.append(company_data)
        
        except Exception as e:
            self.logger.error(f"Ошибка извлечения компаний из таблицы: {e}")
        
        return companies
    
    def _extract_company_from_row(self, row, segment_tag: str) -> Optional[Dict[str, Any]]:
        """
        Извлечение данных компании из строки таблицы
        
        Args:
            row: HTML элемент строки таблицы
            segment_tag: Тег сегмента
            
        Returns:
            Данные компании или None
        """
        try:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                return None
            
            # Обычно структура: № | Название | Оборот | Индекс | Клиенты
            name_cell = cells[1] if len(cells) > 1 else cells[0]
            revenue_cell = cells[2] if len(cells) > 2 else None
            
            # Извлекаем название
            name_link = name_cell.find('a')
            if name_link:
                name = clean_text(name_link.get_text())
                detail_url = name_link.get('href')
                if detail_url:
                    detail_url = urljoin(self.base_url, detail_url)
            else:
                name = clean_text(name_cell.get_text())
                detail_url = ''
            
            if not name:
                return None
            
            # Извлекаем выручку
            revenue = 0
            revenue_year = 2024  # По умолчанию текущий год
            
            if revenue_cell:
                revenue_text = clean_text(revenue_cell.get_text())
                parsed_revenue = parse_revenue(revenue_text)
                if parsed_revenue:
                    revenue = parsed_revenue
            
            company_data = {
                'name': normalize_company_name(name),
                'revenue': revenue,
                'revenue_year': revenue_year,
                'segment_tag': segment_tag,
                'source': 'marketing_tech',
                'rating_ref': detail_url,
                'description': '',
                'site': '',
                'contacts': '',
                'region': ''
            }
            
            # Пытаемся получить дополнительную информацию с детальной страницы
            if detail_url:
                self._enrich_company_data(company_data, detail_url)
            
            return company_data
            
        except Exception as e:
            self.logger.error(f"Ошибка извлечения компании из строки: {e}")
            return None
    
    def _enrich_company_data(self, company_data: Dict[str, Any], detail_url: str) -> None:
        """
        Обогащение данных компании с детальной страницы
        
        Args:
            company_data: Данные компании для обогащения
            detail_url: URL детальной страницы
        """
        try:
            response = safe_request(detail_url, delay=2)
            if not response:
                return
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ищем описание компании
            desc_sections = soup.find_all(['div'], class_=re.compile(r'desc|about|info|content'))
            if desc_sections:
                descriptions = []
                for section in desc_sections:
                    text = clean_text(section.get_text())
                    if text and len(text) > 50:  # Минимальная длина описания
                        descriptions.append(text)
                
                if descriptions:
                    company_data['description'] = descriptions[0][:200]
            
            # Ищем контактную информацию
            contact_text = soup.get_text()
            
            # Телефон
            phone_match = re.search(r'\+7[\s\-\(\)]?\d{3}[\s\-\(\)]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}', contact_text)
            if phone_match:
                company_data['contacts'] = phone_match.group().strip()
            
            # Email
            email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', contact_text)
            if email_match and not company_data['contacts']:
                company_data['contacts'] = email_match.group()
            
            # Сайт компании
            site_links = soup.find_all('a', href=re.compile(r'^https?://'))
            for link in site_links:
                href = link.get('href', '')
                if self._is_company_website(href):
                    company_data['site'] = href
                    break
            
            # Город/регион
            location_keywords = ['москва', 'спб', 'санкт-петербург', 'екатеринбург', 'новосибирск', 'казань']
            page_text = soup.get_text().lower()
            
            for keyword in location_keywords:
                if keyword in page_text:
                    company_data['region'] = keyword.title()
                    break
                    
        except Exception as e:
            self.logger.error(f"Ошибка обогащения данных компании {detail_url}: {e}")
    
    def _is_company_website(self, url: str) -> bool:
        """
        Проверка, является ли URL сайтом компании
        
        Args:
            url: URL для проверки
            
        Returns:
            True если это сайт компании
        """
        if not url:
            return False
        
        # Исключаем сервисные сайты
        excluded_domains = [
            'marketing-tech.ru',
            'google.com',
            'yandex.ru',
            'facebook.com',
            'vk.com',
            'instagram.com',
            'linkedin.com',
            'youtube.com'
        ]
        
        url_lower = url.lower()
        return not any(domain in url_lower for domain in excluded_domains)
    
    def scrape_all(self) -> List[Dict[str, Any]]:
        """
        Парсинг всех категорий marketing-tech
        
        Returns:
            Список всех компаний
        """
        all_companies = []
        
        self.logger.info("Начинаем парсинг marketing-tech.ru")
        
        # BTL агентства
        btl_companies = self.scrape_btl_agencies()
        all_companies.extend(btl_companies)
        
        # Маркетинговые агентства
        marketing_companies = self.scrape_marketing_agencies()
        all_companies.extend(marketing_companies)
        
        # Рекламные агентства
        advertising_companies = self.scrape_advertising_agencies()
        all_companies.extend(advertising_companies)
        
        self.logger.info(f"Всего получено компаний из marketing-tech: {len(all_companies)}")
        
        return all_companies
    
    def save_raw_data(self, companies: List[Dict[str, Any]], filename: str = "marketing_tech_data.json") -> None:
        """
        Сохранение сырых данных в JSON файл
        
        Args:
            companies: Список компаний
            filename: Имя файла для сохранения
        """
        try:
            with open(f"data/raw/{filename}", 'w', encoding='utf-8') as f:
                json.dump(companies, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Сырые данные marketing-tech сохранены в {filename}")
            
        except Exception as e:
            self.logger.error(f"Ошибка сохранения данных: {e}")
            
    def get_company_details(self, company_url: str) -> Dict[str, Any]:
        """
        Получение детальной информации о компании
        
        Args:
            company_url: URL страницы компании
            
        Returns:
            Словарь с данными компании
        """
        company_details = {}
        
        try:
            response = safe_request(company_url, delay=2)
            if not response:
                return company_details
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Извлекаем различные поля
            
            # Название
            name_element = soup.find('h1') or soup.find(['h2', 'h3'], class_=re.compile(r'name|title'))
            if name_element:
                company_details['name'] = clean_text(name_element.get_text())
            
            # Описание
            desc_element = soup.find(['div'], class_=re.compile(r'desc|about|info'))
            if desc_element:
                company_details['description'] = clean_text(desc_element.get_text())[:200]
            
            # Сайт
            site_element = soup.find('a', href=re.compile(r'^https?://'))
            if site_element:
                href = site_element.get('href')
                if self._is_company_website(href):
                    company_details['site'] = href
            
            # Количество сотрудников
            employees_text = soup.get_text()
            employees_match = re.search(r'(\d+)\s*сотрудник|(\d+)\s*человек|штат[:\s]*(\d+)', employees_text, re.IGNORECASE)
            if employees_match:
                employees = employees_match.group(1) or employees_match.group(2) or employees_match.group(3)
                company_details['employees'] = int(employees)
            
        except Exception as e:
            self.logger.error(f"Ошибка получения деталей компании {company_url}: {e}")
        
        return company_details
