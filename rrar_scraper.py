"""
Парсер рейтингов РРАР (AllAdvertising.ru)
"""
import json
import re
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

from ..utils import main_logger, safe_request, clean_text, normalize_company_name
from config import SEGMENT_TAGS

class RRARScraper:
    """Парсер рейтингов РРАР для получения данных о BTL агентствах"""
    
    def _init_(self, base_url: str = "https://www.alladvertising.ru/"):
        self.base_url = base_url
        self.logger = main_logger
        
    def scrape_btl_agencies(self) -> List[Dict[str, Any]]:
        """Парсинг BTL агентств из рейтинга РРАР"""
        agencies = []
        
        try:
            url = urljoin(self.base_url, "top/btl/")
            self.logger.info(f"Парсинг BTL агентств: {url}")
            
            response = safe_request(url)
            if not response:
                return agencies
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ищем компании в рейтинге
            companies = self._extract_companies_from_page(soup, SEGMENT_TAGS["BTL"])
            agencies.extend(companies)
            
            self.logger.info(f"Найдено BTL агентств: {len(companies)}")
            
        except Exception as e:
            self.logger.error(f"Ошибка парсинга BTL агентств: {e}")
            
        return agencies
      def scrape_souvenir_companies(self) -> List[Dict[str, Any]]:
        """Парсинг компаний сувенирной продукции"""
        companies = []
        
        try:
            url = urljoin(self.base_url, "top/gifts/")
            self.logger.info(f"Парсинг сувенирных компаний: {url}")
            
            response = safe_request(url)
            if not response:
                return companies
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ищем компании в рейтинге
            found_companies = self._extract_companies_from_page(soup, SEGMENT_TAGS["SOUVENIR"])
            companies.extend(found_companies)
            
            self.logger.info(f"Найдено сувенирных компаний: {len(found_companies)}")
            
        except Exception as e:
            self.logger.error(f"Ошибка парсинга сувенирных компаний: {e}")
            
        return companies
    
    def scrape_event_agencies(self) -> List[Dict[str, Any]]:
        """Парсинг ивент-агентств"""
        agencies = []
        
        try:
            url = urljoin(self.base_url, "top/event/")
            self.logger.info(f"Парсинг ивент-агентств: {url}")
            
            response = safe_request(url)
            if not response:
                return agencies
                
            soup = BeautifulSoup(response.text, 'html.parser')
          # Ищем компании в рейтинге  
            companies = self._extract_companies_from_page(soup, SEGMENT_TAGS["EVENT"])
            agencies.extend(companies)
            
            self.logger.info(f"Найдено ивент-агентств: {len(companies)}")
            
        except Exception as e:
            self.logger.error(f"Ошибка парсинга ивент-агентств: {e}")
            
        return agencies
    
    def scrape_top100_agencies(self) -> List[Dict[str, Any]]:
        """Парсинг ТОП-100 агентств (полный цикл)"""
        agencies = []
        
        try:
            url = urljoin(self.base_url, "top100/")
            self.logger.info(f"Парсинг ТОП-100 агентств: {url}")
            
            response = safe_request(url)
            if not response:
                return agencies
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ищем компании в рейтинге
            companies = self._extract_companies_from_page(soup, SEGMENT_TAGS["FULL_CYCLE"])
            agencies.extend(companies)
            
            self.logger.info(f"Найдено агентств полного цикла: {len(companies)}")
            
        except Exception as e:
            self.logger.error(f"Ошибка парсинга агентств полного цикла: {e}")
            
        return agencies
      def _extract_companies_from_page(self, soup: BeautifulSoup, segment_tag: str) -> List[Dict[str, Any]]:
        """
        Извлечение данных компаний со страницы рейтинга
        
        Args:
            soup: BeautifulSoup объект страницы
            segment_tag: Тег сегмента компании
            
        Returns:
            Список компаний
        """
        companies = []
        
        try:
            # Ищем различные варианты структуры страницы
            # Вариант 1: Компании в блоках с логотипами
            company_blocks = soup.find_all(['div', 'article'], class_=re.compile(r'company|item|card'))
            
            if not company_blocks:
                # Вариант 2: Ищем ссылки на компании
                company_links = soup.find_all('a', href=re.compile(r'/info/'))
                
                for link in company_links:
                    company_data = self._extract_company_from_link(link, segment_tag)
                    if company_data:
                        companies.append(company_data)
            
            else:
                # Обрабатываем блоки компаний
                for block in company_blocks:
                    company_data = self._extract_company_from_block(block, segment_tag)
                    if company_data:
                        companies.append(company_data)
        
        except Exception as e:
            self.logger.error(f"Ошибка извлечения компаний: {e}")
        
        return companies
    
    def _extract_company_from_link(self, link_element, segment_tag: str) -> Optional[Dict[str, Any]]:
        """
        Извлечение данных компании из ссылки
        
        Args:
            link_element: HTML элемент ссылки
            segment_tag: Тег сегмента
            
        Returns:
            Данные компании или None
        """
        try:
            name = clean_text(link_element.get_text())
            if not name:
                return None
            
            # Получаем ссылку на детальную страницу
            detail_url = link_element.get('href')
            if detail_url:
                detail_url = urljoin(self.base_url, detail_url)
            
            company_data = {
                'name': normalize_company_name(name),
                'segment_tag': segment_tag,
                'source': 'rrar_2025',
                'rating_ref': detail_url,
                'description': '',
                'site': '',
                'contacts': '',
                'region': ''
            }
            
            # Пытаемся получить дополнительную информацию
            if detail_url:
                self._enrich_company_data(company_data, detail_url)
            
            return company_data
            
        except Exception as e:
            self.logger.error(f"Ошибка извлечения компании из ссылки: {e}")
            return None
          def _extract_company_from_block(self, block_element, segment_tag: str) -> Optional[Dict[str, Any]]:
        """
        Извлечение данных компании из блока
        
        Args:
            block_element: HTML элемент блока
            segment_tag: Тег сегмента
            
        Returns:
            Данные компании или None
        """
        try:
            # Ищем название компании
            name_element = block_element.find(['h2', 'h3', 'h4', 'a'])
            if not name_element:
                return None
            
            name = clean_text(name_element.get_text())
            if not name:
                return None
            
            # Ищем описание
            description = ''
            desc_element = block_element.find('p') or block_element.find(['div'], class_=re.compile(r'desc|content|text'))
            if desc_element:
                description = clean_text(desc_element.get_text())
            
            # Ищем ссылку на детальную страницу
            detail_link = block_element.find('a', href=True)
            detail_url = ''
            if detail_link:
                detail_url = urljoin(self.base_url, detail_link['href'])
            
            company_data = {
                'name': normalize_company_name(name),
                'segment_tag': segment_tag,
                'source': 'rrar_2025',
                'rating_ref': detail_url,
                'description': description[:200],  # Ограничиваем длину
                'site': '',
                'contacts': '',
                'region': ''
            }
            
            return company_data
            
        except Exception as e:
            self.logger.error(f"Ошибка извлечения компании из блока: {e}")
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
            
            # Ищем дополнительную информацию
            # Описание
            desc_elements = soup.find_all(['p'], limit=3)
            if desc_elements and not company_data['description']:
                full_desc = ' '.join([clean_text(elem.get_text()) for elem in desc_elements])
                company_data['description'] = full_desc[:200]
            
            # Контакты (телефон, email)
            contact_text = soup.get_text()
            
            # Извлекаем телефон
            phone_match = re.search(r'\+7[\s\-\(\)]?\d{3}[\s\-\(\)]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}', contact_text)
            if phone_match:
                company_data['contacts'] = phone_match.group()
            
            # Извлекаем email
            email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', contact_text)
            if email_match and not company_data['contacts']:
                company_data['contacts'] = email_match.group()
              # Сайт
            site_links = soup.find_all('a', href=re.compile(r'^https?://'))
            for link in site_links:
                href = link.get('href', '')
                domain = urlparse(href).netloc
                if domain and 'alladvertising' not in domain:
                    company_data['site'] = href
                    break
            
        except Exception as e:
            self.logger.error(f"Ошибка обогащения данных компании {detail_url}: {e}")
    
    def scrape_all(self) -> List[Dict[str, Any]]:
        """
        Парсинг всех категорий РРАР
        
        Returns:
            Список всех компаний
        """
        all_companies = []
        
        self.logger.info("Начинаем парсинг всех категорий РРАР")
        
        # BTL агентства
        btl_companies = self.scrape_btl_agencies()
        all_companies.extend(btl_companies)
        
        # Сувенирная продукция  
        souvenir_companies = self.scrape_souvenir_companies()
        all_companies.extend(souvenir_companies)
        
        # Ивент-агентства
        event_companies = self.scrape_event_agencies()
        all_companies.extend(event_companies)
        
        # ТОП-100 (полный цикл)
        top100_companies = self.scrape_top100_agencies()
        all_companies.extend(top100_companies)
        
        self.logger.info(f"Всего получено компаний из РРАР: {len(all_companies)}")
        
        return all_companies
    
    def save_raw_data(self, companies: List[Dict[str, Any]], filename: str = "rrar_data.json") -> None:
        """
        Сохранение сырых данных в JSON файл
        
        Args:
            companies: Список компаний
            filename: Имя файла для сохранения
        """
        try:
            with open(f"data/raw/{filename}", 'w', encoding='utf-8') as f:
                json.dump(companies, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Сырые данные РРАР сохранены в {filename}")
            
        except Exception as e:
            self.logger.error(f"Ошибка сохранения данных: {e}")
