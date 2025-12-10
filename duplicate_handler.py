"""
Модуль для обработки дубликатов компаний
"""
from typing import List, Dict, Any, Optional, Set
import pandas as pd
from difflib import SequenceMatcher

from ..utils import main_logger

class DuplicateHandler:
    """Класс для обработки дубликатов компаний"""
    
    def __init__(self):
        self.logger = main_logger
        
    def remove_duplicates(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Удаление дубликатов из списка компаний
        
        Args:
            companies: Список компаний с возможными дубликатами
            
        Returns:
            Список уникальных компаний
        """
        self.logger.info(f"Начинаем обработку дубликатов для {len(companies)} компаний")
        
        # 1. Группируем по ИНН
        inn_groups = self._group_by_inn(companies)
        
        # 2. Обрабатываем компании без ИНН отдельно
        no_inn_companies = [c for c in companies if not c.get('inn')]
        name_groups = self._group_by_similarity(no_inn_companies)
        
        # 3. Объединяем результаты
        unique_companies = []
        
        # Обрабатываем группы по ИНН
        for inn, group in inn_groups.items():
            if inn:  # Пропускаем пустые ИНН
                merged_company = self._merge_company_group(group)
                unique_companies.append(merged_company)
        
        # Обрабатываем группы по названиям
        for group in name_groups:
            merged_company = self._merge_company_group(group)
            unique_companies.append(merged_company)
        
        self.logger.info(f"После удаления дубликатов: {len(unique_companies)} уникальных компаний")
        
        return unique_companies
    
    def _group_by_inn(self, companies: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Группировка компаний по ИНН
        
        Args:
            companies: Список компаний
            
        Returns:
            Словарь групп по ИНН
        """
        inn_groups = {}
        
        for company in companies:
            inn = company.get('inn', '').strip()
            if inn:
                if inn not in inn_groups:
                    inn_groups[inn] = []
                inn_groups[inn].append(company)
        
        return inn_groups
    
    def _group_by_similarity(self, companies: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """
        Группировка компаний по схожести названий
        
        Args:
            companies: Список компаний без ИНН
            
        Returns:
            Список групп схожих компаний
        """
        groups = []
        processed_indices = set()
        
        for i, company1 in enumerate(companies):
            if i in processed_indices:
                continue
                
            group = [company1]
            processed_indices.add(i)
            
            name1 = self._normalize_name_for_comparison(company1.get('name', ''))
            
            for j, company2 in enumerate(companies[i+1:], i+1):
                if j in processed_indices:
                    continue
                    
                name2 = self._normalize_name_for_comparison(company2.get('name', ''))
                
                if self._are_names_similar(name1, name2):
                    group.append(company2)
                    processed_indices.add(j)
            
            groups.append(group)
        
        return groups
    
    def _normalize_name_for_comparison(self, name: str) -> str:
        """
        Нормализация названия для сравнения
        
        Args:
            name: Исходное название
            
        Returns:
            Нормализованное название
        """
        if not name:
            return ""
        
        # Приводим к нижнему регистру
        name = name.lower()
        
        # Удаляем организационно-правовые формы
        legal_forms = [
            'ооо', 'зао', 'оао', 'ао', 'ип', 'пао',
            'общество с ограниченной ответственностью',
            'закрытое акционерное общество',
            'открытое акционерное общество',
            'акционерное общество',
            'публичное акционерное общество',
            'индивидуальный предприниматель'
        ]
        
        for form in legal_forms:
            name = name.replace(form, '').strip()
        
        # Удаляем кавычки, скобки, специальные символы
        import re
        name = re.sub(r'["\'\(\)\[\]«»]', '', name)
        name = re.sub(r'[^\w\s]', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name
    
    def _are_names_similar(self, name1: str, name2: str, threshold: float = 0.8) -> bool:
        """
        Проверка схожести названий
        
        Args:
            name1: Первое название
            name2: Второе название
            threshold: Порог схожести (0-1)
            
        Returns:
            True если названия схожи
        """
        if not name1 or not name2:
            return False
        
        # Точное совпадение
        if name1 == name2:
            return True
        
        # Проверяем схожесть по алгоритму Sequence Matcher
        similarity = SequenceMatcher(None, name1, name2).ratio()
        
        if similarity >= threshold:
            return True
        
        # Проверяем, содержит ли одно название другое
        if len(name1) > 5 and len(name2) > 5:
            if name1 in name2 or name2 in name1:
                return True
        
        # Проверяем по словам
        words1 = set(name1.split())
        words2 = set(name2.split())
        
        if words1 and words2:
            intersection = words1.intersection(words2)
            union = words1.union(words2)
            
            # Если пересечение составляет большую часть от объединения
            word_similarity = len(intersection) / len(union)
            if word_similarity >= 0.6:
                return True
        
        return False
    
    def _merge_company_group(self, group: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Слияние группы компаний в одну запись
        
        Args:
            group: Группа дубликатов компаний
            
        Returns:
            Объединенные данные компании
        """
        if len(group) == 1:
            return group[0]
        
        # Базовая компания - берем с наиболее полными данными
        base_company = self._select_base_company(group)
        merged = base_company.copy()
        
        # Объединяем данные из других записей
        for company in group:
            if company == base_company:
                continue
            
            merged = self._merge_two_companies(merged, company)
        
        # Объединяем источники
        sources = []
        for company in group:
            source = company.get('source', '')
            if source and source not in sources:
                sources.append(source)
        
        merged['source'] = ', '.join(sources) if sources else merged.get('source', '')
        
        self.logger.debug(f"Объединено {len(group)} записей для компании {merged.get('name', 'Unknown')}")
        
        return merged
    
    def _select_base_company(self, group: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Выбор базовой компании из группы (с наиболее полными данными)
        
        Args:
            group: Группа компаний
            
        Returns:
            Базовая компания
        """
        # Считаем количество заполненных полей для каждой компании
        def count_filled_fields(company):
            count = 0
            for key, value in company.items():
                if value and str(value).strip():
                    count += 1
            return count
        
        # Приоритет источникам по качеству данных
        source_priority = {
            'fns_open_data': 5,
            'marketing_tech': 4, 
            'rrar_2025': 3,
            'rusprofile': 2,
            'list_org': 1
        }
        
        def get_company_score(company):
            filled_fields = count_filled_fields(company)
            source = company.get('source', '')
            source_score = source_priority.get(source, 0)
            
            # Бонус за наличие выручки > 0
            revenue_bonus = 10 if company.get('revenue', 0) > 0 else 0
            
            # Бонус за наличие ИНН
            inn_bonus = 5 if company.get('inn', '') else 0
            
            return filled_fields + source_score + revenue_bonus + inn_bonus
        
        return max(group, key=get_company_score)
    
    def _merge_two_companies(self, base: Dict[str, Any], additional: Dict[str, Any]) -> Dict[str, Any]:
        """
        Слияние двух записей компаний
        
        Args:
            base: Базовая запись
            additional: Дополнительная запись
            
        Returns:
            Объединенная запись
        """
        merged = base.copy()
        
        # Приоритет полей - заполняем пустые поля из дополнительной записи
        for key, value in additional.items():
            if key in merged:
                base_value = merged[key]
                
                # Если базовое значение пустое, берем из дополнительной записи
                if not base_value or (isinstance(base_value, str) and not base_value.strip()):
                    merged[key] = value
                
                # Для числовых полей берем максимальное значение
                elif key in ['revenue', 'employees'] and isinstance(value, (int, float)) and value > 0:
                    if isinstance(base_value, (int, float)) and base_value == 0:
                        merged[key] = value
                    elif isinstance(base_value, (int, float)) and value > base_value:
                        merged[key] = value
                
                # Для описания объединяем, если они разные
                elif key == 'description' and value and value != base_value:
                    if len(str(value)) > len(str(base_value)):
                        merged[key] = value
            else:
                merged[key] = value
        
        return merged
    
    def get_duplicate_statistics(self, original_count: int, unique_count: int) -> Dict[str, Any]:
        """
        Статистика по дубликатам
        
        Args:
            original_count: Изначальное количество записей
            unique_count: Количество уникальных записей
            
        Returns:
            Словарь со статистикой
        """
        duplicates_removed = original_count - unique_count
        duplicate_rate = (duplicates_removed / original_count * 100) if original_count > 0 else 0
        
        return {
            'original_count': original_count,
            'unique_count': unique_count,
            'duplicates_removed': duplicates_removed,
            'duplicate_rate_percent': round(duplicate_rate, 2)
        }
    
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
        return df
    
    def save_deduplicated_data(self, companies: List[Dict[str, Any]], filename: str = "deduplicated_data.csv") -> None:
        """
        Сохранение дедуплицированных данных
        
        Args:
            companies: Список уникальных компаний
            filename: Имя файла
        """
        try:
            df = self.to_dataframe(companies)
            df.to_csv(f"data/interim/{filename}", index=False, encoding='utf-8')
            
            self.logger.info(f"Дедуплицированные данные сохранены в {filename}")
            
        except Exception as e:
            self.logger.error(f"Ошибка сохранения дедуплицированных данных: {e}")
