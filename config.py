"""
Конфигурация проекта для сбора данных о BTL агентствах
"""
import json
import os
from pathlib import Path
from typing import Dict, Any, List

class Config:
    """Класс для управления конфигурацией проекта"""
    
    def _init_(self, config_file: str = "config.json"):
        self.config_file = config_file
        self._config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """Загрузка конфигурации из файла"""
        config_path = Path(self.config_file)
        
        if not config_path.exists():
            # Если config.json не существует, используем example
            example_path = Path("config.example.json")
            if example_path.exists():
                with open(example_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                return self._get_default_config()
        
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Конфигурация по умолчанию"""
        return {
            "scrapers": {
                "rrar": {
                    "base_url": "https://www.alladvertising.ru/",
                    "categories": ["top/btl/", "top/gifts/", "top100/", "top/event/"],
                    "delay": 1,
                    "max_retries": 3
                },
                "marketing_tech": {
                    "base_url": "https://marketing-tech.ru/",
                    "endpoints": {
                        "btl": "company_tags/btl/",
                        "marketing": "company_tags/marketing/",
                        "advertising": "company_tags/advertising/"
                    },
                    "delay": 2,
                    "max_retries": 3
                }
            },
            "filters": {
                "min_revenue":
      200000000,
                "country": "RU",
                "revenue_years": [2022, 2023, 2024]
            },
            "output": {
                "csv_file": "data/companies.csv",
                "raw_data_dir": "data/raw/",
                "interim_data_dir": "data/interim/"
            }
        }
    
    @property
    def scrapers(self) -> Dict[str, Any]:
        """Конфигурация парсеров"""
        return self._config.get("scrapers", {})
    
    @property
    def filters(self) -> Dict[str, Any]:
        """Настройки фильтров"""
        return self._config.get("filters", {})
    
    @property
    def output(self) -> Dict[str, Any]:
        """Настройки вывода"""
        return self._config.get("output", {})
    
    @property
    def llm(self) -> Dict[str, Any]:
        """Настройки LLM"""
        return self._config.get("llm", {})
    
    @property
    def logging_config(self) -> Dict[str, Any]:
        """Настройки логирования"""
        return self._config.get("logging", {})
    
    @property
    def min_revenue(self) -> int:
        """Минимальная выручка"""
        return self.filters.get("min_revenue", 200000000)
    
    @property
    def revenue_years(self) -> List[int]:
        """Годы для анализа выручки"""
        return self.filters.get("revenue_years", [2022, 2023, 2024])

# Глобальный экземпляр конфигурации
config = Config()

# Вспомогательные константы
SEGMENT_TAGS = {
    "BTL": "BTL",
    "SOUVENIR": "SOUVENIR", 
    "FULL_CYCLE": "FULL_CYCLE",
    "COMM_GROUP": "COMM_GROUP",
    "EVENT": "EVENT",
    "PROMO": "PROMO"
}
OKVED_CODES = {
    "73.11": "Деятельность рекламных агентств",
    "82.30": "Организация конференций и выставок",
    "47.78.3": "Торговля розничная сувенирами",
    "73.20": "Исследование конъюнктуры рынка и выявление общественного мнения",
    "82.99": "Прочая деловая деятельность"
}

BTL_KEYWORDS = [
    "btl", "промо", "промоушн", "ивент", "event", "мерчендайзинг", "merchandising",
    "brand activation", "активация", "дегустация", "семплинг", "промо-акции",
    "трейд маркетинг", "trade marketing", "pos материалы", "стимулирование продаж"
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]
