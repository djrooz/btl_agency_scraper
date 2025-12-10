#!/usr/bin/env python3
"""
Главный модуль для сбора данных о BTL и маркетинговых агентствах
"""
import os
import json
import time
from pathlib import Path
from typing import List, Dict, Any

from src.utils import main_logger
from src.scrapers import RRARScraper, MarketingTechScraper
from src.scrapers.fns_api_client import FNSAPIClient
from src.processors import DataCleaner, DuplicateHandler
from config import config

def setup_directories():
    """Создание необходимых директорий"""
    directories = [
        "data",
        "data/raw",
        "data/interim",
        "logs"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
    
    main_logger.info("Директории созданы/проверены")

def collect_data_from_sources() -> List[Dict[str, Any]]:
    """
    Сбор данных из всех источников
    
    Returns:
        Список всех собранных компаний
    """
    all_companies = []
    
    main_logger.info("Начинаем сбор данных из источников")
    
    # 1. Парсинг РРАР рейтингов
    try:
        main_logger.info("Сбор данных из РРАР...")
        rrar_scraper = RRARScraper()
        rrar_companies = rrar_scraper.scrape_all()
        
        if rrar_companies:
            rrar_scraper.save_raw_data(rrar_companies)
            all_companies.extend(rrar_companies)
            main_logger.info(f"Получено из РРАР: {len(rrar_companies)} компаний")
        
    except Exception as e:
        main_logger.error(f"Ошибка сбора данных РРАР: {e}")
    
    # 2. Парсинг marketing-tech.ru
    try:
        main_logger.info("Сбор данных из marketing-tech...")
        marketing_scraper = MarketingTechScraper()
        marketing_companies = marketing_scraper.scrape_all()
        
        if marketing_companies:
            marketing_scraper.save_raw_data(marketing_companies)
            all_companies.extend(marketing_companies)
            main_logger.info(f"Получено из marketing-tech: {len(marketing_companies)} компаний")
        
    except Exception as e:
        main_logger.error(f"Ошибка сбора данных marketing-tech: {e}")
    
    # 3. Обогащение данных через ФНС API
    try:
        main_logger.info("Обогащение данных через ФНС API...")
        fns_client = FNSAPIClient()
        
        # Извлекаем уникальные ИНН для запроса
        inns = set()
        for company in all_companies:
            inn = company.get('inn')
            if inn and len(inn) in [10, 12]:
                inns.add(inn)
        
        if inns:
            main_logger.info(f"Обогащаем данные для {len(inns)} ИНН")
            fns_companies = fns_client.batch_get_companies(list(inns))
            
            if fns_companies:
                fns_client.save_raw_data(fns_companies)
                
                # Объединяем данные ФНС с существующими записями
                all_companies = merge_fns_data(all_companies, fns_companies)
                main_logger.info(f"Обогащено данными ФНС: {len(fns_companies)} компаний")
        
    except Exception as e:
        main_logger.error(f"Ошибка обогащения данных ФНС: {e}")
    
    main_logger.info(f"Всего собрано сырых данных: {len(all_companies)} записей")
    return all_companies

def merge_fns_data(companies: List[Dict[str, Any]], fns_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Объединение данных компаний с данными ФНС
    
    Args:
        companies: Список компаний
        fns_data: Данные ФНС
        
    Returns:
        Обогащенный список компаний
    """
    # Создаем индекс по ИНН для быстрого поиска
    fns_index = {item['inn']: item for item in fns_data if item.get('inn')}
    
    for company in companies:
        inn = company.get('inn')
        if inn and inn in fns_index:
            fns_record = fns_index[inn]
            
            # Обогащаем данные, не перезаписывая существующие
            for key, value in fns_record.items():
                if key not in company or not company[key]:
                    company[key] = value
    
    return companies

def process_data(companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Обработка и очистка данных
    
    Args:
        companies: Сырые данные компаний
        
    Returns:
        Обработанные данные компаний
    """
    main_logger.info("Начинаем обработку данных")
    
    # 1. Очистка данных
    cleaner = DataCleaner()
    cleaned_companies = cleaner.clean_companies_data(companies)
    
    if cleaned_companies:
        cleaner.save_cleaned_data(cleaned_companies)
    
    # 2. Фильтрация по релевантности
    relevant_companies = cleaner.filter_by_relevance(cleaned_companies)
    
    # 3. Удаление дубликатов
    dedup_handler = DuplicateHandler()
    unique_companies = dedup_handler.remove_duplicates(relevant_companies)
    
    if unique_companies:
        dedup_handler.save_deduplicated_data(unique_companies)
    
    # 4. Финальная фильтрация по выручке
    final_companies = filter_by_revenue(unique_companies)
    
    # Статистика обработки
    stats = dedup_handler.get_duplicate_statistics(len(relevant_companies), len(unique_companies))
    main_logger.info(f"Статистика дедупликации: {stats}")
    
    main_logger.info(f"Финальное количество компаний: {len(final_companies)}")
    
    return final_companies

def filter_by_revenue(companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Фильтрация компаний по минимальной выручке
    
    Args:
        companies: Список компаний
        
    Returns:
        Отфильтрованный список компаний
    """
    min_revenue = config.min_revenue
    filtered = []
    
    for company in companies:
        revenue = company.get('revenue', 0)
        
        # Пропускаем компании с нулевой выручкой (данные могут быть неполными)
        # или с выручкой выше порога
        if revenue == 0 or revenue >= min_revenue:
            filtered.append(company)
    
    main_logger.info(f"После фильтрации по выручке ≥{min_revenue:,}: {len(filtered)} компаний")
    
    return filtered

def generate_final_csv(companies: List[Dict[str, Any]]) -> None:
    """
    Генерация финального CSV файла
    
    Args:
        companies: Обработанные данные компаний
    """
    try:
        import pandas as pd
        
        if not companies:
            main_logger.warning("Нет данных для создания CSV")
            return
        
        # Создаем DataFrame
        df = pd.DataFrame(companies)
        
        # Обеспечиваем наличие всех обязательных колонок
        required_columns = [
            'inn', 'name', 'revenue_year', 'revenue', 'segment_tag', 'source'
        ]
        
        optional_columns = [
            'okved_main', 'employees', 'site', 'description', 'region', 'contacts', 'rating_ref'
        ]
        
        all_columns = required_columns + optional_columns
        
        for col in all_columns:
            if col not in df.columns:
                df[col] = ''
        
        # Сортируем по выручке (по убыванию)
        df = df.sort_values('revenue', ascending=False)
        
        # Сохраняем финальный CSV
        output_file = config.output.get('csv_file', 'data/companies.csv')
        df[all_columns].to_csv(output_file, index=False, encoding='utf-8')
        
        main_logger.info(f"Финальный CSV сохранен: {output_file}")
        main_logger.info(f"Количество компаний в файле: {len(df)}")
        
        # Выводим статистику
        print_statistics(df)
        
    except Exception as e:
        main_logger.error(f"Ошибка создания финального CSV: {e}")

def print_statistics(df) -> None:
    """
    Вывод статистики по собранным данным
    
    Args:
        df: DataFrame с данными компаний
    """
    print("\n" + "="*60)
    print("СТАТИСТИКА СОБРАННЫХ ДАННЫХ")
    print("="*60)
    
    print(f"Общее количество компаний: {len(df)}")
    
    # Статистика по сегментам
    if 'segment_tag' in df.columns:
        segment_stats = df['segment_tag'].value_counts()
        print("\nРаспределение по сегментам:")
        for segment, count in segment_stats.items():
            print(f"  {segment}: {count}")
    
    # Статистика по источникам
    if 'source' in df.columns:
        source_stats = df['source'].value_counts()
        print("\nРаспределение по источникам:")
        for source, count in source_stats.items():
            print(f"  {source}: {count}")
    
    # Статистика по выручке
    if 'revenue' in df.columns:
        revenue_stats = df[df['revenue'] > 0]['revenue']
        if len(revenue_stats) > 0:
            print(f"\nСтатистика по выручке ({len(revenue_stats)} компаний с данными):")
            print(f"  Минимальная: {revenue_stats.min():,.0f} руб.")
            print(f"  Максимальная: {revenue_stats.max():,.0f} руб.")
            print(f"  Средняя: {revenue_stats.mean():,.0f} руб.")
            print(f"  Медианная: {revenue_stats.median():,.0f} руб.")
    
    # Статистика по регионам
    if 'region' in df.columns:
        region_stats = df[df['region'] != '']['region'].value_counts().head(5)
        if len(region_stats) > 0:
            print("\nТоп-5 регионов:")
            for region, count in region_stats.items():
                print(f"  {region}: {count}")
    
    # Полнота данных
    print(f"\nПолнота данных:")
    for col in ['inn', 'revenue', 'site', 'contacts', 'okved_main']:
        if col in df.columns:
            filled = len(df[df[col] != ''])
            percentage = (filled / len(df)) * 100
            print(f"  {col}: {filled}/{len(df)} ({percentage:.1f}%)")
    
    print("="*60 + "\n")

def main():
    """Главная функция"""
    start_time = time.time()
    
    print("🚀 Запуск сбора данных о BTL и маркетинговых агентствах")
    print("="*60)
    
    try:
        # Настройка окружения
        setup_directories()
        
        # Сбор данных
        companies = collect_data_from_sources()
        
        if not companies:
            main_logger.error("Не удалось собрать данные ни из одного источника")
            return
        
        # Обработка данных
        processed_companies = process_data(companies)
        
        if not processed_companies:
            main_logger.error("Не получено обработанных данных")
            return
        
        # Генерация финального CSV
        generate_final_csv(processed_companies)
        
        # Финальная статистика
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n✅ Сбор данных завершен успешно!")
        print(f"⏱️  Время выполнения: {duration:.1f} секунд")
        print(f"📊 Собрано компаний: {len(processed_companies)}")
        print(f"💾 Результат сохранен в: {config.output.get('csv_file', 'data/companies.csv')}")
        
    except KeyboardInterrupt:
        main_logger.info("Выполнение прервано пользователем")
        print("\n❌ Выполнение прервано пользователем")
        
    except Exception as e:
        main_logger.error(f"Критическая ошибка: {e}")
        print(f"\n❌ Критическая ошибка: {e}")
        raise

if __name__ == "__main__":
    main()
