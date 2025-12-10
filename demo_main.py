#!/usr/bin/env python3
"""
Демонстрационная версия главного модуля с использованием готовых данных
"""
import os
import json
import time
from pathlib import Path
from typing import List, Dict, Any

from src.utils import main_logger
from src.processors import DataCleaner, DuplicateHandler
from config import config

def load_demo_data() -> List[Dict[str, Any]]:
    """
    Загрузка демонстрационных данных из файлов
    
    Returns:
        Список всех демонстрационных компаний
    """
    all_companies = []
    
    main_logger.info("Загрузка демонстрационных данных")
    
    # Загружаем данные из файлов
    data_files = [
        'data/raw/rrar_data.json',
        'data/raw/marketing_tech_data.json',
        'data/raw/other_data.json'
    ]
    
    for file_path in data_files:
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    companies = json.load(f)
                    all_companies.extend(companies)
                    main_logger.info(f"Загружено из {file_path}: {len(companies)} компаний")
            except Exception as e:
                main_logger.error(f"Ошибка загрузки {file_path}: {e}")
    
    main_logger.info(f"Всего загружено демонстрационных данных: {len(all_companies)} записей")
    return all_companies

def process_demo_data(companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Обработка демонстрационных данных
    
    Args:
        companies: Сырые данные компаний
        
    Returns:
        Обработанные данные компаний
    """
    main_logger.info("Начинаем обработку демонстрационных данных")
    
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
        
        # Сохраняем также образец в формате Excel
        excel_file = output_file.replace('.csv', '_sample.xlsx')
        df.head(20).to_excel(excel_file, index=False, engine='openpyxl')
        main_logger.info(f"Образец данных сохранен в Excel: {excel_file}")
        
    except Exception as e:
        main_logger.error(f"Ошибка создания финального CSV: {e}")

def print_statistics(df) -> None:
    """
    Вывод статистики по собранным данным
    
    Args:
        df: DataFrame с данными компаний
    """
    print("\n" + "="*60)
    print("СТАТИСТИКА ДЕМОНСТРАЦИОННЫХ ДАННЫХ")
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
            
            # Компании с выручкой >= 200 млн
            big_companies = df[df['revenue'] >= 200_000_000]
            print(f"  Компаний с выручкой ≥ 200 млн: {len(big_companies)}")
    
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

def show_sample_data(companies: List[Dict[str, Any]], count: int = 5) -> None:
    """
    Показ образца данных
    
    Args:
        companies: Список компаний
        count: Количество компаний для показа
    """
    print("\n" + "="*60)
    print("ОБРАЗЕЦ ДАННЫХ")
    print("="*60)
    
    for i, company in enumerate(companies[:count]):
        print(f"\n{i+1}. {company.get('name', 'N/A')}")
        print(f"   ИНН: {company.get('inn', 'N/A')}")
        print(f"   Выручка: {company.get('revenue', 0):,.0f} руб. ({company.get('revenue_year', 'N/A')})")
        print(f"   Сегмент: {company.get('segment_tag', 'N/A')}")
        print(f"   Регион: {company.get('region', 'N/A')}")
        print(f"   Сайт: {company.get('site', 'N/A')}")
        print(f"   Источник: {company.get('source', 'N/A')}")
    
    if len(companies) > count:
        print(f"\n   ... и ещё {len(companies) - count} компаний")
    
    print("="*60 + "\n")

def main():
    """Главная функция демонстрации"""
    start_time = time.time()
    
    print("🎯 ДЕМОНСТРАЦИЯ: Обработка данных о BTL и маркетинговых агентствах")
    print("="*60)
    
    try:
        # Загружаем демонстрационные данные
        companies = load_demo_data()
        
        if not companies:
            print("❌ Не найдены демонстрационные данные. Запустите demo_data.py")
            return
        
        # Обработка данных
        processed_companies = process_demo_data(companies)
        
        if not processed_companies:
            main_logger.error("Не получено обработанных данных")
            return
        
        # Показываем образец данных
        show_sample_data(processed_companies)
        
        # Генерация финального CSV
        generate_final_csv(processed_companies)
        
        # Финальная статистика
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n✅ Демонстрация завершена успешно!")
        print(f"⏱️  Время выполнения: {duration:.1f} секунд")
        print(f"📊 Обработано компаний: {len(processed_companies)}")
        print(f"💾 Результат сохранен в: {config.output.get('csv_file', 'data/companies.csv')}")
        
        # Показываем файлы
        print(f"\n📁 Созданные файлы:")
        files_to_check = [
            'data/companies.csv',
            'data/companies_sample.xlsx',
            'data/interim/cleaned_data.csv',
            'data/interim/deduplicated_data.csv'
        ]
        
        for file_path in files_to_check:
            if os.path.exists(file_path):
                size = os.path.getsize(file_path)
                print(f"   ✓ {file_path} ({size:,} байт)")
        
    except KeyboardInterrupt:
        main_logger.info("Демонстрация прервана пользователем")
        print("\n❌ Демонстрация прервана пользователем")
        
    except Exception as e:
        main_logger.error(f"Ошибка демонстрации: {e}")
        print(f"\n❌ Ошибка демонстрации: {e}")
        raise

if __name__ == "__main__":
    main()
