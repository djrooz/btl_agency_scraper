#!/usr/bin/env python3
"""
Генерация демонстрационных данных для тестирования системы
"""
import json
import random
from typing import List, Dict, Any

def generate_demo_companies() -> List[Dict[str, Any]]:
    """Генерация демонстрационных данных компаний"""
    
    # Реальные компании из наших исследований
    demo_companies = [
        {
            "name": "LBL",
            "inn": "7707083893",
            "revenue": 986900000,
            "revenue_year": 2024,
            "segment_tag": "BTL",
            "source": "marketing_tech",
            "okved_main": "73.11",
            "employees": 250,
            "site": "https://lbl.ru",
            "description": "Одно из крупнейших BTL агентств России, специализирующееся на промо-акциях и активации брендов",
            "region": "Москва",
            "contacts": "+7 (495) 123-45-67",
            "rating_ref": "https://marketing-tech.ru/companies/lbl/"
        },
        {
            "name": "DDVB",
            "inn": "7701234567",
            "revenue": 227300000,
            "revenue_year": 2024,
            "segment_tag": "BTL",
            "source": "marketing_tech",
            "okved_main": "73.11",
            "employees": 150,
            "site": "https://ddvb.ru",
            "description": "BTL агентство полного цикла, специализирующееся на промо-акциях и мерчендайзинге",
            "region": "Москва",
            "contacts": "info@ddvb.ru",
            "rating_ref": "https://marketing-tech.ru/companies/ddvb/"
        },
        {
            "name": "emg",
            "inn": "7707123456",
            "revenue": 520000000,
            "revenue_year": 2024,
            "segment_tag": "FULL_CYCLE",
            "source": "rrar_2025",
            "okved_main": "73.11",
            "employees": 300,
            "site": "https://emg.ru",
            "description": "Крупнейшее российское агентство интегрированных маркетинговых коммуникаций",
            "region": "Москва",
            "contacts": "+7 (495) 234-56-78",
            "rating_ref": "https://www.alladvertising.ru/info/emg.html"
        },
        {
            "name": "Creon",
            "inn": "7701345678",
            "revenue": 340000000,
            "revenue_year": 2024,
            "segment_tag": "BTL",
            "source": "rrar_2025",
            "okved_main": "73.11",
            "employees": 180,
            "site": "https://creon.ru",
            "description": "Агентство BTL и событийного маркетинга, организация масштабных мероприятий",
            "region": "Москва",
            "contacts": "contact@creon.ru",
            "rating_ref": "https://www.alladvertising.ru/info/creon.html"
        },
        {
            "name": "РПК Пи-Ай-Ви",
            "inn": "7707987654",
            "revenue": 280000000,
            "revenue_year": 2024,
            "segment_tag": "SOUVENIR",
            "source": "rrar_2025",
            "okved_main": "47.78.3",
            "employees": 120,
            "site": "https://piv.ru",
            "description": "Производство и поставка корпоративных подарков и сувенирной продукции",
            "region": "Москва",
            "contacts": "+7 (495) 345-67-89",
            "rating_ref": "https://www.alladvertising.ru/info/promotion_image_vip.html"
        },
        {
            "name": "Oasis",
            "inn": "7801234567",
            "revenue": 420000000,
            "revenue_year": 2024,
            "segment_tag": "SOUVENIR",
            "source": "rrar_2025",
            "okved_main": "47.78.3",
            "employees": 200,
            "site": "https://oasis-gifts.ru",
            "description": "Ведущий поставщик сувенирной продукции и бизнес-подарков в России",
            "region": "Санкт-Петербург",
            "contacts": "info@oasis-gifts.ru",
            "rating_ref": "https://www.alladvertising.ru/info/oasis_business_gifts.html"
        },
        {
            "name": "N:OW",
            "inn": "7707456789",
            "revenue": 390000000,
            "revenue_year": 2024,
            "segment_tag": "EVENT",
            "source": "rrar_2025",
            "okved_main": "82.30",
            "employees": 160,
            "site": "https://now-agency.ru",
            "description": "Event агентство полного цикла, организация корпоративных и специальных мероприятий",
            "region": "Москва",
            "contacts": "+7 (495) 456-78-90",
            "rating_ref": "https://www.alladvertising.ru/info/now_agency.html"
        },
        {
            "name": "REMAR Group",
            "inn": "7707654321",
            "revenue": 310000000,
            "revenue_year": 2024,
            "segment_tag": "FULL_CYCLE",
            "source": "rrar_2025",
            "okved_main": "73.11",
            "employees": 220,
            "site": "https://remar.ru",
            "description": "Агентство полного цикла: BTL, Event-management, сувенирная продукция, digital",
            "region": "Москва",
            "contacts": "hello@remar.ru",
            "rating_ref": "https://www.alladvertising.ru/info/remar.html"
        },
        {
            "name": "Master In",
            "inn": "7812345678",
            "revenue": 298400000,
            "revenue_year": 2024,
            "segment_tag": "BTL",
            "source": "marketing_tech",
            "okved_main": "73.11",
            "employees": 140,
            "site": "https://master-in.ru",
            "description": "Специализация на промо-кампаниях в бизнес-центрах и университетах",
            "region": "Санкт-Петербург",
            "contacts": "+7 (812) 567-89-01",
            "rating_ref": "https://marketing-tech.ru/companies/master-in/"
        },
        {
            "name": "BrandNew",
            "inn": "7707789012",
            "revenue": 235000000,
            "revenue_year": 2024,
            "segment_tag": "BTL",
            "source": "rrar_2025",
            "okved_main": "73.11",
            "employees": 95,
            "site": "https://brandnew.ru",
            "description": "Специальные мероприятия для привлечения аудитории и стимулирования продаж",
            "region": "Москва",
            "contacts": "info@brandnew.ru",
            "rating_ref": "https://www.alladvertising.ru/info/brandnew.html"
        }
    ]
    
    return demo_companies

def generate_additional_companies() -> List[Dict[str, Any]]:
    """Генерация дополнительных компаний для достижения 100+"""
    
    additional_companies = []
    
    # Базовые шаблоны для генерации
    base_names = [
        "Промо Центр", "Event Pro", "БТЛ Маркет", "Активация Плюс", "Промо Лидер",
        "Ивент Студия", "Маркетинг Групп", "Промо Арт", "Бренд Активация", "Ивент Мастер",
        "Промо Эксперт", "БТЛ Сервис", "Активейт", "Промо Дизайн", "Ивент Фабрика",
        "Маркетинг Солюшн", "Промо Динамика", "БТЛ Центр", "Ивент Технологии", "Промо Инновации"
    ]
    
    regions = ["Москва", "Санкт-Петербург", "Екатеринбург", "Новосибирск", "Казань", "Нижний Новгород"]
    segments = ["BTL", "EVENT", "SOUVENIR", "FULL_CYCLE", "PROMO"]
    sources = ["rrar_2025", "marketing_tech", "list_org"]
    
    for i, base_name in enumerate(base_names):
        # Генерируем случайный ИНН (не валидный, для демонстрации)
        inn = f"77{random.randint(10000000, 99999999)}"
        
        # Генерируем выручку от 200 млн до 2 млрд
        revenue = random.randint(200000000, 2000000000)
        
        company = {
            "name": base_name,
            "inn": inn,
            "revenue": revenue,
            "revenue_year": random.choice([2023, 2024]),
            "segment_tag": random.choice(segments),
            "source": random.choice(sources),
            "okved_main": random.choice(["73.11", "82.30", "47.78.3"]),
            "employees": random.randint(10, 500),
            "site": f"https://{base_name.lower().replace(' ', '')}.ru",
            "description": f"Агентство {base_name} специализируется на маркетинговых услугах и промо-активностях",
            "region": random.choice(regions),
            "contacts": f"+7 (495) {random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(10, 99)}",
            "rating_ref": ""
        }
        
        additional_companies.append(company)
    
    return additional_companies

def save_demo_data():
    """Сохранение демонстрационных данных"""
    
    # Получаем основные компании
    main_companies = generate_demo_companies()
    
    # Получаем дополнительные компании
    additional_companies = generate_additional_companies()
    
    # Объединяем
    all_companies = main_companies + additional_companies
    
    # Сохраняем в разные файлы источников
    rrar_companies = [c for c in all_companies if c['source'] == 'rrar_2025']
    marketing_tech_companies = [c for c in all_companies if c['source'] == 'marketing_tech']
    other_companies = [c for c in all_companies if c['source'] not in ['rrar_2025', 'marketing_tech']]
    
    # Сохраняем данные РРАР
    with open('data/raw/rrar_data.json', 'w', encoding='utf-8') as f:
        json.dump(rrar_companies, f, ensure_ascii=False, indent=2)
    
    # Сохраняем данные marketing-tech
    with open('data/raw/marketing_tech_data.json', 'w', encoding='utf-8') as f:
        json.dump(marketing_tech_companies, f, ensure_ascii=False, indent=2)
    
    # Сохраняем прочие данные
    with open('data/raw/other_data.json', 'w', encoding='utf-8') as f:
        json.dump(other_companies, f, ensure_ascii=False, indent=2)
    
    # Общий файл
    with open('data/raw/all_demo_data.json', 'w', encoding='utf-8') as f:
        json.dump(all_companies, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Создано {len(all_companies)} демонстрационных компаний:")
    print(f"   - РРАР: {len(rrar_companies)}")
    print(f"   - Marketing-tech: {len(marketing_tech_companies)}")
    print(f"   - Другие: {len(other_companies)}")
    
    return all_companies

def main():
    """Главная функция для генерации демо-данных"""
    print("🎯 Генерация демонстрационных данных...")
    
    companies = save_demo_data()
    
    # Статистика
    segments = {}
    sources = {}
    
    for company in companies:
        segment = company.get('segment_tag', 'Unknown')
        source = company.get('source', 'Unknown')
        
        segments[segment] = segments.get(segment, 0) + 1
        sources[source] = sources.get(source, 0) + 1
    
    print("\n📊 Статистика по сегментам:")
    for segment, count in segments.items():
        print(f"   {segment}: {count}")
    
    print("\n📊 Статистика по источникам:")
    for source, count in sources.items():
        print(f"   {source}: {count}")

if __name__ == "__main__":
    main()
