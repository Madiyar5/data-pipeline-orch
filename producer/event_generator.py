import json
import time
import uuid
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

# Инициализация
fake = Faker()

# ==================== КОНСТАНТЫ ====================

# Регионы Казахстана с весами (по населению)
REGIONS = [
    "Almaty",      # 30%
    "Astana",      # 25%
    "Shymkent",    # 15%
    "Karaganda",   # 10%
    "Aktobe",      # 5%
    "Taraz",       # 5%
    "Pavlodar",    # 4%
    "Semey",       # 3%
    "Atyrau",      # 2%
    "Kostanay"     # 1%
]
REGION_WEIGHTS = [30, 25, 15, 10, 5, 5, 4, 3, 2, 1]

# Типы событий с весами
EVENT_TYPES = ["call", "sms", "data_session", "balance_recharge", "service_activation"]
EVENT_WEIGHTS = [20, 15, 50, 10, 5]  # data_session - 50%

# functions 

def generate_masked_msisdn():
    """Генерирует маскированный номер: 7701234****"""
    prefix = "770"
    middle = str(random.randint(1000, 9999))
    masked = "****"
    return f"{prefix}{middle}{masked}"

def select_region():
    """Выбирает регион с учетом весов"""
    return random.choices(REGIONS, weights=REGION_WEIGHTS)[0]

def get_time_multiplier():
    """
    Множитель активности по времени суток (UTC+6 для Казахстана)
    
    Пиковые часы:
    - 08:00-10:00 (утро) - 1.5x
    - 12:00-14:00 (обед) - 1.3x
    - 18:00-22:00 (вечер) - 2.0x
    
    Ночь: 00:00-06:00 - 0.3x
    """
    current_hour = (datetime.now(timezone.utc).hour + 6) % 24
    
    if 8 <= current_hour < 10:
        return 1.5
    elif 12 <= current_hour < 14:
        return 1.3
    elif 18 <= current_hour < 22:
        return 2.0
    elif 0 <= current_hour < 6:
        return 0.3
    else:
        return 1.0

# ==================== ГЕНЕРАТОРЫ СОБЫТИЙ ====================

def generate_call_event():
    """Генерирует событие звонка"""
    event_subtype = random.choice(["incoming", "outgoing"])
    # Звонки обычно короче 10 минут (600 секунд)
    duration = random.randint(10, 600)
    
    return {
        "event_id": str(uuid.uuid4()),
        "msisdn": generate_masked_msisdn(),
        "event_type": "call",
        "event_subtype": event_subtype,
        "duration_seconds": duration,
        "data_mb": None,
        "amount": None,
        "region": select_region(),
        "cell_tower_id": random.randint(1000, 9999),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

def generate_sms_event():
    """Генерирует событие SMS"""
    event_subtype = random.choice(["incoming", "outgoing"])
    
    return {
        "event_id": str(uuid.uuid4()),
        "msisdn": generate_masked_msisdn(),
        "event_type": "sms",
        "event_subtype": event_subtype,
        "duration_seconds": None,
        "data_mb": None,
        "amount": None,
        "region": select_region(),
        "cell_tower_id": random.randint(1000, 9999),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

def generate_data_session_event():
    """Генерирует событие интернет-сессии"""
    # Сессии от 1 минуты до 2 часов
    duration = random.randint(60, 7200)
    # Трафик от 1MB до 500MB
    data_mb = round(random.uniform(1, 500), 2)
    
    return {
        "event_id": str(uuid.uuid4()),
        "msisdn": generate_masked_msisdn(),
        "event_type": "data_session",
        "event_subtype": None,
        "duration_seconds": duration,
        "data_mb": data_mb,
        "amount": None,
        "region": select_region(),
        "cell_tower_id": random.randint(1000, 9999),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

def generate_balance_recharge_event():
    """Генерирует событие пополнения баланса"""
    # Пополнения от 500 до 10000 тенге
    amount = random.choice([500, 1000, 2000, 3000, 5000, 10000])
    
    return {
        "event_id": str(uuid.uuid4()),
        "msisdn": generate_masked_msisdn(),
        "event_type": "balance_recharge",
        "event_subtype": None,
        "duration_seconds": None,
        "data_mb": None,
        "amount": float(amount),
        "region": select_region(),
        "cell_tower_id": None,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

def generate_service_activation_event():
    """Генерирует событие активации услуги"""
    return {
        "event_id": str(uuid.uuid4()),
        "msisdn": generate_masked_msisdn(),
        "event_type": "service_activation",
        "event_subtype": None,
        "duration_seconds": None,
        "data_mb": None,
        "amount": None,
        "region": select_region(),
        "cell_tower_id": None,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

# ==================== ГЛАВНАЯ ФУНКЦИЯ ====================

def generate_event():
    """Генерирует случайное событие с учетом весов"""
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    
    if event_type == "call":
        return generate_call_event()
    elif event_type == "sms":
        return generate_sms_event()
    elif event_type == "data_session":
        return generate_data_session_event()
    elif event_type == "balance_recharge":
        return generate_balance_recharge_event()
    elif event_type == "service_activation":
        return generate_service_activation_event()

def main():
    """
    Главная функция - подключается к Kafka и генерирует события
    """
    # Подключение к Kafka
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("🚀 Producer запущен! Генерирую реалистичные телеком-события...")
    print("📊 Распределение:")
    print("   - data_session: 50%")
    print("   - call: 20%")
    print("   - sms: 15%")
    print("   - balance_recharge: 10%")
    print("   - service_activation: 5%")
    print("🏙️  Регионы: Almaty (30%), Astana (25%), и другие")
    print("⏰ Пиковые часы: 08-10, 12-14, 18-22 (больше событий)")
    print("\nНажми Ctrl+C для остановки\n")
    
    event_count = 0
    
    try:
        while True:
            # Получаем множитель времени
            time_multiplier = get_time_multiplier()
            
            # Вычисляем задержку (в пиковые часы быстрее)
            base_delay = 0.5
            delay = base_delay / time_multiplier
            
            # Генерируем событие
            event = generate_event()
            
            # Отправляем в Kafka
            producer.send('telecom_events', value=event)
            
            event_count += 1
            
            # Логируем каждое 10-е событие
            if event_count % 10 == 0:
                current_hour = (datetime.now(timezone.utc).hour + 6) % 24
                print(f"✅ {event_count:6d} событий | "
                      f"Тип: {event['event_type']:18s} | "
                      f"Регион: {event['region']:10s} | "
                      f"Час: {current_hour:02d}:00 (x{time_multiplier})")
            
            # Задержка с учетом времени суток
            time.sleep(delay)
            
    except KeyboardInterrupt:
        print(f"\n🛑 Остановлено. Всего отправлено: {event_count} событий")
        producer.close()

if __name__ == "__main__":
    main()