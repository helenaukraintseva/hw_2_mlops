import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import uuid
import os
from datetime import datetime

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'transactions')

st.set_page_config(
    page_title="Fraud Detection Interface",
    page_icon="🔍",
    layout="wide"
)

st.title("🔍 Fraud Detection System")
st.markdown("Upload transaction data for real-time fraud detection")

# Инициализация Kafka Producer
@st.cache_resource
def get_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        return producer
    except Exception as e:
        st.error(f"Failed to connect to Kafka: {e}")
        return None

# Загрузка файла
uploaded_file = st.file_uploader(
    "Choose a CSV file", 
    type="csv",
    help="Upload test.csv file with transaction data"
)

if uploaded_file is not None:
    # Чтение данных
    df = pd.read_csv(uploaded_file)
    
    st.subheader("📊 Data Preview")
    st.write(f"Shape: {df.shape}")
    st.dataframe(df.head(10))
    
    # Кнопка отправки
    if st.button("🚀 Send to Kafka", type="primary"):
        producer = get_kafka_producer()
        
        if producer:
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            sent_count = 0
            failed_count = 0
            
            for idx, row in df.iterrows():
                try:
                    # Добавляем уникальный ID
                    transaction = row.to_dict()
                    transaction['transaction_id'] = str(uuid.uuid4())
                    
                    # Отправка в Kafka
                    future = producer.send(KAFKA_TOPIC, value=transaction)
                    future.get(timeout=10)
                    
                    sent_count += 1
                    
                except Exception as e:
                    failed_count += 1
                    st.error(f"Failed to send transaction {idx}: {e}")
                
                # Обновление прогресса
                progress = (idx + 1) / len(df)
                progress_bar.progress(progress)
                status_text.text(f"Processing: {idx + 1}/{len(df)} transactions")
            
            producer.flush()
            
            # Результаты
            st.success(f"✅ Complete! Sent: {sent_count}, Failed: {failed_count}")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Successful", sent_count)
            with col2:
                st.metric("Failed", failed_count)

# Информация
with st.sidebar:
    st.header("ℹ️ Information")
    st.markdown("""
    ### How to use:
    1. Upload a CSV file with transactions
    2. Preview the data
    3. Click 'Send to Kafka' to start processing
    
    ### Monitoring:
    - Kafka UI: [http://localhost:8080](http://localhost:8080)
    - Check `transactions` and `scoring` topics
    
    ### Data format:
    The CSV should contain columns like:
    - transaction_time
    - amount
    - lat, lon
    - merchant_lat, merchant_lon
    - etc.
    """)