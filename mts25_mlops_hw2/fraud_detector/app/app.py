import json
import logging
import os
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import pandas as pd
from preprocessing import DataPreprocessor
from fraud_detector.app.scorer import ModelScorer

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FraudDetectorService:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.input_topic = os.getenv('INPUT_TOPIC', 'transactions')
        self.output_topic = os.getenv('OUTPUT_TOPIC', 'scoring')
        
        # Инициализация компонентов ML
        self.preprocessor = DataPreprocessor()
        self.scorer = ModelScorer()
        
        # Инициализация Kafka
        self._init_kafka()
        
    def _init_kafka(self):
        """Инициализация Kafka consumer и producer"""
        try:
            self.consumer = KafkaConsumer(
                self.input_topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='fraud-detector-group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            logger.info(f"Connected to Kafka. Consuming from {self.input_topic}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
            
    def process_transaction(self, transaction_data):
        """Обработка одной транзакции"""
        try:
            # Получаем ID транзакции
            transaction_id = transaction_data.get('transaction_id')
            
            # Преобразуем в DataFrame для обработки
            df = pd.DataFrame([transaction_data])
            
            # Препроцессинг
            df_processed = self.preprocessor.transform(df)
            
            # Скоринг
            score = self.scorer.predict(df_processed)[0]
            fraud_flag = 1 if score > 0.98 else 0
            
            # Формируем результат
            result = {
                "transaction_id": transaction_id,
                "score": float(score),
                "fraud_flag": fraud_flag
            }
            
            logger.info(f"Processed transaction {transaction_id}: score={score:.4f}, fraud={fraud_flag}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing transaction: {e}")
            return None
            
    def run(self):
        """Основной цикл обработки сообщений"""
        logger.info("Starting fraud detector service...")
        
        try:
            for message in self.consumer:
                transaction = message.value
                logger.debug(f"Received transaction: {transaction.get('transaction_id')}")
                
                # Обработка транзакции
                result = self.process_transaction(transaction)
                
                if result:
                    # Отправка результата в выходной топик
                    future = self.producer.send(self.output_topic, value=result)
                    
                    # Ждем подтверждения отправки
                    try:
                        record_metadata = future.get(timeout=10)
                        logger.debug(f"Result sent to {record_metadata.topic} partition {record_metadata.partition}")
                    except KafkaError as e:
                        logger.error(f"Failed to send result: {e}")
                        
        except KeyboardInterrupt:
            logger.info("Service stopped by user")
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
        finally:
            self.consumer.close()
            self.producer.close()
            logger.info("Service shut down")

if __name__ == "__main__":
    service = FraudDetectorService()
    service.run()