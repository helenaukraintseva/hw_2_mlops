import pandas as pd
import numpy as np
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DataPreprocessor:
    def __init__(self):
        # Загружаем параметры препроцессинга если нужно
        logger.info("Preprocessor initialized")
        
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Применяет препроцессинг к данным транзакции"""
        df_processed = df.copy()
        
        try:
            # Временные признаки
            if 'transaction_time' in df_processed.columns:
                df_processed['transaction_time'] = pd.to_datetime(df_processed['transaction_time'])
                df_processed['hour'] = df_processed['transaction_time'].dt.hour
                df_processed['day_of_week'] = df_processed['transaction_time'].dt.dayofweek
                df_processed['month'] = df_processed['transaction_time'].dt.month
                df_processed.drop('transaction_time', axis=1, inplace=True)
            
            # Расчет расстояния между клиентом и мерчантом
            if all(col in df_processed.columns for col in ['lat', 'lon', 'merchant_lat', 'merchant_lon']):
                df_processed['distance_km'] = self._calculate_distance(
                    df_processed['lat'], df_processed['lon'],
                    df_processed['merchant_lat'], df_processed['merchant_lon']
                )
            
            # Обработка категориальных переменных
            categorical_columns = df_processed.select_dtypes(include=['object']).columns
            for col in categorical_columns:
                if col != 'transaction_id':  # Сохраняем ID
                    # Простое label encoding для демо
                    df_processed[col] = pd.Categorical(df_processed[col]).codes
            
            # Обработка пропущенных значений
            numeric_columns = df_processed.select_dtypes(include=[np.number]).columns
            df_processed[numeric_columns] = df_processed[numeric_columns].fillna(0)
            
            # Удаляем transaction_id перед скорингом
            if 'transaction_id' in df_processed.columns:
                df_processed = df_processed.drop('transaction_id', axis=1)
                
            logger.debug(f"Preprocessing complete. Shape: {df_processed.shape}")
            return df_processed
            
        except Exception as e:
            logger.error(f"Preprocessing error: {e}")
            raise
    
    def _calculate_distance(self, lat1, lon1, lat2, lon2):
        """Расчет расстояния между координатами"""
        R = 6371  # Радиус Земли в км
        
        lat1_rad = np.radians(lat1)
        lat2_rad = np.radians(lat2)
        delta_lat = np.radians(lat2 - lat1)
        delta_lon = np.radians(lon2 - lon1)
        
        a = np.sin(delta_lat/2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(delta_lon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        
        return R * c