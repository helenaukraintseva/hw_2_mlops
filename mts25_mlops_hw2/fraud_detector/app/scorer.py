import numpy as np
import logging
from catboost import CatBoostClassifier
import pandas as pd

logger = logging.getLogger(__name__)

class ModelScorer:
    def __init__(self, model_path: str = "/app/models/model.cbm"):
        """Инициализация модели"""
        try:
            self.model = CatBoostClassifier()
            self.model.load_model(model_path)
            logger.info(f"Model loaded from {model_path}")
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
            
    def predict(self, df: pd.DataFrame) -> np.ndarray:
        """Получение предсказаний модели"""
        try:
            # Получаем вероятность класса 1 (fraud)
            predictions = self.model.predict_proba(df)[:, 1]
            return predictions
        except Exception as e:
            logger.error(f"Prediction error: {e}")
            raise