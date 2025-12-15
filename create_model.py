from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import make_classification
import joblib

# Создаём данные и обучаем модель
X, y = make_classification(n_samples=10000, n_features=20, random_state=42)
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X, y)

# Сохраняем модель
joblib.dump(model, 'model_joblib.pkl')
print("Модель сохранена в файл model_joblib.pkl")
