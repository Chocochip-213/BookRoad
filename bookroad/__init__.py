# bookroad/__init__.py

# 이 프로젝트가 시작될 때 Celery 앱을 항상 임포트하도록 합니다.
from.celery_config import app as celery_app

__all__ = ('celery_app',)