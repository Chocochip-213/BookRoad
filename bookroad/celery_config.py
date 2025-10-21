# bookroad/celery_config.py
import os
from celery import Celery

# Django의 settings.py 파일을 Celery 설정 소스로 사용하도록 지정
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'bookroad.settings')

app = Celery('bookroad')

# 'bookroad'라는 네임스페이스를 가진 모든 Django 설정을 로드
app.config_from_object('django.conf:settings', namespace='bookroad')

# 등록된 Django 앱 설정에서 tasks.py 파일을 자동으로 찾음
app.autodiscover_tasks()