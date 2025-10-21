FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 'your_main_script.py' 부분은 실제 실행할 파이썬 파일명으로 변경하세요.
#CMD ["python", "your_main_script.py"]