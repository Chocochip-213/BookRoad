FROM python:3.12-slim

# 2. 환경 변수 설정
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# 3. 작업 디렉토리 설정
WORKDIR /app

# 4. requirements.txt 파일을 먼저 복사하고 라이브러리 설치
# (이렇게 하면 나중에 코드만 바뀔 때 라이브러리를 다시 설치하지 않아 빌드 속도가 빨라집니다)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. 나머지 모든 소스 코드를 컨테이너 안으로 복사
COPY . .

# 6. 이 컨테이너가 실행될 때 기본적으로 실행할 명령 (옵션)
# docker-compose.yml에서 command를 지정했으므로 이 부분은 주석 처리하거나 비워둬도 됩니다.
# CMD ["python", "run_etl.py"]