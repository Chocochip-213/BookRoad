import os
import pandas as pd
from sqlalchemy import create_engine, text
import re
import logging

# --- (★ 추가) 임베딩 및 RAG DB 적재를 위한 라이브러리 ---
from sentence_transformers import SentenceTransformer
from sqlalchemy import Column, Integer, String, MetaData, Table, Sequence
from pgvector.sqlalchemy import Vector
import numpy as np

# --- 1. 보고서 섹션 1: 전처리 및 노이즈 필터링 ---
NOISE_PATTERNS = [
    # (★ 수정: 기존의 포괄적인 첫 번째 규칙을 더 세분화하고, 새로운 패턴을 대거 추가)
    re.compile(
        r'^\s*(옮긴이 머리말|베타리더|감수의 글|감사의 글|지은이의 말|옮긴이의 말|저자 소개|역자 서문|책머리에|이 책에 대하여|찾아보기|목차|Contents|서문|들어가며|추천사|머리말|서언|발간사|프롤로그|Prologue|맺는 말|에필로그|부록|감사의 말|참고 문헌|연보|해설|주석|편집자의 말|추천의 글|지은이의 글|저자 서문|역자의 말|작품 해설|작가 연보|지은이 소개|옮긴이 소개|지은이 머리말)',
        re.IGNORECASE),
    re.compile(r'^(Exercise|연습문제|이것만은 알고 갑시다)', re.IGNORECASE),
    re.compile(r'^[\s·ㆍ]*(연습문제|요약|핵심정리|확인 문제)[\s·ㆍ]*$', re.IGNORECASE),  # ㆍ 연습문제
    # --- (★ 추가된 노이즈 패턴 - 2025-10-22) ---
    re.compile(r'^\s*([0-9]+판|한국어판|개정판)\s*(서문|머리말|을 내며)', re.IGNORECASE),  # 3판 서문, 한국어판 서문, 개정판을 내며
    re.compile(
        r'^\s*(To (Everyone|Educators|Students)|Acknowledgments|Final Words|References|Introduction|PREFACE|Summary|Index|Glossary)',
        re.IGNORECASE),  # English noise
    re.compile(
        r'^\s*(이 책을 (보는|읽는) (방법|법)|이 책의 (사용|활용)법|이 책의 (사용|목적|구성|특징)|시작하기 전에|주의|저작권 안내|상세 변경 이력|학습 (방법|가이드|로드맵|지원 안내)|강의 (계획|보조 자료)|일러두기|등장인물|용어 (설명|해설|대역표)|(지은이|옮긴이|감수자|기술 감수자|저자|역자|작가|베타리더|리뷰어|편집자).*(소개|의 글|주|서문|후기|머리말|대담|인터뷰|추천사)|표지에 대하여|해제|발문|서론|서설|개요|도입|시작하며|들어가기 전에|글을 (열며|시작하며|내면서)|책을 (내면서|펴내며|시작하며|머리에)|여는 글|책 머리에|숲과 나무 이야기)',
        re.IGNORECASE),  # 각종 한국어 머리말/꼬리말
    re.compile(r'^\s*\[.*\]\s*$'),  # [7가지 키워드...], [시험지] 등 대괄호로만 묶인 라인
    re.compile(r'^\s*\[(문제|칼럼|실습|Do it!|LAB|응용 예제|프로젝트)'),  # [문제]_01, [칼럼] ...
    re.compile(r'^(OMR 답안지|// 나눗셈 연산자|논\.설\.해\.변\.책|서\.발)'),  # Added 서.발
    re.compile(r'^={2,}.*={2,}$'),  # ==== 1권 ====
    re.compile(r'^(부록)\s+[A-Z]\.?'),  # 부록 A.
    re.compile(r'^\s*<표.*>.*$'),  # <표1-1>
    re.compile(r'^\s*<그림.*>.*$'),  # <그림1-1>
    re.compile(r'^#\s*예제\d+'),  # # 예제82
    re.compile(r'^DAY\s*\d+'),  # DAY 01
    re.compile(r'^\d+화\s+'),  # 1화
    re.compile(r'^\s*\|.*\|\s*$'),  # | 참고문헌 |
    re.compile(r'^\s*\[붙임 \d+\]'),  # [붙임 1]
    re.compile(r'^\s*(￭|●|◎|::|\+\+|◇)\s*(연습|프로그래밍|퀴즈)'),  # ￭연습문제 등
    re.compile(r'^\s*Step\d+'),  # Step1
    re.compile(r'^\s*WEEK'),  # WEEK 00
    re.compile(r'^\s*&lt;.*&gt;'),  # &lt;1부&gt;
    re.compile(r'^\s*#\d+'),  # #11 JShell
    # --- (★ 추가된 노이즈 패턴 - 2025-10-22) ---
    re.compile(r'^\s*Value Chain.*BSC.*$', re.IGNORECASE),  # 요약 라인
    re.compile(r'^\s*[A-Z][가-힣]+ / [A-Z][가-힣]+.*$', re.IGNORECASE),  # A개요 / B경영전략...
    re.compile(r'^\s*\(전면개정판\)\s*핵심 정보통신기술 총서.*$', re.IGNORECASE),
    re.compile(r'^\s*이 책에서 소개하는 차트.*$', re.IGNORECASE),
    re.compile(r'^\s*이순신론 / 이민수'),  # 사용자 요청
    re.compile(r'^\s*최신 AI 랭킹 사이트.*$', re.IGNORECASE),
    re.compile(r'^\s*전문 용어 잠깐 알아보기', re.IGNORECASE),
    re.compile(r'^\s*생성형 AI 도구 100선 요약표', re.IGNORECASE),
    re.compile(r'^\s*리포트 : AI 도구 100선.*$', re.IGNORECASE),
    re.compile(r'^\s*(이 책은 데이터베이스를 처음 공부하는|모바일 웹에 빠져 보세요|통계학에 임하는 여러분의 두뇌).*$'),  # 책 소개
    re.compile(r'^\s*(Part|PART|LC|RC)\s*$', re.IGNORECASE),  # "Part"
    re.compile(r'^\s*(LC|RC).*(Part|기초|학습)', re.IGNORECASE),  # "RC 기초학습Part"
]

# --- 2. 보고서 섹션 2.2: 계층적 패턴 규칙집 ---
PATTERNS_RULEBOOK = [
    # (Level, Regex) - (보고서 '표 1' 기반)
    # (★ 수정: 2025-10-22 - 새로운 Level 1 패턴 추가 및 기존 패턴 수정)
    # Level 1: Part / 부 / Ⅰ / 권 / 마당
    (1, re.compile(r"^\s*권\s*(?P<number>\d+)\s*(?P<title>.*)")),  # 권1
    (1, re.compile(r"^\s*제\s*(?P<number>\d+)\s*부:?\s*(?P<title>.*)")),  # 제1부
    (1, re.compile(r"^\s*(?P<number>\d+)\s*부\.?\s*(?P<title>.*)")),  # 1부.
    (1, re.compile(r"^\s*\[\s*(?:PART|Part)\s*(?P<number>\d+)\s*\]\s*(?P<title>.*)", re.IGNORECASE)),  # [PART 1]
    (1, re.compile(r"^\s*(?:Part|PART|부)\s*(?P<number>[IVX\d]+)\.?\s+(?P<title>.*)", re.IGNORECASE)),
    # Part 1, Part1, 부 I
    (1, re.compile(r"^\s*첫째마당\s*\|?\s*(?P<title>.*)")),  # 첫째마당 | ...
    (1, re.compile(r"^\s*(?P<number>[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅪⅫIVX]+)\s*\.?\s+(?P<title>.*)")),  # Ⅰ. ... (★ IVX 추가)
    (1, re.compile(r"^\s*[A-Z]\s+(?P<title>\S.*)")),  # A 기업정보시스템 (★ \S.*로 수정)
    (1, re.compile(r"^\s*(?:Section|STAGE|Lesson|레슨)\s+(?P<number>\d+)\s*:?\s*(?P<title>.*)", re.IGNORECASE)),
    # Section 1, Lesson 01, 레슨 1 (★ title 캡처 추가)

    # Level 2: Chapter / 장 / 1.
    # (★ 수정: 2025-10-22 - '장' 뒤 공백(\s*) 및 '.' 뒤 공백(\s*) 처리)
    (2, re.compile(r"^\s*(?:Chapter|CHAPTER|Chpater)\s*(?P<number>\d+)\.?\s*(?P<title>.*)", re.IGNORECASE)),
    # Chapter 1, Chpater 1
    (2, re.compile(r"^\s*▣\s*(?P<number>\d+)\s*장:?\s*(?P<title>.*)")),  # ▣ 01장:
    (2, re.compile(r"^\s*\(\s*(?P<number>\d+)\s*장\s*\):?\s*(?P<title>.*)")),  # (1장)
    (2, re.compile(r"^\s*(?P<number>\d+)\s*장\.?:?\s*(?P<title>.*)")),  # 6장, 01장., 1장. (★ \s+ -> \s*)
    (2, re.compile(r"^\s*제\s*(?P<number>\d+)\s*장\.?:?\s*(?P<title>.*)")),  # 제1장, 제1장. (★ \s+ -> \s*)
    (2, re.compile(r"^\s*(?P<number>\d+)\.\s*(?P<title>[^.\d].*)")),  # 1. 운영체제의 개요 (★ \s+ -> \s*)
    (2, re.compile(r"^\s*①|②|③|④|⑤|⑥|⑦|⑧|⑨|⑩|⑪|⑫\s+(?P<title>.*)")),  # ① 기초통계이론
    (2, re.compile(r"^\s*(?P<number>\d+)\s*장\.?\s*(?P<title>)$")),  # "1장." (제목이 다음 줄에 오는 경우)

    # Level 3: Section / 절 / [001]
    (3, re.compile(r"^\s*(?P<number>\d+)\s*절\.?\s*(?P<title>.*)")),  # 1절 (★ \s+ -> \s*)
    (3, re.compile(r"^\s*\[(?P<number>\d+)\]\s+(?P<title>.*)")),  # [001]
    (3, re.compile(r"^\s*제\s*(?P<number>\d+)\s*회:?\s+(?P<title>.*)")),  # 제1회
    (3, re.compile(r"^\s*(?P<number>\d+)\s+(?P<title>[a-zA-Z_가-힣].*)")),  # 3) ... (Level 4보다 낮은 우선순위)

    # Level 4: 1.2.1 / 1.2 / 1-1 / 1.1.1.
    (4, re.compile(r"^\s*(?P<number>\d+\.\d+\.\d+\.\d+)\.?\s+(?P<title>.*)")),  # 1.1.1.1
    (4, re.compile(r"^\s*(?P<number>\d+\.\d+\.\d+)\.?\s+(?P<title>.*)")),  # 1.1.1
    (4, re.compile(r"^\s*(?P<number>\d+\.\d+)\.?\s+(?P<title>.*)")),  # 1.1
    (4, re.compile(r"^\s*(?P<number>[\d\w]+-[\d\w]+)\s*:?\s+(?P<title>.*)")),  # 04-4, 1-1, 1-연초재배

    # Level 5: 1) / (1) / (a)
    (5, re.compile(r"^\s*\d+\)\s+(?P<title>.*)")),  # 1)
    (6, re.compile(r"^\s*\(\d+\)\s+(?P<title>.*)")),  # (1)
    (7, re.compile(r"^\s*\([a-zA-Z]\)\s+(?P<title>.*)")),  # (a)
]

# --- 3. 보고서 섹션 2.3: Fallback 패턴 ---
FALLBACK_PATTERN = re.compile(r"^\s*(?P<title>\S.*)")

# --- (★ 신규) 모델 로딩 (스크립트 시작 시 한 번만 로드) ---
# 보고서 5.1 [cite: 157]의 모델 사용
logging.info("Loading sentence transformer model...")
try:
    EMBEDDING_MODEL = SentenceTransformer('jhgan/ko-sroberta-multitask')
    logging.info("Embedding model loaded successfully.")
except Exception as e:
    logging.error(f"Failed to load embedding model: {e}")
    EMBEDDING_MODEL = None


def extract_raw_tocs():
    """
    ingestion DB에서 원본 목차 및 요약 데이터를 추출하여 Pandas DataFrame으로 반환합니다.
    (★ full_description, publisher_description 컬럼 추가)
    """
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    db_host = "postgres_ingestion_db"  # docker-compose.yml의 서비스 이름
    db_port = "5432"

    logging.info("Connecting to ingestion database...")
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        )

        # (★ 수정) summary, full_description, publisher_description을 모두 조회
        query = text(
            "SELECT isbn, title, raw_toc, summary, full_description, publisher_description "
            "FROM books_book WHERE raw_toc IS NOT NULL AND raw_toc != ''"
        )

        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
        logging.info(f"Successfully extracted {len(df)} books with TOCs and descriptions.")
        return df
    except Exception as e:
        logging.error(f"Error connecting to or extracting from ingestion DB: {e}")
        return pd.DataFrame()


def preprocess_line(line):
    """
    보고서 섹션 1에 따라 단일 라인을 전처리합니다.
    """
    # 1. HTML 태그 제거
    line = re.sub(r'<[^>]+>', '', line)

    # 1.5. (★ 추가) 선행 특수 문자( _, ■, •, ㆍ 등) 제거 (로그에서 발견된 문제)
    line = re.sub(r'^[\s_\-■•ㆍ]+', '', line).strip()

    # 2. 페이지 번호 제거 (예: ... 123 또는 27)
    # (★ 수정: ... 점이 없거나 공백이 하나만 있는 경우도 제거)
    line = re.sub(r'([\s.]{2,}|[ \t]+)(([0-9xvi]+)|(\d{1,3}(?:,\d{3})*))(\s*</?b>)?$', '', line,
                  flags=re.IGNORECASE).strip()

    # 3. 라인 자체가 페이지 번호인 경우 (가끔 발생)
    if re.fullmatch(r'([0-9xvi]+)', line, flags=re.IGNORECASE):
        return None

    # 4. 공백 정규화
    line = ' '.join(line.split())

    # 5. 빈 라인 제거
    if not line:
        return None

    # 6. 노이즈 필터링 (★ 업데이트된 패턴 리스트 사용)
    for pattern in NOISE_PATTERNS:
        if pattern.search(line):
            logging.debug(f"Filtered noise line: {line}")
            return None

    return line


def parse_book_toc(raw_toc, isbn):
    """
    보고서 섹션 2.3의 '상태 기반 파서' 구현.
    책 한 권의 전체 raw_toc를 받아 계층 구조를 파싱합니다.
    (★ AttributeError: 'NoneType' 수정 포함)
    """
    lines = raw_toc.splitlines()

    root_node = {"isbn": isbn, "number": "0", "title": "BOOK_ROOT", "level": 0, "children": [], "source_line": 0}
    stack = [root_node]

    parsed_nodes = []
    failed_lines = []

    for i, raw_line in enumerate(lines):
        line_num = i + 1
        line = preprocess_line(raw_line)

        if not line:  # 전처리 결과 빈 라인이거나 노이즈
            continue

        matched = False
        # 1. (보고서 섹션 2.2) 규칙집 순회 (★ 업데이트된 규칙집 사용)
        for (level, pattern) in PATTERNS_RULEBOOK:
            match = pattern.match(line)
            if match:
                data = match.groupdict()

                # --- (★ 수정된 부분: NoneType 오류 방지) ---
                new_node = {
                    "isbn": isbn,
                    "number": (data.get("number") or "").strip(),
                    "title": (data.get("title") or "").strip(),
                    "level": level,
                    "children": [],  # 하위 노드를 가질 수 있음
                    "source_line": line_num
                }
                # --- (수정 끝) ---

                while stack[-1]["level"] >= level:
                    stack.pop()

                parent = stack[-1]
                parent["children"].append(new_node)
                stack.append(new_node)
                parsed_nodes.append(new_node)
                matched = True
                break

        if not matched:
            # 3. (보고서 ) Fallback: 부제/연속 라인 처리
            match = FALLBACK_PATTERN.match(line)
            if match:
                current_node = stack[-1]
                if current_node["level"] > 0:
                    subtitle = match.group('title').strip()
                    if not current_node["title"]:
                        current_node["title"] = subtitle
                    else:
                        current_node["title"] += f" : {subtitle}"
                    for node in reversed(parsed_nodes):
                        if node["source_line"] == current_node["source_line"]:
                            node["title"] = current_node["title"]
                            break
                    matched = True

        if not matched:
            logging.warning(f"Unmatched line for ISBN {isbn} (Line {line_num}): {line}")
            failed_lines.append({
                "isbn": isbn,
                "line_num": line_num,
                "line_content": line
            })

    return parsed_nodes, failed_lines


def run_parsing_pipeline(df):
    """
    모든 책 DataFrame을 순회하며 '상태 기반 파서'를 실행합니다.
    (기존 parse_all_tocs 대체)
    """
    logging.info("Starting new hierarchical parsing pipeline...")

    all_successful_nodes = []
    all_failed_lines = []

    for index, row in df.iterrows():
        raw_toc = row['raw_toc']
        book_isbn = row['isbn']

        if not isinstance(raw_toc, str):
            continue

        nodes, failures = parse_book_toc(raw_toc, book_isbn)

        all_successful_nodes.extend(nodes)
        all_failed_lines.extend(failures)

    logging.info(
        f"Hierarchical parsing complete. Success: {len(all_successful_nodes)} nodes, Failure: {len(all_failed_lines)} lines.")
    return all_successful_nodes, all_failed_lines


def save_results(successful_nodes, failed_lines, output_dir="parsing_results"):
    """
    파싱 성공 노드(CSV) 및 실패 라인(CSV, log)을 파일로 저장합니다.
    (★ DataFrame 생성 방식 개선)
    """
    os.makedirs(output_dir, exist_ok=True)

    if successful_nodes:
        logging.info(f"Saving {len(successful_nodes)} successful nodes to {output_dir}/structured_toc_nodes.csv...")
        try:
            cols = ["isbn", "level", "number", "title", "source_line"]
            df_success = pd.DataFrame(successful_nodes, columns=cols)

            output_path = os.path.join(output_dir, "structured_toc_nodes.csv")
            df_success.to_csv(output_path, index=False, encoding='utf-8-sig')
            logging.info(f"Successfully saved to {output_path}")
        except Exception as e:
            logging.error(f"Failed to save successful nodes: {e}")

    if failed_lines:
        logging.info(f"Saving {len(failed_lines)} failed lines to {output_dir}/parsing_failures.csv/log...")
        try:
            df_failures = pd.DataFrame(failed_lines)
            output_path_csv = os.path.join(output_dir, "parsing_failures.csv")
            df_failures.to_csv(output_path_csv, index=False, encoding='utf-8-sig')

            output_path_log = os.path.join(output_dir, "parsing_failures.log")
            with open(output_path_log, "w", encoding="utf-8") as f:
                for item in failed_lines:
                    f.write(f"ISBN: {item['isbn']}, Line: {item['line_num']}, Content: {item['line_content']}\n")
            logging.info(f"Successfully saved failure logs to {output_path_csv} and {output_path_log}")
        except Exception as e:
            logging.error(f"Failed to save failed lines: {e}")


# --- (★ 신규) RAG DB 연결 엔진 생성 ---
def get_rag_db_engine():
    """
    docker-compose.yml에 정의된 'postgres_rag_db' 서비스에 연결하는
    새로운 SQLAlchemy 엔진을 반환합니다.
    """
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    db_host = "postgres_rag_db"  # docker-compose.yml의 RAG DB 서비스 이름
    db_port = "5432"  # 컨테이너 내부 포트

    logging.info(f"Connecting to RAG database ({db_host})...")
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        )
        # # pgvector 익스텐션 활성화
        # with engine.connect() as connection:
        #     connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        logging.info("RAG DB connection successful and 'vector' extension enabled.")
        return engine
    except Exception as e:
        logging.error(f"Error connecting to RAG DB: {e}")
        return None


# --- (★ 신규) RAG DB 테이블 스키마 정의 및 생성 ---
def create_rag_db_table(engine):
    """
    최종 임베딩된 목차 청크를 저장할 테이블을 RAG DB에 생성합니다.
    보고서의 VectorField(dimensions=768) [cite: 19]를 참조합니다.
    """
    metadata = MetaData()

    # 'toc_chunks' 테이블 정의
    toc_chunks = Table(
        'toc_chunks',
        metadata,
        Column('id', Integer, Sequence('toc_chunks_id_seq'), primary_key=True),
        Column('isbn', String(13), index=True),
        Column('level', Integer),
        Column('number', String(50)),
        Column('chapter_title', String(1024)),
        Column('composite_text', String),  # 임베딩에 사용된 원본 텍스트
        Column('embedding', Vector(768))  # ko-sroberta-multitask 모델 차원 [cite: 20]
    )

    try:
        # --- (★ 수정된 부분) ---
        # engine.connect()를 사용하여 트랜잭션(컨텍스트)을 엽니다.
        with engine.connect() as connection:
            # 1. 이 커넥션(세션)에서 vector 익스텐션을 활성화합니다.
            connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            logging.info("Ensured 'vector' extension is enabled for this connection.")

            # 2. 동일한 커넥션을 사용하여 테이블을 생성합니다.
            metadata.create_all(connection)

            # (참고: psycopg2.errors.DuplicateTable 예외를 피하기 위해
            # metadata.create_all은 이미 테이블이 있으면 생성하지 않습니다.)

        # --- (수정 끝) ---
        logging.info(f"Table '{toc_chunks.name}' ensured in RAG DB.")
        return toc_chunks

    except Exception as e:
        logging.error(f"Failed to create table '{toc_chunks.name}': {e}")
        return None


# --- (★ 신규) 합성 임베딩 생성 함수 ---
def create_and_embed_chunks(successful_nodes, df_raw_books):
    """
    파싱된 노드(목차)와 원본 책 정보(제목, 요약)를 결합하여
    보고서 5.2 [cite: 165]의 '합성 임베딩'을 생성합니다.
    """
    if EMBEDDING_MODEL is None:
        logging.error("Embedding model is not loaded. Skipping embedding step.")
        return pd.DataFrame()

    logging.info(f"Starting composite embedding for {len(successful_nodes)} nodes...")

    # 1. 파싱된 노드 리스트를 DataFrame으로 변환
    df_nodes = pd.DataFrame(successful_nodes)
    if df_nodes.empty:
        logging.warning("No successful nodes to embed.")
        return pd.DataFrame()

    # 2. 책 정보(제목, 요약)와 목차 노드(챕터 제목)를 병합
    df_merged = pd.merge(
        df_nodes,
        df_raw_books[['isbn', 'title', 'summary']],
        on='isbn',
        how='left',
        suffixes=('_chapter', '_book')  # 'title_chapter', 'title_book'
    )

    # 3. 보고서 에 기반한 합성 텍스트 생성
    # 3. (★ 수정) Fallback 로직이 적용된 합성 텍스트 생성
    def create_composite_text(row):
        book_title = row['title_book'] or ""
        chapter_title = row['title_chapter'] or ""

        # --- Fallback 로직 ---
        # 1순위: summary
        summary_text = row.get('summary')
        if not summary_text or pd.isna(summary_text):
            # 2순위: full_description
            summary_text = row.get('full_description')
        if not summary_text or pd.isna(summary_text):
            # 3순위: publisher_description
            summary_text = row.get('publisher_description')
        if not summary_text or pd.isna(summary_text):
            # 모두 없으면 빈 문자열
            summary_text = ""
        # -----------------------
        return f"도서명: {book_title}. 챕터: {chapter_title}. 책소개: {summary_text}"

    df_merged['composite_text'] = df_merged.apply(create_composite_text, axis=1)

    # 4. 텍스트 목록을 임베딩
    texts_to_embed = df_merged['composite_text'].tolist()
    embeddings = EMBEDDING_MODEL.encode(texts_to_embed, show_progress_bar=True)

    # 5. DataFrame에 임베딩 벡터 추가
    df_merged['embedding'] = list(embeddings)

    # 6. RAG DB에 저장할 최종 컬럼 선택 및 이름 변경
    df_final_chunks = df_merged.rename(columns={'title_chapter': 'chapter_title'})
    final_columns = ['isbn', 'level', 'number', 'chapter_title', 'composite_text', 'embedding']

    logging.info(f"Embedding generation complete for {len(df_final_chunks)} chunks.")
    return df_final_chunks[[col for col in final_columns if col in df_final_chunks.columns]]


# --- (★ 신규) RAG DB에 데이터 적재 함수 ---
def load_chunks_to_rag_db(engine, table, df_chunks):
    """
    임베딩이 완료된 DataFrame을 RAG DB의 지정된 테이블에 적재합니다.
    멱등성을 위해 [cite: 173] 기존 데이터를 삭제하고 새로 삽입합니다.
    """
    if df_chunks.empty:
        logging.warning("No chunks to load into RAG DB.")
        return

    table_name = table.name
    logging.info(f"Loading {len(df_chunks)} chunks into RAG DB table '{table_name}'...")

    try:
        with engine.connect() as connection:
            # 멱등성을 위해 기존 데이터 모두 삭제 (ETL 스크립트이므로)
            connection.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY"))
            logging.info(f"Truncated table '{table_name}'.")

            # DataFrame을 DB에 삽입 (pgvector가 numpy 배열을 자동 변환)
            df_chunks.to_sql(
                table_name,
                connection,
                if_exists='append',
                index=False,
                chunksize=1000  # 대용량 데이터를 위해 청크 단위로 삽입
            )
            logging.info(f"Successfully loaded {len(df_chunks)} chunks into '{table_name}'.")

    except Exception as e:
        logging.error(f"Failed to load data into RAG DB: {e}")


# --- (★ 수정) 메인 실행 로직 ---
if __name__ == "__main__":
    OUTPUT_DIR = "parsing_results"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    activity_log_path = os.path.join(OUTPUT_DIR, "parsing_activity.log")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(activity_log_path, mode='w', encoding='utf-8'),
            logging.StreamHandler()  # (★ 수정) 터미널에도 로그가 보이도록 재활성화
        ]
    )

    logging.info("ETL process started with NEW hierarchical parser.")

    # 1. (수정) 데이터 추출 (summary 포함)
    df_raw_books = extract_raw_tocs()

    if not df_raw_books.empty:
        # 2. 데이터 파싱 (기존과 동일)
        successful_nodes, failed_lines = run_parsing_pipeline(df_raw_books)

        # 3. 파싱 결과 파일로 저장 (기존과 동일 - 디버깅/로그용)
        save_results(successful_nodes, failed_lines, output_dir=OUTPUT_DIR)

        # --- (★ 신규) 임베딩 및 RAG DB 적재 파이프라인 ---
        if not successful_nodes:
            logging.warning("No successful nodes found from parsing. Stopping ETL.")
        else:
            # 4. RAG DB 연결
            rag_engine = get_rag_db_engine()

            if rag_engine:
                # 5. RAG DB 테이블 생성
                rag_table = create_rag_db_table(rag_engine)

                if rag_table is not None:
                    # 6. 합성 임베딩 생성
                    df_final_chunks = create_and_embed_chunks(successful_nodes, df_raw_books)

                    # 7. RAG DB에 최종 데이터 적재
                    load_chunks_to_rag_db(rag_engine, rag_table, df_final_chunks)

        logging.info(f"ETL process finished. Results are in '{OUTPUT_DIR}' and 'postgres_rag_db'.")
    else:
        logging.warning("No raw books found. ETL process stopping.")