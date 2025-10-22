# books/tasks.py
import time

from celery import shared_task, group, chain
from bookroad.services import AladinAPI
from .models import Book, Chapter  # 1단계에서 만든 Book, Chapter 모델
from datetime import datetime
import re  # 목차 파싱을 위해 re (정규표현식) 임포트


# (참고) 임베딩 생성을 위한 임시 헬퍼 함수
# 실제로는 HuggingFace Transformers 같은 라이브러리를 사용해야 합니다.
def get_embedding_vector(text):
    """[임시 STUB] 실제 임베딩 모델을 호출하는 함수입니다."""
    # 예: from sentence_transformers import SentenceTransformer
    # model = SentenceTransformer('model_name')
    # return model.encode(text)

    # 지금은 768차원의 임시 벡터(0)를 반환합니다.
    # 이 함수는 실제 모델로 교체되어야 합니다.
    print(f"[Embedding Stub] Processing: {text[:30]}...")
    return [0.0] * 768


# --- (헬퍼 함수: _fetch_all_pages) ---
# 이 함수는 기존과 동일합니다. (변경 없음)
def _fetch_all_pages(api, method, params):
    isbns = []
    start_page = 1
    MAX_RESULTS_PER_PAGE = 50
    MAX_TOTAL_RESULTS = 200

    while True:
        page_params = {**params, 'start': start_page, 'MaxResults': MAX_RESULTS_PER_PAGE}
        response = api.item_list(**page_params) if method == 'item_list' else api.item_search(**page_params)

        if not response or 'item' not in response: break
        for item in response['item']:
            if 'isbn13' in item and item['isbn13']:
                # isbn13이 리스트인 경우 첫 번째 요소를 사용하도록 수정
                isbn_value = item['isbn13']
                if isinstance(isbn_value, list):
                    if isbn_value: # 리스트가 비어있지 않은 경우
                        isbn_value = isbn_value[0]
                    else: # 리스트가 비어있는 경우 건너뜀
                        continue
                
                if len(isbn_value) == 13:
                    isbns.append(isbn_value)

        total_results = response.get('totalResults', 0)
        if not total_results or total_results <= start_page * MAX_RESULTS_PER_PAGE or start_page * MAX_RESULTS_PER_PAGE >= MAX_TOTAL_RESULTS:
            break
        start_page += 1
    return isbns


# === 파이프라인 1 (검색어 대폭 확장) ===
@shared_task(rate_limit='1/s')
def discover_isbns_for_category(category_id):
    """하이브리드 및 다중 질의 전략으로 ISBN 목록을 확장하여 탐색합니다."""
    api = AladinAPI()
    unique_isbns = set()

    # 1. 기본 전략 (베스트셀러, 신간)
    base_strategies = [
        {'method': 'item_list',
         'params': {'QueryType': 'Bestseller', 'CategoryId': category_id, 'SearchTarget': 'Book'}},
        {'method': 'item_list',
         'params': {'QueryType': 'ItemNewAll', 'CategoryId': category_id, 'SearchTarget': 'Book'}},
    ]

    # 2. 확장 검색어 목록 (RAG 성능 향상을 위해 대폭 추가)
    extended_keywords = [
        '기술', '데이터', 'AI', '인공지능', '머신러닝', '딥러닝',
        '프로그래밍', '파이썬', '자바', 'Java', 'JavaScript', 'C++', 'Rust',
        '알고리즘', '자료구조', '네트워크', '보안', '클라우드', 'AWS', 'Azure',
        '백엔드', '프론트엔드', '데이터베이스', 'SQL', '운영체제', '리눅스',
        '컴퓨터 구조', '소프트웨어 공학', '웹 개발', '앱 개발', '모바일'
    ]

    # 3. 모든 검색 전략을 합칩니다.
    query_strategies = base_strategies + [
        {'method': 'item_search',
         'params': {'Query': keyword, 'CategoryId': category_id, 'SearchTarget': 'Book', 'Sort': 'SalesPoint'}}
        for keyword in extended_keywords
    ]

    for strategy in query_strategies:
        try:
            # _fetch_all_pages는 API 요청 실패 시 빈 리스트 []를 반환해야 합니다. (혹은 None)
            isbns_from_query = _fetch_all_pages(api, strategy['method'], strategy['params'])
            if isbns_from_query:
                unique_isbns.update(isbns_from_query)

            time.sleep(0.5)
        except Exception as e:
            # 한두 개의 검색어가 실패해도 전체가 중단되지 않도록 예외 처리
            print(f"Warning: Failed fetching strategy {strategy.get('params')} for CID {category_id}. Error: {e}")
            time.sleep(1)

    print(
        f"Category {category_id}: Discovered {len(unique_isbns)} unique ISBNs from {len(query_strategies)} strategies.")
    return list(unique_isbns)


# === 파이프라인 2 (ISBN 중복이면 "스킵" 로직) ===
@shared_task
def process_discovered_isbns(isbn_list):
    """
    ISBN 목록을 받아 DB와 비교 후,
    '새로운' (중복되지 않은) ISBN에 대해서만 [정보 저장 -> 목차 파싱 -> 임베딩] 체인을 실행합니다.
    """
    if not isbn_list:
        return "No ISBNs to process."

    # 1. 중첩 리스트(list[list])를 1차원 set(set[str])으로 펼칩니다. (방어 코드)
    try:
        flat_isbn_set = {
            str(isbn)
            for item in isbn_list
            for isbn in (item if isinstance(item, list) else [item])
            if item and isinstance(isbn, str) and isbn.isdigit() and len(isbn) == 13
        }
    except TypeError:
        flat_isbn_set = {str(item) for item in isbn_list if
                         isinstance(item, str) and item.isdigit() and len(item) == 13}

    if not flat_isbn_set:
        return f"No valid ISBNs found after flattening list of length {len(isbn_list)}."

    # ▼▼▼ [핵심] DB 필터링 ("중복이면 스킵") 로직 ▼▼▼

    # 2-1. DB에 이미 있는 ISBN 목록을 조회
    existing_isbns = set(
        Book.objects.filter(isbn__in=flat_isbn_set).values_list('isbn', flat=True)
    )

    # 2-2. '새로운' ISBN 목록만 필터링
    new_isbns = [isbn for isbn in flat_isbn_set if isbn not in existing_isbns]

    if not new_isbns:
        return f"No new ISBNs to fetch. (Processed {len(flat_isbn_set)}, all existed)."
    # ▲▲▲ [핵심] "중복이면 스킵" 로직 끝 ▲▲▲

    # 'new_isbns' (필터링된 리스트)를 사용합니다.
    job_group = group(
        chain(
            fetch_and_save_book_details.s(isbn),
            parse_toc_and_create_chapters.s(),
            generate_embeddings_for_book.s()
        )
        for isbn in new_isbns  # "새로운" ISBN에 대해서만 체인 생성
    )

    job_group.apply_async()
    return f"Started processing chains for {len(new_isbns)} new ISBNs (out of {len(flat_isbn_set)} discovered)."


# === 파이프라인 3 ===
@shared_task(bind=True, max_retries=3, default_retry_delay=60, rate_limit='60/m')
def fetch_and_save_book_details(self, isbn13):
    """ISBN13으로 상세 정보를 조회하고 'Book' 모델에 저장 (또는 업데이트) 합니다."""
    api = AladinAPI()
    try:
        response = api.item_lookup(
            ItemId=isbn13,
            ItemIdType='ISBN13',
            OptResult='authors,Toc,fullDescription,publisherReview,itemPage'  # Toc(대문자) 요청
        )
        if not response or 'item' not in response or not response['item']:
            return f"Failed: No details for {isbn13}"

        item = response['item'][0]

        # ▼▼▼ [핵심 수정] subInfo 객체를 안전하게 가져옵니다. ▼▼▼
        # 만약 subInfo가 응답에 없으면, 빈 딕셔너리({})를 사용해 에러를 방지합니다.
        sub_info = item.get('subInfo', {})
        # ▲▲▲ [핵심 수정] ▲▲▲

        # ... (pub_date = None 및 날짜 파싱 로직은 그대로 둠) ...
        pub_date = None
        try:
            pub_date = datetime.strptime(item.get('pubDate'), '%Y-%m-%d').date()
        except (ValueError, TypeError):
            pass

        book, created = Book.objects.update_or_create(
            isbn=item.get('isbn13'),  # 조회 기준 키
            defaults={
                # --- Top-Level 정보 ---
                'title': item.get('title', ''),
                'author': item.get('author', ''),
                'summary': item.get('description', ''),
                'publisher': item.get('publisher', ''),
                'publication_date': pub_date,
                'full_description': item.get('fullDescription', ''),

                # --- [수정] Top-Level (Fallback 추가) ---
                # publisherReview가 없으면 fullDescription2를 사용합니다.
                'publisher_description': item.get('publisherReview', item.get('fullDescription2', '')),

                # --- [수정] Nested (subInfo) 정보 ---
                'subtitle': sub_info.get('subTitle', ''),
                'page_count': sub_info.get('itemPage', None) or None,
                'authors_json': sub_info.get('authors', None),
                'raw_toc': sub_info.get('toc', ''),  # <-- ★★★ 드디어 'toc'를 올바르게 가져옵니다 ★★★
            }
        )

        action = "Created" if created else "Updated"
        print(f"Successfully {action} book: {book.title}")
        return isbn13

    except Exception as e:
        self.retry(exc=e)
        return f"Error processing {isbn13}: {e}"


# === 파이프라인 4 ===
@shared_task
def parse_toc_and_create_chapters(isbn13):
    """
    저장된 'Book'의 'raw_toc'를 파싱하여 'Chapter' 객체를 생성합니다.
    (다음 태스크(5)로 isbn13을 넘김)
    """

    # ▼▼▼ [핵심 수정] 방어 코드 추가 ▼▼▼
    # 3번 태스크에서 실패 문자열을 넘겨받았는지 확인합니다.
    # (isbn13이 13자리 숫자가 아니면 실행 중단)
    if not (isinstance(isbn13, str) and isbn13.isdigit() and len(isbn13) == 13):
        return f"Skipped TOC: Received invalid ISBN string '{str(isbn13)[:50]}...'"
    # ▲▲▲ [핵심 수정] ▲▲▲

    try:
        book = Book.objects.get(isbn=isbn13)
        if not book.raw_toc:
            return f"Skipped TOC: No raw_toc for {isbn13}"

        # ... (기존 목차 파싱 로직은 그대로) ...
        book.chapters.all().delete()
        chapters_to_create = []
        lines = book.raw_toc.split('\r\n')
        order = 1

        for line in lines:
            cleaned_line = line.strip()
            if not cleaned_line: continue
            level = 1
            if re.match(r'^(Part|부)\s*\d+', cleaned_line, re.IGNORECASE):
                level = 1
            elif re.match(r'^(Chapter|장)\s*\d+', cleaned_line, re.IGNORECASE):
                level = 2
            elif cleaned_line.startswith('  ') and not cleaned_line.startswith('    '):
                level = 2
            elif cleaned_line.startswith('    '):
                level = 3
            chapters_to_create.append(Chapter(book=book, order=order, level=level, title=cleaned_line))
            order += 1

        if chapters_to_create:
            Chapter.objects.bulk_create(chapters_to_create)
            book.toc_parsing_failed = False
        else:
            book.toc_parsing_failed = True

        book.save()
        print(f"Successfully parsed TOC for {isbn13} into {len(chapters_to_create)} chapters.")
        return isbn13  # [중요] '성공' 시에만 다음 태스크로 isbn13을 전달

    except Book.DoesNotExist:
        return f"Failed TOC: Book {isbn13} not found in DB."
    except Exception as e:
        try:
            book = Book.objects.get(isbn=isbn13)
            book.toc_parsing_failed = True
            book.save()
        except Book.DoesNotExist:
            pass
        return f"Error parsing TOC for {isbn13}: {e}"


# === 파이프라인 5 ===
@shared_task
def generate_embeddings_for_book(isbn13):
    """
    Book의 'summary'와 각 'Chapter'의 'title'에 대한 임베딩을 생성합니다.
    [최종 태스크]
    """

    # ▼▼▼ [핵심 수정] 방어 코드 추가 ▼▼▼
    # 4번 태스크에서 실패 문자열을 넘겨받았는지 확인합니다.
    if not (isinstance(isbn13, str) and isbn13.isdigit() and len(isbn13) == 13):
        return f"Skipped Embed: Received invalid result from TOC task '{str(isbn13)[:50]}...'"
    # ▲▲▲ [핵심 수정] ▲▲▲

    try:
        book = Book.objects.get(isbn=isbn13)

        # ... (기존 임베딩 생성 로직은 그대로) ...
        if book.summary and not book.summary_embedding:
            book.summary_embedding = get_embedding_vector(book.summary)
            book.save()

        chapters_to_embed = book.chapters.filter(title_embedding__isnull=True)
        chapters_to_update = []

        for chapter in chapters_to_embed:
            chapter.title_embedding = get_embedding_vector(chapter.title)
            chapters_to_update.append(chapter)

        if chapters_to_update:
            Chapter.objects.bulk_update(chapters_to_update, ['title_embedding'])

        return f"Successfully generated embeddings for {isbn13} (Book summary + {len(chapters_to_update)} chapters)."

    except Book.DoesNotExist:
        return f"Failed Embed: Book {isbn13} not found in DB."
    except Exception as e:
        return f"Error generating embeddings for {isbn13}: {e}"