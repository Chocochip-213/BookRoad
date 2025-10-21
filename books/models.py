# books/models.py
from django.db import models
from pgvector.django import VectorField

class Book(models.Model):
    # --- 기본 정보 ---
    title = models.CharField(max_length=512, help_text="도서 제목")
    author = models.CharField(max_length=256, help_text="저자 (대표)")
    isbn = models.CharField(max_length=13, unique=True, db_index=True, help_text="ISBN 13자리")
    summary = models.TextField(blank=True, help_text="도서 요약 정보")

    # --- 보고서 기반 확장 필드 ---
    subtitle = models.CharField(max_length=512, blank=True, help_text="부제")
    publisher = models.CharField(max_length=256, blank=True, help_text="출판사")
    publication_date = models.DateField(null=True, blank=True, help_text="출간일")
    page_count = models.PositiveIntegerField(null=True, blank=True, help_text="쪽수")
    full_description = models.TextField(blank=True, help_text="책소개 (상세)")
    publisher_description = models.TextField(blank=True, help_text="출판사 제공 책소개")
    authors_json = models.JSONField(null=True, blank=True, help_text="저자 상세 정보 (JSON)")

    # 원본 목차
    raw_toc = models.TextField(blank=True, help_text="알라딘 API 원본 목차(파싱 전)")


    # --- 상태 관리 및 임베딩 ---
    toc_parsing_failed = models.BooleanField(default=False, help_text="목차 파싱 실패 여부")
    summary_embedding = VectorField(dimensions=768, null=True, blank=True, help_text="요약 정보 임베딩 벡터")

    # --- 타임스탬프 ---
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.title

class Chapter(models.Model):
    book = models.ForeignKey(Book, related_name='chapters', on_delete=models.CASCADE, help_text="연관 도서")
    order = models.PositiveIntegerField(help_text="챕터 순서")
    level = models.PositiveIntegerField(default=1, help_text="챕터 계층 레벨")
    title = models.TextField(help_text="챕터 제목")
    title_embedding = VectorField(dimensions=768, null=True, blank=True, help_text="챕터 제목 임베딩 벡터")

    class Meta:
        ordering = ['order']

    def __str__(self):
        return f"[{self.book.title}] {self.title}"