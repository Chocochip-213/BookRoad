# books/management/commands/start_discovery.py

import json
import os
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from celery import group, chain
from books.tasks import discover_isbns_for_category, process_discovered_isbns


class Command(BaseCommand):
    help = '설정 파일(기본: target_categories.json)에서 카테고리 ID를 읽어 전체 데이터 파이프라인을 시작합니다.'

    # [변경점 1] 인자 받는 방식을 파일 경로로 변경
    def add_arguments(self, parser):
        parser.add_argument(
            '--file',
            type=str,
            default='target_categories.json',
            help='사용할 카테고리 ID가 포함된 JSON 파일 경로. (기본값: target_categories.json)'
        )

    def handle(self, *args, **options):
        file_path = options['file']

        # [변경점 2] JSON 파일 읽기 및 예외 처리
        try:
            # 프로젝트 루트 디렉토리 기준으로 파일 경로를 안전하게 구성
            full_path = os.path.join(settings.BASE_DIR, file_path)
            with open(full_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                category_ids = config.get('category_ids', )

            if not category_ids:
                raise CommandError(f"'{file_path}' 파일에 'category_ids' 목록이 비어있거나 존재하지 않습니다.")

        except FileNotFoundError:
            raise CommandError(f"설정 파일을 찾을 수 없습니다: '{full_path}'")
        except json.JSONDecodeError:
            raise CommandError(f"'{file_path}' 파일이 올바른 JSON 형식이 아닙니다.")

        self.stdout.write(
            self.style.SUCCESS(f"✅ '{file_path}' 파일에서 {len(category_ids)}개 카테고리를 읽었습니다. 파이프라인을 시작합니다.")
        )

        # [변경점 3] 파이프라인 로직은 그대로 유지
        discovery_group = group(discover_isbns_for_category.s(cid) for cid in category_ids)
        process_task = process_discovered_isbns.s()
        pipeline = chain(discovery_group | process_task)

        pipeline.apply_async()

        self.stdout.write(
            self.style.WARNING("🚀 Celery 워커에게 작업을 전달했습니다. 백그라운드에서 데이터 구축이 시작됩니다.")
        )
        self.stdout.write(
            self.style.NOTICE("   (진행 상황은 'docker-compose logs -f celery_worker' 명령어로 확인하세요)")
        )