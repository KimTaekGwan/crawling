#!/usr/bin/env python
"""
Entry point for Naver search result scraper.
"""
import argparse
from src.main import main, set_force_run, set_skip_existing, set_parallel_count


def parse_arguments():
    """명령줄 인자를 파싱합니다."""
    parser = argparse.ArgumentParser(description="네이버 검색 결과 크롤링 도구")

    parser.add_argument(
        "--force",
        action="store_true",
        help="작업 이력이 없어도 강제로 실행 (기본: 작업 이력이 없으면 건너뜀)",
    )

    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="이미 크롤링된 키워드도 다시 크롤링 (기본: 크롤링된 키워드는 건너뜀)",
    )

    parser.add_argument(
        "--parallel",
        type=int,
        default=4,
        help="병렬 처리 수 설정 (기본값: 4)",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    # 강제 실행 옵션 설정
    set_force_run(args.force)

    # 기존 키워드 건너뛰기 옵션 설정 (기본값: True, --no-skip-existing 옵션 주면 False)
    set_skip_existing(not args.no_skip_existing)

    # 병렬 처리 수 설정
    set_parallel_count(args.parallel)

    # 메인 함수 실행
    main()
