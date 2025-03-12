#!/usr/bin/env python
"""
URL 상세 정보 크롤링 실행 스크립트
"""
import argparse
from src.detail_crawler import crawl_details_from_csv, set_parallel_count
import src.config as config


def parse_arguments():
    """명령줄 인자를 파싱합니다."""
    parser = argparse.ArgumentParser(description="URL 상세 정보 크롤링 도구")

    parser.add_argument(
        "--new", action="store_true", help="처음부터 다시 크롤링 (기존 데이터 무시)"
    )

    parser.add_argument(
        "--interval", type=int, default=10, help="중간 저장 간격 (기본값: 10개 URL마다)"
    )

    parser.add_argument(
        "--input",
        type=str,
        default=config.ALL_DATA_FILE_NAME,
        help=f"입력 CSV 파일명 (기본값: {config.ALL_DATA_FILE_NAME})",
    )

    parser.add_argument(
        "--output",
        type=str,
        default="details_" + config.ALL_DATA_FILE_NAME,
        help=f"출력 CSV 파일명 (기본값: details_{config.ALL_DATA_FILE_NAME})",
    )

    parser.add_argument(
        "--parallel",
        type=int,
        default=4,
        help="병렬 처리 수 설정 (기본값: 4)",
    )

    return parser.parse_args()


def main():
    """메인 함수"""
    # 명령줄 인자 파싱
    args = parse_arguments()

    # 작업 모드 (기본: 이어서 작업)
    resume_mode = not args.new
    mode_description = "이어서 작업" if resume_mode else "처음부터 작업"

    # 병렬 처리 수 설정
    set_parallel_count(args.parallel)

    print(f"상세 정보 크롤링을 {mode_description}합니다.")
    print(f"입력 파일: {args.input}")
    print(f"출력 파일: {args.output}")
    print(f"중간 저장 간격: {args.interval}개 URL마다")
    print(f"병렬 처리 수: {args.parallel}")

    # 크롤링 시작
    crawl_details_from_csv(args.input, args.output, args.interval, resume_mode)


if __name__ == "__main__":
    main()
