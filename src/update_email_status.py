#!/usr/bin/env python3
"""
실패한 이메일 주소 상태 업데이트 도구

이 스크립트는 failed_emails.txt 파일에서 이메일 주소 목록을 읽어와서
해당 이메일 주소들의 상태를 '이메일 주소 없음(NO_EMAIL)' 상태로 변경합니다.

사용법:
python src/update_email_status.py [--input failed_emails.txt] [--verbose]
"""

import os
import sys
import logging
import argparse
from typing import List, Set, Dict

# 프로젝트 모듈 임포트
import src.config as config
from src.email_status_db import EmailDatabase

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 이메일 상태 코드 정의
NO_EMAIL_STATUS = config.EMAIL_STATUS["NO_EMAIL"]  # 이메일 주소 없음 상태 코드 (3)


def read_email_list(input_file: str) -> Set[str]:
    """
    입력 파일에서 이메일 주소 목록을 읽어옵니다.

    Args:
        input_file: 이메일 주소 목록이 있는 파일 경로

    Returns:
        이메일 주소 집합
    """
    try:
        if not os.path.exists(input_file):
            logger.error(f"파일이 존재하지 않습니다: {input_file}")
            return set()

        with open(input_file, "r") as file:
            # 각 줄에서 공백을 제거하고 빈 줄이 아닌 경우에만 이메일로 추가
            emails = {line.strip() for line in file if line.strip()}

        logger.info(f"{input_file}에서 {len(emails)}개의 이메일 주소를 읽어왔습니다.")
        return emails

    except Exception as e:
        logger.error(f"이메일 목록을 읽는 중 오류 발생: {e}")
        return set()


def update_email_status(emails: Set[str], db_file: str = None) -> Dict[str, bool]:
    """
    이메일 주소 목록의 상태를 '이메일 주소 없음(NO_EMAIL)'으로 업데이트합니다.

    Args:
        emails: 상태를 업데이트할 이메일 주소 집합
        db_file: 사용할 데이터베이스 파일 경로 (None이면 기본값 사용)

    Returns:
        업데이트 결과 딕셔너리 (이메일: 성공 여부)
    """
    # 이메일 데이터베이스 인스턴스 생성 및 연결
    db = EmailDatabase(db_file) if db_file else EmailDatabase()
    if not db.connect():
        logger.error("데이터베이스 연결에 실패했습니다.")
        return {email: False for email in emails}

    try:
        # 모든 이메일의 상태를 NO_EMAIL로 업데이트
        logger.info(
            f"이메일 상태 업데이트 중... (상태 코드: NO_EMAIL({NO_EMAIL_STATUS}))"
        )
        results = db.update_multiple_email_status(emails, NO_EMAIL_STATUS)

        # 업데이트 결과 로깅
        success_count = sum(1 for success in results.values() if success)
        logger.info(f"업데이트 완료: {success_count}/{len(emails)}개 성공")

        return results

    except Exception as e:
        logger.error(f"이메일 상태 업데이트 중 오류 발생: {e}")
        return {email: False for email in emails}

    finally:
        # 데이터베이스 연결 종료
        db.close()


def save_results(results: Dict[str, bool], success_file: str, error_file: str) -> None:
    """
    업데이트 결과를 파일로 저장합니다.

    Args:
        results: 업데이트 결과 딕셔너리 (이메일: 성공 여부)
        success_file: 성공한 이메일 목록을 저장할 파일 경로
        error_file: 실패한 이메일 목록을 저장할 파일 경로
    """
    try:
        # 성공 및 실패 이메일 분류
        successes = [email for email, success in results.items() if success]
        errors = [email for email, success in results.items() if not success]

        # 성공 목록 저장
        with open(success_file, "w") as file:
            for email in sorted(successes):
                file.write(f"{email}\n")

        logger.info(f"{len(successes)}개의 이메일 상태 업데이트 성공: {success_file}")

        # 실패 목록 저장 (있는 경우에만)
        if errors:
            with open(error_file, "w") as file:
                for email in sorted(errors):
                    file.write(f"{email}\n")

            logger.warning(f"{len(errors)}개의 이메일 상태 업데이트 실패: {error_file}")

    except Exception as e:
        logger.error(f"결과 저장 중 오류 발생: {e}")


def display_database_stats(db_file: str = None) -> None:
    """
    데이터베이스 상태 통계를 출력합니다.

    Args:
        db_file: 데이터베이스 파일 경로 (None이면 기본값 사용)
    """
    db = EmailDatabase(db_file) if db_file else EmailDatabase()
    if not db.connect():
        logger.error("통계를 위한 데이터베이스 연결에 실패했습니다.")
        return

    try:
        # 상태별 통계 조회
        stats = db.get_status_statistics()

        if not stats:
            logger.warning("데이터베이스에 이메일 상태 정보가 없습니다.")
            return

        # 상태별 통계 출력
        print("\n데이터베이스 이메일 상태 통계:")
        total = sum(stats.values())

        for status_code, count in sorted(stats.items()):
            status_name = None
            for name, code in config.EMAIL_STATUS.items():
                if code == status_code:
                    status_name = name
                    break

            status_name = status_name or f"알 수 없음({status_code})"
            print(f"- {status_name}: {count}개 ({count/total*100:.1f}%)")

        print(f"총 이메일 수: {total}개")

    except Exception as e:
        logger.error(f"통계 조회 중 오류 발생: {e}")

    finally:
        db.close()


def main():
    """
    메인 함수: 커맨드 라인 인자 처리 및 이메일 상태 업데이트 실행
    """
    parser = argparse.ArgumentParser(
        description="실패한 이메일 주소 상태 업데이트 도구"
    )

    parser.add_argument(
        "--input",
        type=str,
        default="failed_emails.txt",
        help="이메일 주소 목록이 있는 파일 (기본값: failed_emails.txt)",
    )
    parser.add_argument(
        "--success-output",
        type=str,
        default="updated_emails.txt",
        help="상태 업데이트에 성공한 이메일 목록을 저장할 파일 (기본값: updated_emails.txt)",
    )
    parser.add_argument(
        "--error-output",
        type=str,
        default="update_errors.txt",
        help="상태 업데이트에 실패한 이메일 목록을 저장할 파일 (기본값: update_errors.txt)",
    )
    parser.add_argument(
        "--db-file", type=str, help="사용할 데이터베이스 파일 경로 (기본값: emails.db)"
    )
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="업데이트 없이 데이터베이스 통계만 표시합니다",
    )
    parser.add_argument("--verbose", action="store_true", help="상세 로그를 출력합니다")

    args = parser.parse_args()

    # 로그 레벨 설정
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # 통계만 표시하는 경우
    if args.stats_only:
        display_database_stats(args.db_file)
        return

    # 이메일 목록 읽기
    emails = read_email_list(args.input)

    if not emails:
        logger.error(f"이메일 주소 목록이 비어 있습니다: {args.input}")
        return

    # 이메일 상태 업데이트
    logger.info(
        f"{len(emails)}개 이메일의 상태를 'NO_EMAIL({NO_EMAIL_STATUS})'로 업데이트합니다..."
    )
    results = update_email_status(emails, args.db_file)

    # 결과 집계
    success_count = sum(1 for success in results.values() if success)
    error_count = len(results) - success_count

    logger.info(
        f"이메일 상태 업데이트 완료: 성공 {success_count}개, 실패 {error_count}개"
    )

    # 결과 저장
    save_results(results, args.success_output, args.error_output)

    # 결과 요약 출력
    print(f"\n상태 업데이트 결과:")
    print(f"- 총 처리: {len(emails)}개 이메일")
    print(f"- 성공: {success_count}개 (저장 파일: {args.success_output})")
    print(
        f"- 실패: {error_count}개"
        + (f" (저장 파일: {args.error_output})" if error_count > 0 else "")
    )

    # 성공/실패 이메일 샘플 출력
    if success_count > 0:
        print("\n업데이트 성공 샘플 (최대 5개):")
        for i, email in enumerate(sorted([e for e, s in results.items() if s])[:5], 1):
            print(f"{i}. {email}")

    if error_count > 0:
        print("\n업데이트 실패 샘플 (최대 5개):")
        for i, email in enumerate(
            sorted([e for e, s in results.items() if not s])[:5], 1
        ):
            print(f"{i}. {email}")

    # 데이터베이스 통계 출력
    display_database_stats(args.db_file)


if __name__ == "__main__":
    main()
