#!/usr/bin/env python3
"""
특정 이메일 주소들의 상태를 SENT(전송 완료) 상태로 변경하는 스크립트

사용법:
1. 텍스트 파일로 이메일 목록 전달:
   python update_email_status.py --file emails.txt

2. 명령줄로 이메일 목록 직접 전달:
   python update_email_status.py --emails email1@example.com email2@example.com

3. 코드 내에 하드코딩된 이메일 목록 사용:
   python update_email_status.py
"""

import argparse
import logging
import sqlite3
import sys
from typing import List

import src.config as config
from src.db_storage import get_db_connection

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 데이터베이스 파일명
DB_FILENAME = config.DEFAULT_DB_FILENAME


def update_emails_to_sent(emails: List[str], db_filename: str = None) -> None:
    """
    지정된 이메일 주소들의 상태를 SENT(전송 완료)로 변경합니다.

    Args:
        emails: 상태를 변경할 이메일 주소 목록
        db_filename: 데이터베이스 파일 경로 (None인 경우 기본값 사용)
    """
    if not emails:
        logger.warning("변경할 이메일 주소가, 목록이 비어 있습니다.")
        return

    # 데이터베이스 파일명 설정
    if db_filename is None:
        db_filename = DB_FILENAME

    logger.info(f"총 {len(emails)}개 이메일 주소의 상태를 SENT로 변경합니다.")

    # 이메일 목록 출력
    for i, email in enumerate(emails, 1):
        logger.info(f"{i}. {email}")

    # 사용자 확인
    confirm = input(
        "\n위 이메일 주소들의 상태를 SENT(전송 완료)로 변경하시겠습니까? (y/n): "
    )
    if confirm.lower() not in ("y", "yes"):
        logger.info("작업이 취소되었습니다.")
        return

    # 데이터베이스 연결
    conn = get_db_connection(db_filename)
    try:
        # row factory 설정
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 이메일 상태 변경 전 현재 상태 확인
        placeholders = ",".join(["?"] * len(emails))
        cursor.execute(
            f"""
            SELECT email, email_status, url FROM websites 
            WHERE email IN ({placeholders})
            """,
            emails,
        )
        rows = cursor.fetchall()

        if not rows:
            logger.warning("데이터베이스에서 지정된 이메일 주소를 찾을 수 없습니다.")
            return

        logger.info("현재 상태:")
        for row in rows:
            status_text = "알 수 없음"
            if row["email_status"] == config.EMAIL_STATUS["NOT_SENT"]:
                status_text = "미전송"
            elif row["email_status"] == config.EMAIL_STATUS["SENT"]:
                status_text = "전송 완료"
            elif row["email_status"] == config.EMAIL_STATUS["ALREADY_SENT"]:
                status_text = "이미 전송됨"
            elif row["email_status"] == config.EMAIL_STATUS["ERROR"]:
                status_text = "오류"
            elif row["email_status"] == config.EMAIL_STATUS["NO_EMAIL"]:
                status_text = "이메일 없음"

            logger.info(
                f"이메일: {row['email']}, URL: {row['url']}, 상태: {row['email_status']} ({status_text})"
            )

        # 이메일 상태 변경
        cursor.execute(
            f"""
            UPDATE websites 
            SET email_status = ?, email_date = CURRENT_TIMESTAMP
            WHERE email IN ({placeholders})
            """,
            [config.EMAIL_STATUS["SENT"]] + emails,
        )

        # 변경된 행 수 확인
        affected_rows = cursor.rowcount
        conn.commit()

        logger.info(
            f"성공적으로 {affected_rows}개 이메일 주소의 상태가 SENT({config.EMAIL_STATUS['SENT']})로 변경되었습니다."
        )

        # 변경 후 상태 다시 확인
        cursor.execute(
            f"""
            SELECT email, email_status, url FROM websites 
            WHERE email IN ({placeholders})
            """,
            emails,
        )
        updated_rows = cursor.fetchall()

        logger.info("변경 후 상태:")
        for row in updated_rows:
            status_text = "알 수 없음"
            if row["email_status"] == config.EMAIL_STATUS["NOT_SENT"]:
                status_text = "미전송"
            elif row["email_status"] == config.EMAIL_STATUS["SENT"]:
                status_text = "전송 완료"
            elif row["email_status"] == config.EMAIL_STATUS["ALREADY_SENT"]:
                status_text = "이미 전송됨"
            elif row["email_status"] == config.EMAIL_STATUS["ERROR"]:
                status_text = "오류"
            elif row["email_status"] == config.EMAIL_STATUS["NO_EMAIL"]:
                status_text = "이메일 없음"

            logger.info(
                f"이메일: {row['email']}, URL: {row['url']}, 상태: {row['email_status']} ({status_text})"
            )

    except sqlite3.Error as e:
        logger.error(f"데이터베이스 오류: {e}")
        conn.rollback()
    finally:
        conn.close()


def read_emails_from_file(filename: str) -> List[str]:
    """
    파일에서 이메일 주소 목록을 읽어옵니다.

    Args:
        filename: 이메일 주소가 있는 파일 경로

    Returns:
        이메일 주소 목록
    """
    emails = []
    try:
        with open(filename, "r", encoding="utf-8") as f:
            for line in f:
                email = line.strip()
                if email and "@" in email:
                    emails.append(email)
    except Exception as e:
        logger.error(f"파일 읽기 오류: {e}")

    return emails


def main():
    # 명령행 인자 파싱
    parser = argparse.ArgumentParser(
        description="특정 이메일 주소들의 상태를 SENT(전송 완료)로 변경하는 스크립트"
    )
    parser.add_argument(
        "--db",
        type=str,
        default=DB_FILENAME,
        help=f"데이터베이스 파일 (기본값: {DB_FILENAME})",
    )
    parser.add_argument(
        "--emails",
        type=str,
        nargs="+",
        help="상태를 변경할 이메일 주소 목록",
    )
    parser.add_argument(
        "--file",
        type=str,
        help="이메일 주소가 있는 파일 경로 (한 줄에 하나의 이메일 주소)",
    )

    args = parser.parse_args()

    # 이메일 목록 가져오기
    emails = []

    if args.file:
        emails = read_emails_from_file(args.file)
        if not emails:
            logger.error(f"파일 {args.file}에서 유효한 이메일 주소를 찾을 수 없습니다.")
            sys.exit(1)
    elif args.emails:
        emails = args.emails
    else:
        # 하드코딩된 이메일 목록 (필요에 따라 수정)
        emails = [
            # 여기에 이메일 주소 목록 추가
            "hammin777@naver.com",
            # 추가 이메일 주소...
        ]

    # 이메일 상태 업데이트
    update_emails_to_sent(emails, args.db)


if __name__ == "__main__":
    main()
