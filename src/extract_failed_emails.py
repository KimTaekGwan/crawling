#!/usr/bin/env python3
"""
네이버 메일 발송 실패 이메일 추출 도구

이 스크립트는 네이버 메일의 발송 실패 알림에서 실패한 이메일 주소를 추출합니다.
발신자 navermail_noreply@navercorp.com에서 보낸 "[네이버 메일] xxx@example.com으로 메일 발송이 실패되었습니다."
형식의 이메일에서 실패한 이메일 주소를 추출합니다.

사용법:
python src/extract_failed_emails.py [--output output_file.txt]
"""

import os
import re
import imaplib
import email
import email.header
import argparse
import logging
import sys
from typing import List, Dict, Any, Optional, Tuple, Set

# 프로젝트 모듈 임포트
import src.config as config

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 네이버 IMAP 서버 정보
IMAP_SERVER = "imap.naver.com"
IMAP_PORT = 993  # SSL 포트

# 발신자 메일 주소
SENDER_EMAIL = "navermail_noreply@navercorp.com"


def decode_header_str(header_str: str) -> str:
    """
    이메일 헤더 문자열을 디코딩합니다.

    Args:
        header_str: 디코딩할 헤더 문자열

    Returns:
        디코딩된 문자열
    """
    if not header_str:
        return ""

    decoded_parts = email.header.decode_header(header_str)
    decoded_str = ""

    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            try:
                if encoding:
                    decoded_part = part.decode(encoding)
                else:
                    # 인코딩이 지정되지 않은 경우, utf-8 및 cp949 시도
                    try:
                        decoded_part = part.decode("utf-8")
                    except UnicodeDecodeError:
                        try:
                            decoded_part = part.decode("cp949")
                        except UnicodeDecodeError:
                            # 마지막 시도로 errors='replace' 사용
                            decoded_part = part.decode("utf-8", errors="replace")
            except Exception as e:
                logger.warning(f"헤더 디코딩 오류: {e}, 원본 사용")
                decoded_part = str(part)
        else:
            # 이미 문자열인 경우
            decoded_part = part

        decoded_str += decoded_part

    return decoded_str


def extract_failed_email_from_subject(subject: str) -> Optional[str]:
    """
    메일 제목에서 실패한 이메일 주소를 추출합니다.

    Args:
        subject: 이메일 제목

    Returns:
        실패한 이메일 주소 또는 None
    """
    # "[네이버 메일] xxx@example.com으로 메일 발송이 실패되었습니다." 형식에서 추출
    pattern = r"\[네이버 메일\] ([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})으로 메일 발송이 실패되었습니다"
    match = re.search(pattern, subject)

    if match:
        return match.group(1)  # 첫 번째 캡처 그룹이 이메일 주소

    return None


def connect_to_mailbox(
    username: str, password: str, mailbox: str = "INBOX"
) -> Tuple[Optional[imaplib.IMAP4_SSL], bool]:
    """
    IMAP 서버에 연결하고 특정 메일함을 선택합니다.

    Args:
        username: IMAP 서버 사용자 이름 (이메일 주소)
        password: 비밀번호 또는 앱 비밀번호
        mailbox: 선택할 메일함 (기본값: "INBOX")

    Returns:
        (IMAP 연결 객체, 성공 여부) 튜플
    """
    try:
        # IMAP 서버 연결
        mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)

        # 로그인
        mail.login(username, password)
        logger.info(f"{username}로 IMAP 서버에 성공적으로 로그인했습니다.")

        # 메일함 선택
        status, messages = mail.select(mailbox)

        if status != "OK":
            logger.error(f"{mailbox} 메일함 선택 실패: {messages}")
            mail.logout()
            return None, False

        logger.info(
            f"{mailbox} 메일함을 선택했습니다. 총 {int(messages[0])}개의 메시지가 있습니다."
        )
        return mail, True

    except imaplib.IMAP4.error as e:
        logger.error(f"IMAP 오류: {e}")
        return None, False
    except Exception as e:
        logger.error(f"연결 중 오류 발생: {e}")
        return None, False


def search_emails_in_batches(
    mail: imaplib.IMAP4_SSL, sender: str, batch_size: int = 1000
) -> List[List[str]]:
    """
    발신자를 기준으로 이메일을 검색하고 배치 단위로 반환합니다.

    네이버 IMAP 서버가 한 번에 반환할 수 있는 이메일 수에 제한이 있을 수 있으므로,
    날짜를 기준으로 배치 처리합니다.

    Args:
        mail: IMAP 연결 객체
        sender: 발신자 이메일 주소
        batch_size: 각 배치의 크기

    Returns:
        이메일 ID 배치 목록
    """
    try:
        # 먼저 전체 이메일 개수 확인
        search_str = f'FROM "{sender}"'
        status, data = mail.search(None, search_str)

        if status != "OK":
            logger.error(f"이메일 검색 실패: {data}")
            return []

        all_email_ids = data[0].split()
        logger.info(f"총 {len(all_email_ids)}개의 이메일을 찾았습니다.")

        # 배치로 나누기
        batches = []
        for i in range(0, len(all_email_ids), batch_size):
            batch = all_email_ids[i : i + batch_size]
            batches.append([id.decode() for id in batch])

        logger.info(f"{len(batches)}개의 배치로 처리합니다.")
        return batches

    except Exception as e:
        logger.error(f"이메일 검색 중 오류 발생: {e}")
        return []


def fetch_email_headers(
    mail: imaplib.IMAP4_SSL, email_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    이메일 ID 목록에서 헤더 정보만 가져옵니다.

    Args:
        mail: IMAP 연결 객체
        email_ids: 이메일 ID 목록

    Returns:
        헤더 정보 목록
    """
    headers_list = []

    for email_id in email_ids:
        try:
            # 헤더만 가져오기
            status, data = mail.fetch(email_id, "(BODY.PEEK[HEADER])")
            if status != "OK":
                logger.error(f"이메일 {email_id} 헤더 가져오기 실패: {data}")
                continue

            # 이메일 헤더 파싱
            header_data = data[0][1]
            msg = email.message_from_bytes(header_data)

            subject = decode_header_str(msg["Subject"])

            headers_list.append({"id": email_id, "subject": subject})

        except Exception as e:
            logger.error(f"이메일 {email_id} 헤더 처리 중 오류: {e}")

    return headers_list


def extract_failed_emails_from_headers(headers_list: List[Dict[str, Any]]) -> Set[str]:
    """
    이메일 헤더 목록에서 실패한 이메일 주소를 추출합니다.

    Args:
        headers_list: 이메일 헤더 정보 목록

    Returns:
        실패한 이메일 주소 집합
    """
    failed_emails = set()

    for header in headers_list:
        subject = header["subject"]
        failed_email = extract_failed_email_from_subject(subject)

        if failed_email:
            failed_emails.add(failed_email)

    return failed_emails


def save_to_file(emails: Set[str], output_file: str) -> None:
    """
    이메일 주소 목록을 파일로 저장합니다.

    Args:
        emails: 이메일 주소 집합
        output_file: 출력 파일 경로
    """
    try:
        with open(output_file, "w") as f:
            for email in sorted(emails):
                f.write(f"{email}\n")

        logger.info(f"{len(emails)}개의 이메일 주소를 {output_file}에 저장했습니다.")

    except Exception as e:
        logger.error(f"파일 저장 중 오류 발생: {e}")


def main():
    """
    메인 함수: 커맨드 라인 인자 처리 및 이메일 추출 실행
    """
    parser = argparse.ArgumentParser(
        description="네이버 메일 발송 실패 이메일 추출 도구"
    )

    parser.add_argument(
        "--output",
        type=str,
        default="failed_emails.txt",
        help="추출한 이메일 주소를 저장할 파일 (기본값: failed_emails.txt)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="한 번에 처리할 이메일 배치 크기 (기본값: 1000)",
    )
    parser.add_argument("--verbose", action="store_true", help="상세 로그를 출력합니다")

    args = parser.parse_args()

    # 로그 레벨 설정
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # 이메일 계정 정보
    username = config.EMAIL_SENDER
    password = config.EMAIL_PASSWORD

    if not username or not password:
        logger.error(
            "이메일 계정 정보가 설정되지 않았습니다. config.py 또는 .env 파일을 확인하세요."
        )
        return

    # 메일함 연결
    mail, success = connect_to_mailbox(username, password)
    if not success or mail is None:
        return

    try:
        # 이메일 검색 및 실패 이메일 추출
        failed_emails = set()

        # 배치 단위로 처리
        batches = search_emails_in_batches(mail, SENDER_EMAIL, args.batch_size)

        total_processed = 0
        for i, batch in enumerate(batches, 1):
            logger.info(f"배치 {i}/{len(batches)} 처리 중 ({len(batch)}개 이메일)")

            # 헤더 정보만 가져오기
            headers = fetch_email_headers(mail, batch)

            # 실패 이메일 추출
            batch_failed_emails = extract_failed_emails_from_headers(headers)
            failed_emails.update(batch_failed_emails)

            total_processed += len(batch)
            logger.info(
                f"현재까지 처리: {total_processed}개 이메일, 발견된 실패 이메일: {len(failed_emails)}개"
            )

        # 결과 출력
        if failed_emails:
            logger.info(f"총 {len(failed_emails)}개의 실패한 이메일 주소를 찾았습니다.")

            # 결과 파일 저장
            save_to_file(failed_emails, args.output)

            # 화면에 일부 출력
            print(f"\n총 {len(failed_emails)}개의 실패한 이메일 주소를 찾았습니다:")
            for i, email in enumerate(sorted(failed_emails)[:20], 1):
                if i <= 20:  # 처음 20개만 표시
                    print(f"{i}. {email}")

            if len(failed_emails) > 20:
                print(f"... 외 {len(failed_emails) - 20}개")

            print(f"\n전체 목록은 '{args.output}' 파일에 저장되었습니다.")
        else:
            logger.warning("실패한 이메일 주소를 찾을 수 없습니다.")

    except Exception as e:
        logger.error(f"이메일 추출 중 오류 발생: {e}")

    finally:
        # 연결 종료
        try:
            mail.close()
            mail.logout()
            logger.info("IMAP 서버 연결이 종료되었습니다.")
        except Exception:
            pass


if __name__ == "__main__":
    main()
