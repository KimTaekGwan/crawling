#!/usr/bin/env python3
"""
네이버 메일 받은 편지함 확인 모듈

이 모듈은 IMAP 프로토콜을 이용하여 네이버 메일의 받은 편지함을 확인하고
이메일 목록을 표시하는 기능을 제공합니다.

환경 설정은 config.py 파일 또는 .env 파일에서 관리됩니다.
.env 파일 예시:
```
EMAIL_SENDER=your_email@naver.com
EMAIL_PASSWORD=your_password_or_app_password
```

사용 예시:
```
# 최근 10개 이메일 확인
python src/email_checker.py --limit 10

# 특정 발신자로부터 온 이메일만 확인
python src/email_checker.py --sender someone@example.com

# 특정 제목을 포함한 이메일만 확인
python src/email_checker.py --subject "특정 키워드"

# 특정 날짜 이후의 이메일만 확인
python src/email_checker.py --since "01-Jan-2023"
```
"""

import os
import re
import imaplib
import email
import email.header
import argparse
import logging
import sys
import datetime
from email.utils import parsedate_to_datetime
from typing import List, Dict, Any, Optional, Tuple

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


def extract_email_address(email_str: str) -> str:
    """
    이메일 주소 형식(예: "홍길동 <example@example.com>")에서 실제 이메일 주소만 추출합니다.

    Args:
        email_str: 이메일 주소 문자열

    Returns:
        추출된 이메일 주소
    """
    if not email_str:
        return ""

    # 이메일 형식에서 주소만 추출
    match = re.search(r"<([^>]+)>", email_str)
    if match:
        return match.group(1)

    # '<>'가 없는 경우 원본 문자열이 이메일 주소일 수 있음
    # 간단한 이메일 검증
    if re.match(r"[^@]+@[^@]+\.[^@]+", email_str):
        return email_str

    return email_str


def get_email_body(msg) -> Tuple[str, str]:
    """
    이메일 메시지에서 본문을 추출합니다. HTML과 텍스트 버전 모두 반환합니다.

    Args:
        msg: 이메일 메시지 객체

    Returns:
        (텍스트 본문, HTML 본문) 튜플
    """
    text_body = ""
    html_body = ""

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition"))

            # 첨부 파일은 건너뜁니다
            if "attachment" in content_disposition:
                continue

            if content_type == "text/plain" and not text_body:
                try:
                    charset = part.get_content_charset() or "utf-8"
                    text_body = part.get_payload(decode=True).decode(
                        charset, errors="replace"
                    )
                except Exception as e:
                    logger.warning(f"텍스트 본문 추출 오류: {e}")

            elif content_type == "text/html" and not html_body:
                try:
                    charset = part.get_content_charset() or "utf-8"
                    html_body = part.get_payload(decode=True).decode(
                        charset, errors="replace"
                    )
                except Exception as e:
                    logger.warning(f"HTML 본문 추출 오류: {e}")
    else:
        # 멀티파트가 아닌 경우
        content_type = msg.get_content_type()
        try:
            charset = msg.get_content_charset() or "utf-8"
            body = msg.get_payload(decode=True).decode(charset, errors="replace")

            if content_type == "text/plain":
                text_body = body
            elif content_type == "text/html":
                html_body = body
        except Exception as e:
            logger.warning(f"본문 추출 오류: {e}")

    return text_body, html_body


def format_date(date_str: str) -> str:
    """
    이메일 날짜 문자열을 가독성 있는 형식으로 변환합니다.

    Args:
        date_str: 이메일 날짜 문자열

    Returns:
        포맷된 날짜 문자열
    """
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return date_str


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


def search_emails(
    mail: imaplib.IMAP4_SSL,
    sender: str = None,
    subject: str = None,
    since_date: str = None,
    unseen_only: bool = False,
) -> List[str]:
    """
    조건에 맞는 이메일을 검색합니다.

    Args:
        mail: IMAP 연결 객체
        sender: 발신자 이메일 주소 (부분 일치)
        subject: 제목 키워드 (부분 일치)
        since_date: 이 날짜 이후의 이메일만 검색 (형식: "01-Jan-2023")
        unseen_only: 읽지 않은 이메일만 검색할지 여부

    Returns:
        검색된 이메일 ID 목록
    """
    search_criteria = []

    # 읽지 않은 이메일만 검색
    if unseen_only:
        search_criteria.append("UNSEEN")

    # 발신자로 검색
    if sender:
        search_criteria.append(f'FROM "{sender}"')

    # 제목으로 검색
    if subject:
        search_criteria.append(f'SUBJECT "{subject}"')

    # 날짜로 검색
    if since_date:
        search_criteria.append(f'SINCE "{since_date}"')

    # 검색 조건이 없으면 모든 이메일 검색
    if not search_criteria:
        search_criteria.append("ALL")

    # 검색 조건 결합
    search_str = " ".join(search_criteria)
    logger.info(f"검색 조건: {search_str}")

    try:
        status, data = mail.search(None, search_str)
        if status != "OK":
            logger.error(f"이메일 검색 실패: {data}")
            return []

        # 이메일 ID 목록 반환
        email_ids = data[0].split()
        logger.info(f"총 {len(email_ids)}개의 이메일을 찾았습니다.")
        return [id.decode() for id in email_ids]

    except Exception as e:
        logger.error(f"이메일 검색 중 오류 발생: {e}")
        return []


def fetch_email_details(mail: imaplib.IMAP4_SSL, email_id: str) -> Dict[str, Any]:
    """
    특정 이메일의 상세 정보를 가져옵니다.

    Args:
        mail: IMAP 연결 객체
        email_id: 이메일 ID

    Returns:
        이메일 상세 정보를 담은 사전
    """
    try:
        status, data = mail.fetch(email_id, "(RFC822)")
        if status != "OK":
            logger.error(f"이메일 {email_id} 가져오기 실패: {data}")
            return {}

        # 이메일 파싱
        raw_email = data[0][1]
        msg = email.message_from_bytes(raw_email)

        # 기본 헤더 정보 추출
        subject = decode_header_str(msg["Subject"])
        from_addr = decode_header_str(msg["From"])
        to_addr = decode_header_str(msg["To"])
        date_str = msg["Date"]
        formatted_date = format_date(date_str) if date_str else "날짜 없음"

        # 이메일 본문 추출
        text_body, html_body = get_email_body(msg)

        # 첨부 파일 정보 추출
        attachments = []
        if msg.is_multipart():
            for part in msg.walk():
                content_disposition = str(part.get("Content-Disposition"))
                if "attachment" in content_disposition:
                    filename = part.get_filename()
                    if filename:
                        filename = decode_header_str(filename)
                        attachments.append(filename)

        # 이메일 요약 정보 반환
        return {
            "id": email_id,
            "subject": subject,
            "from": from_addr,
            "from_email": extract_email_address(from_addr),
            "to": to_addr,
            "date": formatted_date,
            "text_body": text_body,
            "html_body": html_body,
            "has_attachments": bool(attachments),
            "attachments": attachments,
            "raw_message": msg,
        }

    except Exception as e:
        logger.error(f"이메일 {email_id} 상세 정보 가져오기 중 오류 발생: {e}")
        return {}


def display_email_summary(
    email_details: Dict[str, Any], show_body: bool = False
) -> None:
    """
    이메일 요약 정보를 표시합니다.

    Args:
        email_details: 이메일 상세 정보
        show_body: 본문도 표시할지 여부
    """
    if not email_details:
        print("이메일 정보가 없습니다.")
        return

    print("\n" + "=" * 80)
    print(f"제목: {email_details['subject']}")
    print(f"발신자: {email_details['from']}")
    print(f"수신자: {email_details['to']}")
    print(f"날짜: {email_details['date']}")

    if email_details["has_attachments"]:
        print(f"첨부 파일: {', '.join(email_details['attachments'])}")

    if show_body and email_details["text_body"]:
        print("\n" + "-" * 40 + " 본문 " + "-" * 40)
        # 본문 일부만 표시 (너무 길면 잘라냄)
        body_preview = email_details["text_body"][:500]
        if len(email_details["text_body"]) > 500:
            body_preview += "... (생략됨)"
        print(body_preview)

    print("=" * 80)


def check_emails(
    limit: int = 10,
    sender: str = None,
    subject: str = None,
    since_date: str = None,
    show_body: bool = False,
    unseen_only: bool = False,
    mark_as_read: bool = False,
) -> None:
    """
    이메일을 검색하고 상세 정보를 표시합니다.

    Args:
        limit: 최대 표시할 이메일 수
        sender: 발신자 이메일 주소 (부분 일치)
        subject: 제목 키워드 (부분 일치)
        since_date: 이 날짜 이후의 이메일만 검색 (형식: "01-Jan-2023")
        show_body: 이메일 본문을 표시할지 여부
        unseen_only: 읽지 않은 이메일만 표시할지 여부
        mark_as_read: 확인한 이메일을 읽음으로 표시할지 여부
    """
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
        # 이메일 검색
        email_ids = search_emails(mail, sender, subject, since_date, unseen_only)

        if not email_ids:
            print("조건에 맞는 이메일이 없습니다.")
            return

        # 최신 이메일부터 표시
        email_ids.reverse()
        # 제한된 수만큼만 처리
        email_ids = email_ids[:limit]

        print(f"\n{len(email_ids)}개의 이메일을 표시합니다:")

        for i, email_id in enumerate(email_ids, 1):
            email_details = fetch_email_details(mail, email_id)
            if not email_details:
                continue

            print(f"\n[{i}/{len(email_ids)}] ", end="")
            display_email_summary(email_details, show_body)

            # 이메일을 읽음으로 표시
            if mark_as_read:
                mail.store(email_id, "+FLAGS", r"\Seen")
                logger.debug(f"이메일 {email_id}를 읽음으로 표시했습니다.")

            # 사용자 입력 처리 (여러 이메일 표시 시 계속 진행 여부 확인)
            if i < len(email_ids):
                user_input = input(
                    "\n다음 이메일을 표시하려면 Enter를 누르세요 (q를 누르면 종료): "
                )
                if user_input.lower() == "q":
                    break

    except Exception as e:
        logger.error(f"이메일 확인 중 오류 발생: {e}")

    finally:
        # 연결 종료
        try:
            mail.close()
            mail.logout()
            logger.info("IMAP 서버 연결이 종료되었습니다.")
        except Exception:
            pass


def main():
    """
    메인 함수: 커맨드 라인 인자 처리 및 이메일 확인 실행
    """
    parser = argparse.ArgumentParser(description="네이버 메일 받은 편지함 확인 도구")

    parser.add_argument(
        "--limit", type=int, default=10, help="최대 표시할 이메일 수 (기본값: 10)"
    )
    parser.add_argument("--sender", type=str, help="발신자 이메일 주소 (부분 일치)")
    parser.add_argument("--subject", type=str, help="제목 키워드 (부분 일치)")
    parser.add_argument(
        "--since", type=str, help="이 날짜 이후의 이메일만 표시 (형식: '01-Jan-2023')"
    )
    parser.add_argument(
        "--show-body", action="store_true", help="이메일 본문을 표시합니다"
    )
    parser.add_argument(
        "--unseen-only", action="store_true", help="읽지 않은 이메일만 표시합니다"
    )
    parser.add_argument(
        "--mark-as-read",
        action="store_true",
        help="확인한 이메일을 읽음으로 표시합니다",
    )
    parser.add_argument("--verbose", action="store_true", help="상세 로그를 출력합니다")

    args = parser.parse_args()

    # 로그 레벨 설정
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # 이메일 확인 실행
    check_emails(
        limit=args.limit,
        sender=args.sender,
        subject=args.subject,
        since_date=args.since,
        show_body=args.show_body,
        unseen_only=args.unseen_only,
        mark_as_read=args.mark_as_read,
    )


if __name__ == "__main__":
    main()
