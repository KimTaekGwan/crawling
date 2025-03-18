"""
Module for automatically sending emails through Naver Mail.

이 모듈은 네이버 메일을 통해 자동으로 이메일을 전송하는 기능을 제공합니다.
데이터베이스에 저장된 URL 정보에서 이메일 주소를 추출하여 이메일을 전송합니다.

템플릿 파일 사용법:
- HTML 템플릿: templates/email_template.html
- 텍스트 템플릿: templates/email_template.txt

이메일 설정은 config.py 파일 또는 .env 파일에서 관리됩니다.
.env 파일 예시:
```
EMAIL_SENDER=your_email@naver.com
EMAIL_PASSWORD=your_password_or_app_password
```

터미널에서 실행:
```
./naver-email [options]
```

테스트 이메일 전송:
```
./naver-email --test-email recipient@example.com
```
"""

import os
import time
import logging
import sqlite3
import concurrent.futures
import signal
import sys
import threading
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Set, Tuple

import src.config as config
from src.db_storage import get_db_connection, filter_urls_by_keywords, initialize_db

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 데이터베이스 파일명
DB_FILENAME = config.DEFAULT_DB_FILENAME

# 병렬 처리 수 설정
_parallel_count = config.EMAIL_PARALLEL_COUNT

# 강제 실행 여부 플래그
_force_run = False

# 전송된 메일 개수 카운터
_sent_count = 0
_error_count = 0
_no_email_count = 0
_already_sent_count = 0
_total_count = 0

# 카운터 락
_counter_lock = threading.Lock()

# 종료 플래그
_terminate = False


def set_force_run(force=False):
    """
    강제 실행 여부를 설정합니다.

    Args:
        force: 강제 실행 활성화 여부
    """
    global _force_run
    _force_run = force
    if _force_run:
        logger.info(
            "강제 실행 모드가 활성화되었습니다. 이미 전송된 이메일도 다시 전송합니다."
        )


def set_parallel_count(count=4):
    """
    병렬 처리 수를 설정합니다.

    Args:
        count: 동시에 처리할 이메일 수 (기본값: 4)
    """
    global _parallel_count
    _parallel_count = max(1, count)  # 최소 1 이상
    logger.info(f"병렬 처리 수가 {_parallel_count}로 설정되었습니다.")


def update_email_status(
    conn: sqlite3.Connection, url: str, status: int, commit: bool = True
) -> None:
    """
    이메일 전송 상태를 업데이트합니다.

    Args:
        conn: 데이터베이스 연결 객체
        url: 업데이트할 URL
        status: 새 상태 코드
        commit: 커밋 여부 (기본값: True)
    """
    try:
        # websites 테이블에 email_status 및 email_date 컬럼이 없으면 추가
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(websites)")
        columns = [row["name"] for row in cursor.fetchall()]

        if "email_status" not in columns:
            cursor.execute(
                "ALTER TABLE websites ADD COLUMN email_status INTEGER DEFAULT 0"
            )
            logger.info("websites 테이블에 email_status 컬럼을 추가했습니다.")

        if "email_date" not in columns:
            cursor.execute("ALTER TABLE websites ADD COLUMN email_date TIMESTAMP")
            logger.info("websites 테이블에 email_date 컬럼을 추가했습니다.")

        # 상태 업데이트
        cursor.execute(
            """
            UPDATE websites 
            SET email_status = ?, email_date = CURRENT_TIMESTAMP
            WHERE url = ?
            """,
            (status, url),
        )
        if commit:
            conn.commit()
        logger.debug(f"URL {url}의 이메일 상태가 {status}로 업데이트되었습니다.")
    except sqlite3.Error as e:
        logger.error(f"데이터베이스 업데이트 오류: {e}")
        if commit:
            conn.rollback()


def send_email(
    recipient_email: str, subject: str = None, custom_content: str = None
) -> bool:
    """
    네이버 메일을 통해 이메일을 전송합니다.

    Args:
        recipient_email: 수신자 이메일 주소
        subject: 이메일 제목 (None인 경우 config에서 가져옴)
        custom_content: 사용자 정의 내용 (None인 경우 config에서 가져옴)

    Returns:
        성공 여부 (True/False)
    """
    try:
        # SMTP 서버 설정
        smtp_server = config.EMAIL_SMTP_SERVER
        smtp_port = config.EMAIL_SMTP_PORT
        sender_email = config.EMAIL_SENDER
        password = config.EMAIL_PASSWORD

        # 제목 설정
        if subject is None:
            subject = config.EMAIL_SUBJECT

        # 메시지 생성
        msg = MIMEMultipart("alternative")
        msg["From"] = sender_email
        msg["To"] = recipient_email
        msg["Subject"] = subject

        # 텍스트 버전 추가
        text_part_content = config.EMAIL_TEXT_CONTENT
        if not text_part_content:
            logger.error(
                "텍스트 이메일 내용이 비어 있습니다. 템플릿 파일을 확인해주세요."
            )
            return False

        text_part = MIMEText(text_part_content, "plain", "utf-8")
        msg.attach(text_part)

        # HTML 버전 추가
        html_part_content = (
            custom_content if custom_content else config.EMAIL_HTML_CONTENT
        )
        if not html_part_content:
            logger.error(
                "HTML 이메일 내용이 비어 있습니다. 템플릿 파일을 확인해주세요."
            )
            return False

        html_part = MIMEText(html_part_content, "html", "utf-8")
        msg.attach(html_part)

        # SMTP 연결 및 메일 전송
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # TLS 보안 처리
            server.login(sender_email, password)
            server.sendmail(sender_email, recipient_email, msg.as_string())

        logger.info(f"{recipient_email}에게 이메일을 성공적으로 전송했습니다.")
        return True

    except Exception as e:
        logger.error(f"{recipient_email}에게 이메일 전송 중 오류 발생: {e}")
        return False


def process_email_for_url(conn: sqlite3.Connection, url: str) -> int:
    """
    URL에 해당하는 웹사이트의 이메일로 메시지를 전송합니다.

    Args:
        conn: 데이터베이스 연결 객체
        url: 대상 URL

    Returns:
        상태 코드 (config.EMAIL_STATUS 참조)
    """
    global _sent_count, _error_count, _no_email_count, _already_sent_count, _terminate

    # 종료 신호 확인
    if _terminate:
        return config.EMAIL_STATUS["ERROR"]

    try:
        # URL에 대한 정보 조회
        cursor = conn.cursor()
        cursor.execute("SELECT email, email_status FROM websites WHERE url = ?", (url,))
        row = cursor.fetchone()

        if not row:
            logger.warning(f"URL {url}에 대한 정보를 찾을 수 없습니다.")
            return config.EMAIL_STATUS["ERROR"]

        email_address = row["email"]
        current_status = row.get("email_status", 0)  # 컬럼이 없을 경우 0(NOT_SENT) 반환

        # 이미 전송된 경우 (강제 실행 모드가 아닌 경우)
        if not _force_run and current_status == config.EMAIL_STATUS["SENT"]:
            with _counter_lock:
                _already_sent_count += 1
            logger.info(f"URL {url}에 대한 이메일은 이미 전송되었습니다.")
            return config.EMAIL_STATUS["ALREADY_SENT"]

        # 이메일 주소가 없는 경우
        if not email_address:
            with _counter_lock:
                _no_email_count += 1
            logger.warning(f"URL {url}에 이메일 주소가 없습니다.")
            return config.EMAIL_STATUS["NO_EMAIL"]

        # 이메일 전송
        success = send_email(email_address)

        if success:
            with _counter_lock:
                _sent_count += 1
            logger.info(
                f"URL {url}의 이메일 {email_address}로 메시지를 성공적으로 전송했습니다."
            )
            return config.EMAIL_STATUS["SENT"]
        else:
            with _counter_lock:
                _error_count += 1
            logger.error(
                f"URL {url}의 이메일 {email_address}로 메시지 전송에 실패했습니다."
            )
            return config.EMAIL_STATUS["ERROR"]

    except Exception as e:
        with _counter_lock:
            _error_count += 1
        logger.error(f"URL {url} 처리 중 오류 발생: {e}")
        return config.EMAIL_STATUS["ERROR"]


def process_email_thread(conn: sqlite3.Connection, url: str) -> None:
    """
    스레드에서 실행될 URL 처리 함수입니다.

    Args:
        conn: 데이터베이스 연결 객체
        url: 처리할 URL
    """
    status = process_email_for_url(conn, url)
    update_email_status(conn, url, status)

    # 처리 사이에 약간의 딜레이 추가
    time.sleep(config.EMAIL_BETWEEN_DELAY)


def process_url_batch(urls: List[str]) -> None:
    """
    URL 배치를 병렬로 처리합니다.

    Args:
        urls: 처리할 URL 목록
    """
    global _total_count, _terminate

    # 로컬 데이터베이스 연결 (각 스레드에서 별도의 연결 사용)
    conn = get_db_connection(DB_FILENAME)

    try:
        # 병렬 처리를 위한 스레드 풀 생성
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=_parallel_count
        ) as executor:
            # 각 URL에 대해 이메일 전송 함수 실행
            future_to_url = {
                executor.submit(process_email_thread, conn, url): url for url in urls
            }

            # 완료된 작업 처리
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    future.result()  # 결과 대기 (예외 발생 시 처리)
                except Exception as e:
                    logger.error(f"URL {url} 처리 중 예외 발생: {e}")

                # 진행 상황 업데이트
                with _counter_lock:
                    _total_count += 1
                    completion = (_total_count / len(urls)) * 100

                if _total_count % 10 == 0 or _total_count == len(urls):
                    logger.info(
                        f"진행 상황: {_total_count}/{len(urls)} URLs 처리됨 ({completion:.1f}%)"
                    )
                    logger.info(
                        f"전송: {_sent_count}, 에러: {_error_count}, "
                        f"이메일 없음: {_no_email_count}, 이미 전송됨: {_already_sent_count}"
                    )

                # 종료 플래그 확인
                if _terminate:
                    logger.info("종료 요청을 받았습니다. URL 처리를 중단합니다.")
                    break

    finally:
        # 데이터베이스 연결 종료
        conn.close()


def signal_handler(sig, frame):
    """
    SIGINT, SIGTERM 시그널 핸들러입니다.
    Ctrl+C 또는 종료 요청 시 실행됩니다.
    """
    global _terminate
    logger.info("종료 신호를 받았습니다. 현재 작업을 완료 후 프로그램을 종료합니다.")
    _terminate = True


def send_emails_for_websites(
    db_filename: str = None, email_filter: Dict = None, batch_size: int = 100
) -> None:
    """
    데이터베이스의 웹사이트 정보를 기반으로 이메일을 전송합니다.

    Args:
        db_filename: 데이터베이스 파일 경로 (None인 경우 기본값 사용)
        email_filter: 이메일 필터링 조건 (None인 경우 모든 URL 대상)
        batch_size: 한 번에 처리할 URL 배치 크기
    """
    global _sent_count, _error_count, _no_email_count, _already_sent_count, _total_count, _terminate

    # 데이터베이스 파일명 설정
    if db_filename is None:
        db_filename = DB_FILENAME

    # 시그널 핸들러 등록 (Ctrl+C 및 종료 신호 처리)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 시작 시간 기록
    start_time = datetime.now()
    logger.info(f"이메일 전송 작업 시작: {start_time}")

    # 데이터베이스 연결
    conn = get_db_connection(db_filename)

    try:
        # websites 테이블에 필요한 컬럼 추가 (없는 경우)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(websites)")
        columns = [row["name"] for row in cursor.fetchall()]

        migrations = []
        if "email_status" not in columns:
            migrations.append(
                "ALTER TABLE websites ADD COLUMN email_status INTEGER DEFAULT 0"
            )

        if "email_date" not in columns:
            migrations.append("ALTER TABLE websites ADD COLUMN email_date TIMESTAMP")

        for migration in migrations:
            cursor.execute(migration)

        if migrations:
            conn.commit()
            logger.info("데이터베이스 스키마 마이그레이션 완료")

        # 처리할 URL 목록 가져오기
        if email_filter:
            urls = filter_urls_by_keywords(conn, email_filter)
            logger.info(f"필터링된 {len(urls)}개의 URL을 처리합니다.")
        else:
            # email이 있는 모든 URL 가져오기
            cursor.execute(
                """
                SELECT url FROM websites 
                WHERE email IS NOT NULL AND email != ''
                ORDER BY url
                """
            )
            urls = [row["url"] for row in cursor.fetchall()]
            logger.info(f"총 {len(urls)}개의 URL을 처리합니다.")

        if not urls:
            logger.warning("처리할 URL이 없습니다.")
            return

        # URL을 배치로 나누기
        batches = [urls[i : i + batch_size] for i in range(0, len(urls), batch_size)]
        logger.info(
            f"{len(batches)}개의 배치로 나누어 처리합니다. (배치당 최대 {batch_size}개)"
        )

        # 각 배치 처리
        for i, batch in enumerate(batches, 1):
            if _terminate:
                logger.info("종료 요청으로 인해 남은 배치 처리를 중단합니다.")
                break

            logger.info(f"배치 {i}/{len(batches)} 처리 중 ({len(batch)}개 URL)")
            process_url_batch(batch)

            # 배치 간 잠시 대기
            if i < len(batches) and not _terminate:
                logger.info("다음 배치로 넘어가기 전에 5초 대기합니다...")
                time.sleep(5)

        # 종료 시간 및 통계 출력
        end_time = datetime.now()
        elapsed = end_time - start_time
        logger.info(f"이메일 전송 작업 완료: {end_time} (소요 시간: {elapsed})")
        logger.info(
            f"총 URL: {len(urls)}, 전송 성공: {_sent_count}, 오류: {_error_count}, "
            f"이메일 없음: {_no_email_count}, 이미 전송됨: {_already_sent_count}"
        )

    except Exception as e:
        logger.error(f"이메일 전송 작업 중 오류 발생: {e}")

    finally:
        # 데이터베이스 연결 종료
        conn.close()


def main():
    """
    메인 함수: 커맨드 라인 인자 처리 및 이메일 전송 실행
    """
    import argparse

    # 템플릿 파일 확인
    if not hasattr(config, "HTML_TEMPLATE_EXISTS") or not hasattr(
        config, "TEXT_TEMPLATE_EXISTS"
    ):
        logger.error("템플릿 파일 존재 여부를 확인할 수 없습니다.")
    else:
        if not config.HTML_TEMPLATE_EXISTS:
            logger.warning(
                f"HTML 템플릿 파일이 없습니다: {config.EMAIL_HTML_TEMPLATE_PATH}"
            )
            logger.warning("기본 HTML 템플릿을 사용합니다.")

        if not config.TEXT_TEMPLATE_EXISTS:
            logger.warning(
                f"텍스트 템플릿 파일이 없습니다: {config.EMAIL_TEXT_TEMPLATE_PATH}"
            )
            logger.warning("기본 텍스트 템플릿을 사용합니다.")

        if not config.HTML_TEMPLATE_EXISTS or not config.TEXT_TEMPLATE_EXISTS:
            logger.warning(f"템플릿 파일이 없습니다. 템플릿 파일을 생성하려면:")
            logger.warning(
                f"1. 디렉토리 확인: {config.TEMPLATES_DIR} 디렉토리가 존재하는지 확인"
            )
            logger.warning(
                f"2. HTML 템플릿 파일 생성: {config.EMAIL_HTML_TEMPLATE_PATH}"
            )
            logger.warning(
                f"3. 텍스트 템플릿 파일 생성: {config.EMAIL_TEXT_TEMPLATE_PATH}"
            )

            # 사용자 확인 요청
            confirm = input(
                "템플릿 파일이 없습니다. 기본 템플릿으로 계속 진행하시겠습니까? (y/n): "
            )
            if confirm.lower() not in ("y", "yes"):
                logger.info("사용자가 취소했습니다. 프로그램을 종료합니다.")
                sys.exit(0)

            logger.info("기본 템플릿으로 계속 진행합니다.")

    # 명령행 인자 파싱
    parser = argparse.ArgumentParser(
        description="네이버 메일을 통한 이메일 자동 전송 도구"
    )
    parser.add_argument(
        "--force", action="store_true", help="이미 전송된 이메일도 다시 전송합니다."
    )
    parser.add_argument(
        "--db",
        type=str,
        default=DB_FILENAME,
        help=f"데이터베이스 파일 (기본값: {DB_FILENAME})",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=_parallel_count,
        help=f"병렬 처리 수 (기본값: {_parallel_count})",
    )
    parser.add_argument(
        "--batch-size", type=int, default=100, help="배치당 URL 수 (기본값: 100)"
    )
    parser.add_argument(
        "--include", type=str, nargs="+", help="포함할 키워드 목록 (URL 필터링)"
    )
    parser.add_argument(
        "--exclude", type=str, nargs="+", help="제외할 키워드 목록 (URL 필터링)"
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="로그 레벨 설정 (기본값: INFO)",
    )
    parser.add_argument(
        "--test-email",
        type=str,
        help="테스트 이메일 주소 (지정 시 해당 주소로만 테스트 메일을 전송합니다)",
    )
    parser.add_argument(
        "--test-emails",
        type=str,
        nargs="+",
        help="여러 테스트 이메일 주소 (지정 시 해당 주소들로만 테스트 메일을 전송합니다)",
    )
    parser.add_argument(
        "--subject",
        type=str,
        help="이메일 제목 (테스트 이메일 전송 시 사용됩니다)",
    )
    parser.add_argument(
        "--text-content",
        type=str,
        help="테스트 이메일의 텍스트 내용 (지정 시 기본값 대신 사용됩니다)",
    )
    parser.add_argument(
        "--html-content",
        type=str,
        help="테스트 이메일의 HTML 내용 (지정 시 기본값 대신 사용됩니다)",
    )
    parser.add_argument(
        "--html-file",
        type=str,
        help="테스트 이메일의 HTML 내용이 있는 파일 경로 (지정 시 --html-content보다 우선됩니다)",
    )
    parser.add_argument(
        "--text-file",
        type=str,
        help="테스트 이메일의 텍스트 내용이 있는 파일 경로 (지정 시 --text-content보다 우선됩니다)",
    )

    args = parser.parse_args()

    # 로그 레벨 설정
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # 강제 실행 설정
    set_force_run(args.force)

    # 병렬 처리 수 설정
    set_parallel_count(args.parallel)

    # 테스트 이메일 전송 모드 확인
    if args.test_email or args.test_emails:
        # HTML 및 텍스트 내용 읽기
        html_content = None
        text_content = None

        # HTML 파일에서 내용 읽기
        if args.html_file:
            try:
                with open(args.html_file, "r", encoding="utf-8") as f:
                    html_content = f.read()
                logger.info(f"HTML 내용을 파일 {args.html_file}에서 읽었습니다.")
            except Exception as e:
                logger.error(f"HTML 파일 {args.html_file} 읽기 실패: {e}")
        elif args.html_content:
            html_content = args.html_content

        # 텍스트 파일에서 내용 읽기
        if args.text_file:
            try:
                with open(args.text_file, "r", encoding="utf-8") as f:
                    text_content = f.read()
                logger.info(f"텍스트 내용을 파일 {args.text_file}에서 읽었습니다.")
            except Exception as e:
                logger.error(f"텍스트 파일 {args.text_file} 읽기 실패: {e}")
        elif args.text_content:
            text_content = args.text_content

        send_test_emails(
            args.test_email, args.test_emails, args.subject, html_content, text_content
        )
        return

    # 필터 설정
    email_filter = {}
    if args.include:
        email_filter["include"] = args.include
    if args.exclude:
        email_filter["exclude"] = args.exclude

    # 이메일 전송 실행
    send_emails_for_websites(
        db_filename=args.db,
        email_filter=email_filter if email_filter else None,
        batch_size=args.batch_size,
    )


def send_test_emails(
    single_email: str = None,
    multiple_emails: List[str] = None,
    subject: str = None,
    html_content: str = None,
    text_content: str = None,
) -> None:
    """
    테스트 목적으로 특정 이메일 주소로 메일을 보냅니다.

    Args:
        single_email: 단일 이메일 주소
        multiple_emails: 여러 이메일 주소 목록
        subject: 이메일 제목 (None인 경우 config에서 가져옴)
        html_content: HTML 내용 (None인 경우 config에서 가져옴)
        text_content: 텍스트 내용 (None인 경우 config에서 가져옴)
    """
    # 이메일 주소 목록 생성
    email_addresses = []
    if single_email:
        email_addresses.append(single_email)
    if multiple_emails:
        email_addresses.extend(multiple_emails)

    if not email_addresses:
        logger.error("테스트 이메일 주소가 지정되지 않았습니다.")
        return

    # 커스텀 내용이 없는 경우 템플릿 확인
    if not html_content and not text_content:
        # 템플릿 파일 확인
        if hasattr(config, "HTML_TEMPLATE_EXISTS") and hasattr(
            config, "TEXT_TEMPLATE_EXISTS"
        ):
            if not config.HTML_TEMPLATE_EXISTS:
                logger.warning(
                    f"HTML 템플릿 파일이 없습니다: {config.EMAIL_HTML_TEMPLATE_PATH}"
                )
                logger.warning("기본 HTML 템플릿을 사용합니다.")

            if not config.TEXT_TEMPLATE_EXISTS:
                logger.warning(
                    f"텍스트 템플릿 파일이 없습니다: {config.EMAIL_TEXT_TEMPLATE_PATH}"
                )
                logger.warning("기본 텍스트 템플릿을 사용합니다.")

            if not config.HTML_TEMPLATE_EXISTS or not config.TEXT_TEMPLATE_EXISTS:
                # 테스트 이메일을 보내는 경우에는 자동으로 진행
                logger.warning(
                    "테스트 모드에서는 기본 템플릿을 사용하여 계속 진행합니다."
                )

    logger.info(
        f"테스트 모드: {len(email_addresses)}개의 이메일 주소로 메일을 전송합니다."
    )

    # 각 이메일 주소로 메일 전송
    success_count = 0
    error_count = 0

    for email in email_addresses:
        logger.info(f"테스트 이메일 {email}로 전송 시도 중...")

        # 사용자 정의 내용으로 이메일 전송
        if html_content or text_content:
            # send_email 함수를 직접 호출하지 않고 내부 구현을 다시 작성
            try:
                # SMTP 서버 설정
                smtp_server = config.EMAIL_SMTP_SERVER
                smtp_port = config.EMAIL_SMTP_PORT
                sender_email = config.EMAIL_SENDER
                password = config.EMAIL_PASSWORD

                # 제목 설정
                email_subject = subject if subject else config.EMAIL_SUBJECT

                # 메시지 생성
                msg = MIMEMultipart("alternative")
                msg["From"] = sender_email
                msg["To"] = email
                msg["Subject"] = email_subject

                # 텍스트 버전 추가
                text_part_content = (
                    text_content if text_content else config.EMAIL_TEXT_CONTENT
                )
                text_part = MIMEText(text_part_content, "plain", "utf-8")
                msg.attach(text_part)

                # HTML 버전 추가
                html_part_content = (
                    html_content if html_content else config.EMAIL_HTML_CONTENT
                )
                html_part = MIMEText(html_part_content, "html", "utf-8")
                msg.attach(html_part)

                # SMTP 연결 및 메일 전송
                with smtplib.SMTP(smtp_server, smtp_port) as server:
                    server.starttls()  # TLS 보안 처리
                    server.login(sender_email, password)
                    server.sendmail(sender_email, email, msg.as_string())

                success_count += 1
                logger.info(f"{email}로 테스트 이메일 전송 성공 (커스텀 내용)")

            except Exception as e:
                error_count += 1
                logger.error(f"{email}에게 이메일 전송 중 오류 발생: {e}")
        else:
            # 기본 내용으로 이메일 전송
            if send_email(email, subject):
                success_count += 1
                logger.info(f"{email}로 테스트 이메일 전송 성공")
            else:
                error_count += 1
                logger.error(f"{email}로 테스트 이메일 전송 실패")

    # 결과 출력
    logger.info(
        f"테스트 이메일 전송 완료: 성공 {success_count}개, 실패 {error_count}개 (총 {len(email_addresses)}개)"
    )


if __name__ == "__main__":
    main()
