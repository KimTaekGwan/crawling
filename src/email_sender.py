"""
Module for automatically sending emails through Naver Mail using BCC batching.

이 모듈은 네이버 메일을 통해 자동으로 이메일을 전송하는 기능을 제공합니다.
데이터베이스에 저장된 URL 정보에서 이메일 주소를 추출하여
설정된 배치 크기만큼 묶어 BCC(숨은 참조)로 이메일을 전송합니다.

템플릿 파일 사용법:
- HTML 템플릿: templates/email_template.html
- 텍스트 템플릿: templates/email_template.txt

이메일 설정은 config.py 파일 또는 .env 파일에서 관리됩니다.
.env 파일 예시:
```
EMAIL_SENDER=your_email@naver.com
EMAIL_PASSWORD=your_password_or_app_password
EMAIL_BCC_BATCH_SIZE=50
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
    # conn이 None이면 새 연결 생성
    thread_local_conn = conn is None
    if thread_local_conn:
        conn = get_db_connection(DB_FILENAME)

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
    finally:
        # 이 함수 내에서 생성한 연결이면 여기서 닫음
        if thread_local_conn and conn:
            conn.close()


def update_batch_email_status(
    conn: sqlite3.Connection, url_status_map: Dict[str, int], commit: bool = True
) -> int:
    """
    여러 URL의 이메일 전송 상태를 한 번에 업데이트합니다.

    Args:
        conn: 데이터베이스 연결 객체
        url_status_map: URL과 상태 코드의 매핑 딕셔너리
        commit: 커밋 여부 (기본값: True)

    Returns:
        업데이트된 레코드 수
    """
    if not url_status_map:
        return 0

    # conn이 None이면 새 연결 생성
    thread_local_conn = conn is None
    if thread_local_conn:
        conn = get_db_connection(DB_FILENAME)

    updated_count = 0
    try:
        # websites 테이블에 email_status 및 email_date 컬럼이 없으면 추가 (한 번만 확인)
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

        # 각 URL의 상태 업데이트 (트랜잭션 하나로 처리)
        for url, status in url_status_map.items():
            cursor.execute(
                """
                UPDATE websites 
                SET email_status = ?, email_date = CURRENT_TIMESTAMP
                WHERE url = ?
                """,
                (status, url),
            )
            updated_count += cursor.rowcount

        if commit:
            conn.commit()
            logger.info(
                f"총 {updated_count}개 URL의 이메일 상태가 성공적으로 업데이트되었습니다."
            )
    except sqlite3.Error as e:
        logger.error(f"데이터베이스 배치 업데이트 오류: {e}")
        if commit:
            conn.rollback()
        updated_count = 0
    finally:
        # 이 함수 내에서 생성한 연결이면 여기서 닫음
        if thread_local_conn and conn:
            conn.close()

    return updated_count


def send_bcc_batch_email(
    recipient_emails: List[str], subject: str = None, custom_content: str = None
) -> Tuple[bool, List[str]]:
    """
    여러 수신자에게 숨은 참조(BCC)로 이메일을 한 번에 전송합니다.

    Args:
        recipient_emails: 수신자 이메일 주소 목록
        subject: 이메일 제목 (None인 경우 config에서 가져옴)
        custom_content: 사용자 정의 내용 (None인 경우 config에서 가져옴)

    Returns:
        (성공 여부, 이메일 주소 목록) 튜플. 성공하면 전체 목록 반환, 실패하면 빈 목록 반환
    """
    if not recipient_emails:
        logger.warning("수신자 이메일 주소 목록이 비어 있습니다.")
        return False, []

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
        # To 필드는 발신자로 설정 (수신자는 BCC로 처리)
        msg["To"] = sender_email
        # BCC 필드 설정
        msg["Bcc"] = ", ".join(recipient_emails)
        msg["Subject"] = subject

        # 텍스트 버전 추가
        text_part_content = config.EMAIL_TEXT_CONTENT
        if not text_part_content:
            logger.error(
                "텍스트 이메일 내용이 비어 있습니다. 템플릿 파일을 확인해주세요."
            )
            return False, []

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
            return False, []

        html_part = MIMEText(html_part_content, "html", "utf-8")
        msg.attach(html_part)

        # SMTP 연결 및 메일 전송
        match config.EMAIL_SMTP_PORT:
            case 465:
                with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
                    server.login(sender_email, password)
                    # BCC 필드의 주소들로 메일 전송 (From 주소는 발신자, To 주소도 발신자로 설정)
                    server.sendmail(
                        sender_email, [sender_email] + recipient_emails, msg.as_string()
                    )
            case 587:
                with smtplib.SMTP(smtp_server, smtp_port) as server:
                    server.starttls()  # TLS 보안 처리
                    server.login(sender_email, password)
                    # BCC 필드의 주소들로 메일 전송 (From 주소는 발신자, To 주소도 발신자로 설정)
                    server.sendmail(
                        sender_email, [sender_email] + recipient_emails, msg.as_string()
                    )

        logger.info(
            f"{len(recipient_emails)}명의 수신자에게 BCC로 이메일을 성공적으로 전송했습니다."
        )
        return True, recipient_emails

    except Exception as e:
        logger.error(f"BCC 이메일 전송 중 오류 발생: {e}")
        return False, []


def signal_handler(sig, frame):
    """
    SIGINT, SIGTERM 시그널 핸들러입니다.
    Ctrl+C 또는 종료 요청 시 실행됩니다.
    """
    global _terminate
    logger.info("종료 신호를 받았습니다. 현재 작업을 완료 후 프로그램을 종료합니다.")
    _terminate = True


def display_email_summary(
    urls: List[str],
    email_details: List[Dict],
    emails_with_no_address: List[str],
    already_sent_count: int,
    bcc_batch_size: int,
) -> bool:
    """
    이메일 발송 요약 정보를 표시하고 사용자 확인을 요청합니다.

    Args:
        urls: 처리할 URL 목록
        email_details: 이메일 상세 정보 목록
        emails_with_no_address: 이메일 주소가 없는 URL 목록
        already_sent_count: 이미 전송된 이메일 수
        bcc_batch_size: BCC 배치 크기

    Returns:
        사용자가 발송을 확인했는지 여부 (True/False)
    """
    # 발송 예정 이메일 수
    total_emails_to_send = len(email_details)

    # 도메인별 통계 계산
    domain_counts = {}
    for detail in email_details:
        domain = detail["domain"]
        domain_counts[domain] = domain_counts.get(domain, 0) + 1

    # 배치 수 계산
    batch_count = (
        (total_emails_to_send + bcc_batch_size - 1) // bcc_batch_size
        if total_emails_to_send > 0
        else 0
    )

    # 발송 요약 정보 표시
    print("\n" + "=" * 60)
    print("📧 이메일 발송 요약 정보 (이미 전송된 항목 제외)")
    print("=" * 60)

    # 전체 처리 URL 수 (urls는 SQL 쿼리에서 이미 필터링된 URL 목록)
    total_processed_urls = len(urls) + already_sent_count
    print(f"전체 처리 대상 URL 수: {total_processed_urls}개")

    if already_sent_count > 0:
        print(
            f"이미 전송된 이메일(SENT/ALREADY_SENT): {already_sent_count}개 (발송 대상에서 제외됨)"
        )

    print(f"발송 대상 URL 수: {len(urls)}개")
    print(f"이메일 주소가 없는 URL 수: {len(emails_with_no_address)}개")
    print(f"실제 발송 예정 이메일 수: {total_emails_to_send}개")
    print(f"BCC 배치 크기: {bcc_batch_size}개 (총 {batch_count}개 배치)")

    # 도메인별 통계
    print("\n📊 도메인별 발송 통계:")
    for domain, count in sorted(
        domain_counts.items(), key=lambda x: x[1], reverse=True
    ):
        percent = (
            (count / total_emails_to_send) * 100 if total_emails_to_send > 0 else 0
        )
        print(f"  - {domain}: {count}개 ({percent:.1f}%)")

    # 이메일 샘플 표시 (처음 5개)
    if email_details:
        print("\n📋 발송 예정 이메일 샘플 (처음 5개):")
        for i, detail in enumerate(email_details[:5], 1):
            print(f"  {i}. {detail['url']} -> {detail['email']}")

        # 마지막 5개 (중복되지 않는 경우에만)
        if len(email_details) > 10:
            print("\n  ...")
            print("\n📋 발송 예정 이메일 샘플 (마지막 5개):")
            for i, detail in enumerate(email_details[-5:], len(email_details) - 4):
                print(f"  {i}. {detail['url']} -> {detail['email']}")

    print("\n" + "=" * 60)

    # 사용자 확인 요청
    confirm = input("\n위 정보로 이메일을 발송하시겠습니까? (y/n): ")
    return confirm.lower() in ("y", "yes")


def send_emails_for_websites(
    db_filename: str = None, email_filter: Dict = None, batch_size: int = 100
) -> None:
    """
    데이터베이스의 웹사이트 정보를 기반으로 이메일을 전송합니다.
    이미 성공적으로 전송된 이메일(email_status=1)은 처리 대상에서 제외됩니다.

    이메일은 config.EMAIL_BCC_BATCH_SIZE 설정에 따라 여러 명의 수신자에게 BCC로 한 번에 전송됩니다.

    Args:
        db_filename: 데이터베이스 파일 경로 (None인 경우 기본값 사용)
        email_filter: 이메일 필터링 조건 (None인 경우 모든 URL 대상)
        batch_size: URL 처리 배치 크기 (데이터베이스에서 조회 단위)
    """
    global _sent_count, _error_count, _no_email_count, _already_sent_count, _total_count, _terminate

    # 데이터베이스 파일명 설정
    if db_filename is None:
        db_filename = DB_FILENAME

    # BCC 배치 크기 설정
    bcc_batch_size = config.EMAIL_BCC_BATCH_SIZE

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
        already_sent_count = 0

        if email_filter:
            # 키워드 필터링된 URL 목록 가져오기
            urls = filter_urls_by_keywords(conn, email_filter)
            logger.info(f"키워드 필터링으로 {len(urls)}개의 URL을 찾았습니다.")

            # 전체 URL 수 기록
            total_found_urls = len(urls)

            # 이미 성공적으로 전송된 이메일은 제외
            cursor.execute(
                """
                SELECT url FROM websites 
                WHERE url IN ({}) AND (email_status IS NULL OR (email_status != ? AND email_status != ?)) 
                AND email IS NOT NULL AND email != ''
                ORDER BY url
                """.format(
                    ",".join(["?"] * len(urls))
                ),
                urls
                + [config.EMAIL_STATUS["SENT"], config.EMAIL_STATUS["ALREADY_SENT"]],
            )

            filtered_urls = [row["url"] for row in cursor.fetchall()]
            already_sent_count = len(urls) - len(filtered_urls)
            urls = filtered_urls

            logger.info(f"필터링된 {len(urls)}개의 URL을 처리합니다.")
            if already_sent_count > 0:
                logger.info(
                    f"{already_sent_count}개의 URL은 이미 성공적으로 이메일을 전송하여 제외되었습니다."
                )
        else:
            # 전체 이메일 주소가 있는 URL 수 먼저 확인
            cursor.execute(
                """
                SELECT COUNT(*) as total FROM websites 
                WHERE email IS NOT NULL AND email != ''
                """
            )
            row = cursor.fetchone()
            total_email_urls = row["total"] if row else 0

            # email이 있고 아직 성공적으로 전송되지 않은 URL만 가져오기
            cursor.execute(
                """
                SELECT url FROM websites 
                WHERE email IS NOT NULL AND email != '' 
                AND (email_status IS NULL OR (email_status != ? AND email_status != ?))
                ORDER BY url
                """,
                (config.EMAIL_STATUS["SENT"], config.EMAIL_STATUS["ALREADY_SENT"]),
            )
            urls = [row["url"] for row in cursor.fetchall()]
            already_sent_count = total_email_urls - len(urls)

            logger.info(f"이메일 주소가 있는 URL: 총 {total_email_urls}개")
            logger.info(
                f"이미 전송 완료된 URL: {already_sent_count}개 (SENT 또는 ALREADY_SENT 상태)"
            )
            logger.info(
                f"발송 대상 URL: {len(urls)}개 (이미 성공적으로 전송된 이메일은 제외)"
            )

        if not urls:
            logger.warning(
                "처리할 URL이 없습니다. 모든 이메일이 이미 성공적으로 전송되었거나 이메일 주소가 없습니다."
            )
            return

        # 이메일 주소 분석 및 발송 요약 정보 생성
        email_details = []
        emails_with_no_address = []

        # 상세 이메일 정보 수집
        for url in urls:
            try:
                cursor.execute(
                    "SELECT url, email, email_status FROM websites WHERE url = ?",
                    (url,),
                )
                row = cursor.fetchone()

                # 이메일이 있고 ALREADY_SENT, SENT 상태가 아닌 경우만 처리
                if (
                    row
                    and row["email"]
                    and (
                        row["email_status"] is None
                        or (
                            row["email_status"] != config.EMAIL_STATUS["SENT"]
                            and row["email_status"]
                            != config.EMAIL_STATUS["ALREADY_SENT"]
                        )
                    )
                ):
                    email_address = row["email"]
                    email_domain = (
                        email_address.split("@")[1]
                        if "@" in email_address
                        else "unknown"
                    )

                    # 이메일 상세 정보 추가
                    email_details.append(
                        {"url": url, "email": email_address, "domain": email_domain}
                    )
                else:
                    if (
                        row
                        and row["email"]
                        and (
                            row["email_status"] == config.EMAIL_STATUS["SENT"]
                            or row["email_status"]
                            == config.EMAIL_STATUS["ALREADY_SENT"]
                        )
                    ):
                        # 이미 전송된 이메일 카운트 증가
                        already_sent_count += 1
                    else:
                        emails_with_no_address.append(url)
            except Exception as e:
                logger.error(f"URL {url}의 이메일 분석 중 오류 발생: {e}")
                emails_with_no_address.append(url)

        # 발송 요약 정보 표시 및 사용자 확인
        if not display_email_summary(
            urls,
            email_details,
            emails_with_no_address,
            already_sent_count,
            bcc_batch_size,
        ):
            logger.info("사용자가 이메일 발송을 취소했습니다. 프로그램을 종료합니다.")
            return

        logger.info("사용자 확인 완료. BCC 배치 방식으로 이메일 발송을 시작합니다.")

        # 이메일이 없는 URL 먼저 처리
        if emails_with_no_address:
            no_email_status_updates = {
                url: config.EMAIL_STATUS["NO_EMAIL"] for url in emails_with_no_address
            }
            update_batch_email_status(conn, no_email_status_updates)
            _no_email_count += len(emails_with_no_address)
            logger.info(
                f"{len(emails_with_no_address)}개의 이메일 없는 URL 상태를 업데이트했습니다."
            )

        # 이메일 주소가 있는 항목을 BCC 배치로 처리
        total_batches = (
            (len(email_details) + bcc_batch_size - 1) // bcc_batch_size
            if email_details
            else 0
        )
        logger.info(
            f"이메일 주소가 있는 {len(email_details)}개 항목을 {total_batches}개의 BCC 배치로 처리합니다."
        )

        for batch_idx in range(0, len(email_details), bcc_batch_size):
            if _terminate:
                logger.info("종료 요청으로 인해 남은 배치 처리를 중단합니다.")
                break

            # 현재 배치 가져오기
            current_batch = email_details[batch_idx : batch_idx + bcc_batch_size]
            batch_emails = [item["email"] for item in current_batch]
            batch_urls = [item["url"] for item in current_batch]

            logger.info(
                f"배치 {batch_idx // bcc_batch_size + 1}/{total_batches} 처리 중 ({len(current_batch)}개 이메일)..."
            )

            # BCC로 배치 이메일 전송
            success, sent_emails = send_bcc_batch_email(batch_emails)

            # 상태 업데이트
            if success:
                # 성공한 경우 모든 URL의 상태를 SENT로 업데이트
                success_status_updates = {
                    url: config.EMAIL_STATUS["SENT"] for url in batch_urls
                }
                update_batch_email_status(conn, success_status_updates)
                _sent_count += len(current_batch)
                logger.info(
                    f"배치 {batch_idx // bcc_batch_size + 1} 전송 성공: {len(current_batch)}개 이메일"
                )
            else:
                # 실패한 경우 모든 URL의 상태를 ERROR로 업데이트
                error_status_updates = {
                    url: config.EMAIL_STATUS["ERROR"] for url in batch_urls
                }
                update_batch_email_status(conn, error_status_updates)
                _error_count += len(current_batch)
                logger.error(
                    f"배치 {batch_idx // bcc_batch_size + 1} 전송 실패: {len(current_batch)}개 이메일"
                )

            # 배치 간 잠시 대기 (너무 빠른 발송은 스팸으로 분류될 수 있음)
            if batch_idx + bcc_batch_size < len(email_details) and not _terminate:
                logger.info(
                    f"다음 배치로 넘어가기 전에 {config.EMAIL_BETWEEN_DELAY}초 대기..."
                )
                time.sleep(config.EMAIL_BETWEEN_DELAY)

            # 진행률 표시
            _total_count = batch_idx + len(current_batch)
            completion = (
                (_total_count / len(email_details)) * 100 if email_details else 100
            )
            logger.info(
                f"진행 상황: {_total_count}/{len(email_details)} 이메일 처리됨 ({completion:.1f}%)"
            )
            logger.info(
                f"전송: {_sent_count}, 에러: {_error_count}, "
                f"이메일 없음: {_no_email_count}, 이미 전송됨: {_already_sent_count}"
            )

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
        "--db",
        type=str,
        default=DB_FILENAME,
        help=f"데이터베이스 파일 (기본값: {DB_FILENAME})",
    )
    parser.add_argument(
        "--batch-size", type=int, default=100, help="URL 배치 크기 (기본값: 100)"
    )
    parser.add_argument(
        "--bcc-size",
        type=int,
        default=config.EMAIL_BCC_BATCH_SIZE,
        help=f"BCC 배치 크기 (기본값: {config.EMAIL_BCC_BATCH_SIZE})",
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

    # BCC 크기 설정
    if args.bcc_size and args.bcc_size != config.EMAIL_BCC_BATCH_SIZE:
        config.EMAIL_BCC_BATCH_SIZE = args.bcc_size
        logger.info(f"BCC 배치 크기를 {config.EMAIL_BCC_BATCH_SIZE}로 설정했습니다.")

    logger.info("이미 성공적으로 전송된 이메일은 항상 건너뛰는 모드로 실행합니다.")
    logger.info(
        f"제외 대상 상태 코드: SENT({config.EMAIL_STATUS['SENT']}), ALREADY_SENT({config.EMAIL_STATUS['ALREADY_SENT']})"
    )

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

    # BCC로 테스트 이메일 전송
    if len(email_addresses) > 1:
        # 여러 이메일 주소가 있는 경우 BCC로 한 번에 전송
        logger.info(
            f"BCC 방식으로 {len(email_addresses)}개의 테스트 이메일을 한 번에 전송합니다."
        )
        success, sent_emails = send_bcc_batch_email(
            email_addresses, subject, html_content
        )
        if success:
            logger.info(f"테스트 이메일 BCC 전송 성공: {len(sent_emails)}개 이메일")
        else:
            logger.error("테스트 이메일 BCC 전송 실패")
    else:
        # 단일 이메일 주소인 경우 일반 방식으로 전송
        logger.info(
            f"단일 이메일 주소 {email_addresses[0]}로 테스트 이메일을 전송합니다."
        )

        # 사용자 정의 내용으로 이메일 전송
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
            msg["To"] = email_addresses[0]
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
            match config.EMAIL_SMTP_PORT:
                case 465:
                    with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
                        server.login(sender_email, password)
                        # BCC 필드의 주소들로 메일 전송 (From 주소는 발신자, To 주소도 발신자로 설정)
                        server.sendmail(
                            sender_email, email_addresses[0], msg.as_string()
                        )
                case 587:
                    with smtplib.SMTP(smtp_server, smtp_port) as server:
                        server.starttls()  # TLS 보안 처리
                        server.login(sender_email, password)
                        # BCC 필드의 주소들로 메일 전송 (From 주소는 발신자, To 주소도 발신자로 설정)
                        server.sendmail(
                            sender_email, email_addresses[0], msg.as_string()
                        )

            logger.info(f"{email_addresses[0]}로 테스트 이메일 전송 성공")

        except Exception as e:
            logger.error(f"{email_addresses[0]}에게 이메일 전송 중 오류 발생: {e}")


if __name__ == "__main__":
    main()
