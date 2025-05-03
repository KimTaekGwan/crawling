"""
Module for sending personalized emails through Naver Mail.

이 모듈은 네이버 메일을 통해 개인화된 이메일을 전송하는 기능을 제공합니다.
데이터베이스에 저장된 URL 정보에서 이메일 주소를 추출하여 개인화된 이메일을 전송합니다.
이메일 내 {{TITLE}}과 같은 변수는 각 사용자의 값으로 대체됩니다.

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
./naver-email --test-emails "recipient1@example.com,recipient2@example.com" --test-titles "제목1,제목2"
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
import re
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Set, Tuple, Optional, Any

# tqdm import 추가
from tqdm import tqdm

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


def replace_template_variables(template: str, variables: Dict[str, str]) -> str:
    """
    템플릿 내의 변수를 실제 값으로 치환합니다.
    
    Args:
        template: 템플릿 문자열 (예: "안녕하세요, {{TITLE}}님")
        variables: 변수와 값의 딕셔너리 (예: {"TITLE": "홍길동"})
        
    Returns:
        치환된 문자열
    """
    result = template
    for key, value in variables.items():
        pattern = r'{{[\s]*' + key + r'[\s]*}}'
        result = re.sub(pattern, str(value) if value else "", result)
    return result


def _send_email_with_connection(
    server: smtplib.SMTP,
    sender_email: str,
    recipient_email: str,
    subject: str,
    variables: Dict[str, str],
    html_template: Optional[str],
    text_template: Optional[str]
) -> bool:
    """
    Helper function to send a single email using an existing SMTP connection.
    (이 함수는 기존 SMTP 연결을 사용하여 단일 이메일을 보내는 도우미 함수입니다.)

    Args:
        server: 활성 SMTP 서버 객체 (smtplib.SMTP 또는 smtplib.SMTP_SSL)
        sender_email: 발신자 이메일 주소
        recipient_email: 수신자 이메일 주소
        subject: 이메일 제목 템플릿
        variables: 템플릿 변수 딕셔너리
        html_template: HTML 이메일 템플릿 문자열
        text_template: 텍스트 이메일 템플릿 문자열

    Returns:
        성공 여부 (True/False)
    """
    try:
        # 변수 치환된 제목
        personalized_subject = replace_template_variables(subject, variables)

        # 메시지 생성
        msg = MIMEMultipart("alternative")
        msg["From"] = sender_email
        msg["To"] = recipient_email
        msg["Subject"] = personalized_subject

        # 텍스트 버전 추가
        if not text_template:
            logger.error(f"[{recipient_email}] 텍스트 이메일 내용이 비어 있습니다.")
            return False
        personalized_text = replace_template_variables(text_template, variables)
        text_part = MIMEText(personalized_text, "plain", "utf-8")
        msg.attach(text_part)

        # HTML 버전 추가
        if not html_template:
            logger.error(f"[{recipient_email}] HTML 이메일 내용이 비어 있습니다.")
            return False
        personalized_html = replace_template_variables(html_template, variables)
        html_part = MIMEText(personalized_html, "html", "utf-8")
        msg.attach(html_part)

        # 이메일 발송 (연결된 서버 사용)
        send_start_time = time.perf_counter()
        server.sendmail(sender_email, [recipient_email], msg.as_string())
        send_end_time = time.perf_counter()
        logger.debug(f"[{recipient_email}] Email sent via existing connection in {send_end_time - send_start_time:.4f} seconds.")
        return True

    except smtplib.SMTPRecipientsRefused as e:
        logger.error(f"[{recipient_email}] 수신자 주소 거부됨: {e}")
        return False
    except smtplib.SMTPDataError as e:
         logger.error(f"[{recipient_email}] 데이터 전송 오류: {e}")
         return False
    except smtplib.SMTPException as e:
        # Catch other SMTP specific errors during sendmail
        logger.error(f"[{recipient_email}] SMTP 전송 오류 발생: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"개인화된 이메일 ({recipient_email}) 전송 중 예기치 않은 오류 발생 (in helper): {e}", exc_info=True)
        return False


def send_personalized_email(
    recipient_email: str,
    subject: str,
    variables: Dict[str, str],
    html_template: str = None,
    text_template: str = None
) -> bool:
    """
    개인화된 이메일을 특정 수신자에게 전송합니다. (연결을 새로 생성하고 닫습니다.)

    Args:
        recipient_email: 수신자 이메일 주소
        subject: 이메일 제목
        variables: 변수와 값의 딕셔너리 (예: {"TITLE": "홍길동"})
        html_template: HTML 템플릿 (None인 경우 config에서 가져옴)
        text_template: 텍스트 템플릿 (None인 경우 config에서 가져옴)

    Returns:
        성공 여부 (True/False)
    """
    if not recipient_email:
        logger.warning("수신자 이메일 주소가 비어 있습니다.")
        return False

    # SMTP 서버 설정
    smtp_server_host = config.EMAIL_SMTP_SERVER
    smtp_port = config.EMAIL_SMTP_PORT
    sender_email = config.EMAIL_SENDER
    password = config.EMAIL_PASSWORD
    use_ssl = config.EMAIL_SSL

    # 제목 설정
    if not subject:
        subject = config.EMAIL_SUBJECT

    # 템플릿 내용 로드 (None이면 config 사용)
    html_content = html_template if html_template else config.EMAIL_HTML_CONTENT
    text_content = text_template if text_template else config.EMAIL_TEXT_CONTENT

    if not text_content or not html_content:
        logger.error("이메일 템플릿 내용이 비어 있습니다. config 또는 인자를 확인해주세요.")
        return False

    server = None
    success = False
    try:
        # --- SMTP 연결 및 로그인 ---
        logger.debug(f"[{recipient_email}] Establishing new SMTP connection...")
        conn_start_time = time.perf_counter()
        if use_ssl:
            server = smtplib.SMTP_SSL(smtp_server_host, smtp_port, timeout=30)
        else:
            server = smtplib.SMTP(smtp_server_host, smtp_port, timeout=30)
            server.starttls()
        conn_end_time = time.perf_counter()
        logger.debug(f"[{recipient_email}] Connected in {conn_end_time - conn_start_time:.4f} seconds. Logging in...")

        login_start_time = time.perf_counter()
        server.login(sender_email, password)
        login_end_time = time.perf_counter()
        logger.debug(f"[{recipient_email}] Logged in successfully in {login_end_time - login_start_time:.4f} seconds. Sending email...")
        # -------------------------

        # --- 실제 이메일 발송 (헬퍼 함수 사용) ---
        success = _send_email_with_connection(
            server=server,
            sender_email=sender_email,
            recipient_email=recipient_email,
            subject=subject,
            variables=variables,
            html_template=html_content,
            text_template=text_content
        )
        # ------------------------------------

        if success:
            logger.info(f"개인화된 이메일(단일)을 {recipient_email}에게 성공적으로 전송했습니다.")
        else:
            # 오류 로깅은 _send_email_with_connection 내에서 처리됨
            logger.error(f"개인화된 이메일(단일)을 {recipient_email}에게 전송하는 데 실패했습니다.")

        return success

    except smtplib.SMTPConnectError as e:
        logger.error(f"[{recipient_email}] SMTP 서버 연결 실패: {e}", exc_info=True)
        return False
    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"[{recipient_email}] SMTP 인증 실패: {e}", exc_info=True)
        return False
    except smtplib.SMTPSenderRefused as e:
        logger.error(f"[{recipient_email}] 발신자 주소 거부됨: {e}", exc_info=True)
        return False
    # SMTPRecipientsRefused 및 SMTPDataError는 헬퍼 함수에서 처리
    except smtplib.SMTPException as e:
        # Catch other SMTP specific errors (connect/auth)
        logger.error(f"[{recipient_email}] SMTP 오류 발생: {e}", exc_info=True)
        return False
    except ConnectionResetError as e:
        logger.error(f"[{recipient_email}] 연결 초기화 오류: {e}", exc_info=True)
        return False
    except TimeoutError as e:
         logger.error(f"[{recipient_email}] 연결 시간 초과 오류: {e}", exc_info=True)
         return False
    except Exception as e:
        logger.error(f"개인화된 이메일(단일) ({recipient_email}) 처리 중 예기치 않은 오류 발생: {e}", exc_info=True)
        return False
    finally:
        # --- SMTP 연결 종료 ---
        if server:
            try:
                server.quit()
                logger.debug(f"[{recipient_email}] SMTP connection closed.")
            except Exception as e:
                logger.error(f"[{recipient_email}] SMTP 연결 종료 중 오류 발생: {e}")
        # --------------------


def signal_handler(sig, frame):
    """
    SIGINT, SIGTERM 시그널 핸들러입니다.
    Ctrl+C 또는 종료 요청 시 실행됩니다.
    """
    global _terminate
    logger.info("종료 신호를 받았습니다. 현재 작업을 완료 후 프로그램을 종료합니다.")
    _terminate = True


def display_email_summary(email_details: List[Dict[str, Any]], already_sent_count: int) -> bool:
    """
    이메일 발송 요약 정보를 표시하고 사용자 확인을 요청합니다.

    Args:
        email_details: 이메일 상세 정보 목록
        already_sent_count: 이미 전송된 이메일 수

    Returns:
        사용자가 발송을 확인했는지 여부 (True/False)
    """
    # 발송 예정 이메일 수
    total_emails_to_send = len(email_details)

    # 도메인별 통계 계산
    domain_counts = {}
    for detail in email_details:
        email = detail.get("email", "")
        if "@" in email:
            domain = email.split("@")[1]
            domain_counts[domain] = domain_counts.get(domain, 0) + 1

    # 발송 요약 정보 표시
    print("\n" + "=" * 60)
    print("📧 개인화 이메일 발송 요약 정보 (이미 전송된 항목 제외)")
    print("=" * 60)

    # 전체 처리 URL 수
    total_processed_urls = total_emails_to_send + already_sent_count
    print(f"전체 처리 대상 URL 수: {total_processed_urls}개")

    if already_sent_count > 0:
        print(f"이미 전송된 이메일(SENT/ALREADY_SENT): {already_sent_count}개 (발송 대상에서 제외됨)")

    print(f"실제 발송 예정 이메일 수: {total_emails_to_send}개")
    print(f"모든 이메일은 개별적으로 전송되며, 각 이메일 사이에 1초의 지연이 있습니다.")

    # 도메인별 통계
    print("\n📊 도메인별 발송 통계:")
    for domain, count in sorted(domain_counts.items(), key=lambda x: x[1], reverse=True):
        percent = (count / total_emails_to_send) * 100 if total_emails_to_send > 0 else 0
        print(f"  - {domain}: {count}개 ({percent:.1f}%)")

    # 이메일 샘플 표시 (처음 5개)
    if email_details:
        print("\n📋 발송 예정 이메일 샘플 (처음 5개):")
        for i, detail in enumerate(email_details[:5], 1):
            url = detail.get("url", "N/A")
            email = detail.get("email", "N/A")
            title = detail.get("title", "N/A")
            print(f"  {i}. {url} -> {email} (제목: {title})")

        # 마지막 5개 (중복되지 않는 경우에만)
        if len(email_details) > 10:
            print("\n  ...")
            print("\n📋 발송 예정 이메일 샘플 (마지막 5개):")
            for i, detail in enumerate(email_details[-5:], len(email_details) - 4):
                url = detail.get("url", "N/A")
                email = detail.get("email", "N/A")
                title = detail.get("title", "N/A")
                print(f"  {i}. {url} -> {email} (제목: {title})")

    print("\n" + "=" * 60)

    # 사용자 확인 요청
    confirm = input("\n위 정보로 개인화된 이메일을 발송하시겠습니까? (y/n): ")
    return confirm.lower() in ("y", "yes")


def send_personalized_emails_for_websites(
    db_filename: str = None, 
    min_date: str = None,
    email_filter: Dict = None
) -> None:
    """
    데이터베이스의 웹사이트 정보를 기반으로 개인화된 이메일을 전송합니다.
    SMTP 연결을 재사용하여 효율성을 높입니다.
    이미 성공적으로 전송된 이메일(email_status=1 또는 4)은 처리 대상에서 제외됩니다.

    Args:
        db_filename: 데이터베이스 파일 경로 (None인 경우 기본값 사용)
        min_date: 최소 크롤링 날짜 (None인 경우 모든 날짜 대상)
        email_filter: 이메일 필터링 조건 (None인 경우 모든 URL 대상)
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
    logger.info(f"개인화된 이메일 전송 작업 시작: {start_time}")

    # 데이터베이스 연결
    conn = get_db_connection(db_filename)
    server = None  # SMTP 서버 객체 초기화

    try:
        # websites 테이블에 필요한 컬럼 추가 (없는 경우)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(websites)")
        columns = [row["name"] for row in cursor.fetchall()]

        migrations = []
        if "email_status" not in columns:
            migrations.append("ALTER TABLE websites ADD COLUMN email_status INTEGER DEFAULT 0")

        if "email_date" not in columns:
            migrations.append("ALTER TABLE websites ADD COLUMN email_date TIMESTAMP")

        for migration in migrations:
            cursor.execute(migration)

        if migrations:
            conn.commit()
            logger.info("데이터베이스 스키마 마이그레이션 완료")

        # 이미 전송된 이메일 카운트
        cursor.execute(
            """
            SELECT COUNT(*) as total FROM websites 
            WHERE email IS NOT NULL AND email != '' 
            AND (email_status = ? OR email_status = ?)
            """,
            (config.EMAIL_STATUS["SENT"], config.EMAIL_STATUS["ALREADY_SENT"])
        )
        row = cursor.fetchone()
        already_sent_count = row["total"] if row else 0

        # 처리할 대상 쿼리 작성
        base_query = """
            SELECT url, keyword, title, phone_number, email, crawled_date
            FROM websites
            WHERE email IS NOT NULL AND email != ''
            AND (email_status IS NULL OR (email_status != ? AND email_status != ?))
        """
        params = [config.EMAIL_STATUS["SENT"], config.EMAIL_STATUS["ALREADY_SENT"]]
        
        # 날짜 필터 추가
        if min_date:
            base_query += " AND crawled_date >= ?"
            params.append(min_date)
            
        # 키워드 필터 추가
        if email_filter and "include" in email_filter:
            include_conditions = []
            for keyword in email_filter["include"]:
                include_conditions.append("url LIKE ?")
                params.append(f"%{keyword}%")
            if include_conditions:
                base_query += f" AND ({' OR '.join(include_conditions)})"
                
        if email_filter and "exclude" in email_filter:
            for keyword in email_filter["exclude"]:
                base_query += " AND url NOT LIKE ?"
                params.append(f"%{keyword}%")
        
        base_query += " ORDER BY url"
        
        # 쿼리 실행
        cursor.execute(base_query, params)
        rows = cursor.fetchall()
        
        # 이메일 상세 정보 준비
        email_details = []
        for row in rows:
            email_details.append({
                "url": row["url"],
                "email": row["email"],
                "title": row["title"],
                "keyword": row["keyword"],
                "phone_number": row["phone_number"],
                "crawled_date": row["crawled_date"]
            })
            
        if not email_details:
            logger.warning("처리할 이메일이 없습니다. 모든 이메일이 이미 성공적으로 전송되었거나 이메일 주소가 없습니다.")
            return
            
        logger.info(f"총 {len(email_details)}개의 이메일을 전송할 예정입니다.")
        
        # 발송 요약 정보 표시 및 사용자 확인
        if not display_email_summary(email_details, already_sent_count):
            logger.info("사용자가 이메일 발송을 취소했습니다. 프로그램을 종료합니다.")
            return
            
        logger.info("사용자 확인 완료. 개인화된 이메일 발송을 시작합니다.")
        
        # --- SMTP 연결 설정 (루프 시작 전) ---
        smtp_server_host = config.EMAIL_SMTP_SERVER
        smtp_port = config.EMAIL_SMTP_PORT
        sender_email = config.EMAIL_SENDER
        password = config.EMAIL_PASSWORD
        use_ssl = config.EMAIL_SSL

        try:
            logger.info("Connecting to SMTP server...")
            conn_start_time = time.perf_counter()
            if use_ssl:
                server = smtplib.SMTP_SSL(smtp_server_host, smtp_port, timeout=30)
            else:
                server = smtplib.SMTP(smtp_server_host, smtp_port, timeout=30)
                server.starttls()
            conn_end_time = time.perf_counter()
            logger.info(f"Connected in {conn_end_time - conn_start_time:.4f} seconds. Logging in...")

            login_start_time = time.perf_counter()
            server.login(sender_email, password)
            login_end_time = time.perf_counter()
            logger.info(f"Logged in successfully in {login_end_time - login_start_time:.4f} seconds.")

        except (smtplib.SMTPConnectError, smtplib.SMTPAuthenticationError, smtplib.SMTPException, TimeoutError) as e:
            logger.error(f"SMTP 서버 연결 또는 로그인 실패: {e}", exc_info=True)
            logger.error("이메일 발송을 진행할 수 없습니다. SMTP 설정을 확인하세요.")
            return # 함수 종료
        except Exception as e:
             logger.error(f"SMTP 설정 중 예기치 않은 오류 발생: {e}", exc_info=True)
             logger.error("이메일 발송을 진행할 수 없습니다.")
             return # 함수 종료
        # ------------------------------------

        # 이메일 발송 시작 (tqdm 적용)
        logger.info(f"총 {len(email_details)}개의 개인화된 이메일을 전송합니다 (연결 재사용)...")
        for i, detail in enumerate(tqdm(email_details, desc="Sending Emails", unit="email"), 1):
            if _terminate:
                logger.info("종료 요청으로 인해 남은 이메일 처리를 중단합니다.")
                break

            url = detail["url"]
            email = detail["email"]
            title = detail.get("title", "N/A") # title이 없을 경우 대비

            # 변수 딕셔너리 구성
            variables = {
                "TITLE": title,
                "URL": url,
                "KEYWORD": detail.get("keyword", ""),
                "PHONE": detail.get("phone_number", ""),
                "DATE": detail.get("crawled_date", "")
            }

            try:
                # 개인화된 이메일 전송 (헬퍼 함수 사용, 연결된 서버 전달)
                success = _send_email_with_connection(
                    server=server,
                    sender_email=sender_email,
                    recipient_email=email,
                    subject=config.EMAIL_SUBJECT,
                    variables=variables,
                    html_template=config.EMAIL_HTML_CONTENT,
                    text_template=config.EMAIL_TEXT_CONTENT
                )

                # 상태 업데이트
                if success:
                    update_email_status(conn, url, config.EMAIL_STATUS["SENT"])
                    with _counter_lock:
                        _sent_count += 1
                    logger.info(f"이메일 전송 성공 ({i}/{len(email_details)}): {email}")
                else:
                    # 실패 로깅은 _send_email_with_connection에서 처리됨
                    update_email_status(conn, url, config.EMAIL_STATUS["ERROR"])
                    with _counter_lock:
                        _error_count += 1
                    # 이미 헬퍼 함수에서 로그를 남기므로 여기서는 중복 로그 피함
                    # logger.error(f"이메일 전송 실패 ({i}/{len(email_details)}): {email}")

                # 다음 이메일 전송 전에 지연
                if i < len(email_details) and not _terminate:
                    time.sleep(config.EMAIL_SEND_DELAY_SECONDS) # 설정값 사용

            except Exception as e:
                # _send_email_with_connection 외부의 예외 (예: DB 업데이트 오류 등)
                logger.error(f"URL {url} ({email}) 처리 중 예기치 않은 오류 발생: {e}", exc_info=True)
                try:
                    update_email_status(conn, url, config.EMAIL_STATUS["ERROR"])
                except Exception as db_e:
                    logger.error(f"오류 상태 업데이트 중 추가 오류 발생 ({url}): {db_e}")
                with _counter_lock:
                    _error_count += 1

        # 종료 시간 및 통계 출력
        end_time = datetime.now()
        elapsed = end_time - start_time
        logger.info(f"이메일 전송 작업 완료: {end_time} (소요 시간: {elapsed})")
        # 최종 카운트 (전역 변수 사용)
        logger.info(f"총 시도: {len(email_details)}, 전송 성공: {_sent_count}, 오류: {_error_count}")
        # logger.info(f"참고: 이메일 없음: {_no_email_count}, 이미 전송됨: {_already_sent_count}") # 이 값들은 시작 시점에 계산됨

    except Exception as e:
        logger.error(f"이메일 전송 작업 중 주요 오류 발생: {e}", exc_info=True)
    finally:
        # --- SMTP 연결 종료 (루프 종료 후) ---
        if server:
            try:
                server.quit()
                logger.info("SMTP connection closed.")
            except Exception as e:
                logger.error(f"SMTP 연결 종료 중 오류 발생: {e}")
        # -----------------------------------

        # 데이터베이스 연결 종료
        if conn:
            conn.close()
            logger.info("Database connection closed.")


def send_test_personalized_emails(
    test_emails: List[str],
    test_titles: List[str] = None,
    subject: str = None,
    html_content: str = None,
    text_content: str = None
) -> None:
    """
    테스트 목적으로 여러 수신자에게 개인화된 이메일을 전송합니다.
    (개선: SMTP 연결을 재사용하고, 공통 헬퍼 함수를 사용합니다)

    Args:
        test_emails: 테스트 이메일 주소 목록
        test_titles: 테스트 제목 목록 (None인 경우 기본값 사용)
        subject: 이메일 제목 (None인 경우 config에서 가져옴)
        html_content: HTML 내용 (None인 경우 config에서 가져옴)
        text_content: 텍스트 내용 (None인 경우 config에서 가져옴)
    """
    if not test_emails:
        logger.error("테스트 이메일 주소가 지정되지 않았습니다.")
        return

    # HTML 및 텍스트 내용 설정 (기존과 동일)
    if not html_content:
        html_content = config.EMAIL_HTML_CONTENT
    if not text_content:
        text_content = config.EMAIL_TEXT_CONTENT

    # 제목 설정 (기존과 동일)
    if not subject:
        subject = config.EMAIL_SUBJECT

    # 제목 목록 준비 (기존과 동일)
    if not test_titles or len(test_titles) < len(test_emails):
        if not test_titles:
            test_titles = []
        # 제목이 부족하면 기본 제목 추가
        default_title_start_index = len(test_titles) + 1
        test_titles.extend([f"테스트 제목 {i}" for i in range(default_title_start_index, len(test_emails) + 1)])


    logger.info(f"테스트 모드: {len(test_emails)}개의 이메일 주소로 개인화된 메일을 전송합니다 (연결 재사용).")

    # SMTP 서버 설정 가져오기
    smtp_server_host = config.EMAIL_SMTP_SERVER
    smtp_port = config.EMAIL_SMTP_PORT
    sender_email = config.EMAIL_SENDER
    password = config.EMAIL_PASSWORD
    use_ssl = config.EMAIL_SSL

    server = None  # 서버 객체 초기화
    try:
        # --- SMTP 연결 및 로그인 (한 번만 수행) ---
        logger.debug("Connecting to SMTP server for test emails...")
        conn_start_time = time.perf_counter()
        if use_ssl:
            server = smtplib.SMTP_SSL(smtp_server_host, smtp_port, timeout=30)
        else:
            server = smtplib.SMTP(smtp_server_host, smtp_port, timeout=30)
            server.starttls()
        conn_end_time = time.perf_counter()
        logger.debug(f"Connected in {conn_end_time - conn_start_time:.4f} seconds. Logging in...")

        login_start_time = time.perf_counter()
        server.login(sender_email, password)
        login_end_time = time.perf_counter()
        logger.debug(f"Logged in successfully in {login_end_time - login_start_time:.4f} seconds.")
        # ----------------------------------------

        sent_count = 0
        error_count = 0
        for i, (email, title) in enumerate(zip(test_emails, test_titles), 1):
            if _terminate: # 테스트 중에도 종료 신호 확인
                 logger.info("테스트 중단 신호 감지됨.")
                 break

            try:
                # 변수 딕셔너리 구성
                variables = {
                    "TITLE": title,
                    "URL": "https://example.com/test",
                    "KEYWORD": "테스트 키워드",
                    "PHONE": "010-1234-5678",
                    "DATE": datetime.now().strftime("%Y-%m-%d")
                }

                # --- 이메일 발송 (공통 헬퍼 함수 사용) ---
                success = _send_email_with_connection(
                    server=server,
                    sender_email=sender_email,
                    recipient_email=email,
                    subject=subject, # 인자로 받은 또는 config의 제목 사용
                    variables=variables,
                    html_template=html_content, # 인자 또는 config의 내용 사용
                    text_template=text_content  # 인자 또는 config의 내용 사용
                )
                # -----------------------------------

                if success:
                    logger.info(f"테스트 이메일 {i}/{len(test_emails)} 전송 성공: {email} (제목: {title})")
                    sent_count += 1
                else:
                    # 실패 로깅은 _send_email_with_connection 에서 처리
                    logger.error(f"테스트 이메일 {i}/{len(test_emails)} 전송 실패: {email} (제목: {title})")
                    error_count += 1

            except Exception as e:
                # _send_email_with_connection 외부의 예외
                logger.error(f"테스트 이메일 {i}/{len(test_emails)} ({email}) 처리 중 예기치 않은 오류: {e}", exc_info=True)
                error_count += 1
                # 심각한 오류 시 연결 재시도나 중단을 고려할 수 있으나, 여기서는 다음 메일 진행

            # 테스트 메일 간의 지연 (선택 사항)
            if i < len(test_emails) and not _terminate:
                time.sleep(config.EMAIL_SEND_DELAY_SECONDS) # 설정값 사용 (없으면 기본값 1초 등)

        logger.info(f"테스트 이메일 전송 완료. 성공: {sent_count}, 실패: {error_count}")

    except smtplib.SMTPConnectError as e:
        logger.error(f"테스트 이메일용 SMTP 서버 연결 실패: {e}", exc_info=True)
    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"테스트 이메일용 SMTP 인증 실패: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"테스트 이메일 처리 중 초기 설정 오류: {e}", exc_info=True)
    finally:
        # --- SMTP 연결 종료 ---
        if server:
            try:
                server.quit()
                logger.debug("SMTP connection closed for test emails.")
            except Exception as e:
                logger.error(f"SMTP 연결 종료 중 오류 발생: {e}")
        # --------------------


def main():
    """
    메인 함수: 커맨드 라인 인자 처리 및 이메일 전송 실행
    """
    import argparse

    # 템플릿 파일 확인
    if not hasattr(config, "HTML_TEMPLATE_EXISTS") or not hasattr(config, "TEXT_TEMPLATE_EXISTS"):
        logger.error("템플릿 파일 존재 여부를 확인할 수 없습니다.")
    else:
        if not config.HTML_TEMPLATE_EXISTS:
            logger.warning(f"HTML 템플릿 파일이 없습니다: {config.EMAIL_HTML_TEMPLATE_PATH}")
            logger.warning("기본 HTML 템플릿을 사용합니다.")

        if not config.TEXT_TEMPLATE_EXISTS:
            logger.warning(f"텍스트 템플릿 파일이 없습니다: {config.EMAIL_TEXT_TEMPLATE_PATH}")
            logger.warning("기본 텍스트 템플릿을 사용합니다.")

        if not config.HTML_TEMPLATE_EXISTS or not config.TEXT_TEMPLATE_EXISTS:
            logger.warning(f"템플릿 파일이 없습니다. 템플릿 파일을 생성하려면:")
            logger.warning(f"1. 디렉토리 확인: {config.TEMPLATES_DIR} 디렉토리가 존재하는지 확인")
            logger.warning(f"2. HTML 템플릿 파일 생성: {config.EMAIL_HTML_TEMPLATE_PATH}")
            logger.warning(f"3. 텍스트 템플릿 파일 생성: {config.EMAIL_TEXT_TEMPLATE_PATH}")

            # 사용자 확인 요청
            confirm = input("템플릿 파일이 없습니다. 기본 템플릿으로 계속 진행하시겠습니까? (y/n): ")
            if confirm.lower() not in ("y", "yes"):
                logger.info("사용자가 취소했습니다. 프로그램을 종료합니다.")
                sys.exit(0)

            logger.info("기본 템플릿으로 계속 진행합니다.")

    # 명령행 인자 파싱
    parser = argparse.ArgumentParser(description="네이버 메일을 통한 개인화된 이메일 전송 도구")
    parser.add_argument("--db", type=str, default=DB_FILENAME, help=f"데이터베이스 파일 (기본값: {DB_FILENAME})")
    parser.add_argument("--date", type=str, help="최소 크롤링 날짜 (YYYY-MM-DD 형식)")
    parser.add_argument("--include", type=str, nargs="+", help="포함할 키워드 목록 (URL 필터링)")
    parser.add_argument("--exclude", type=str, nargs="+", help="제외할 키워드 목록 (URL 필터링)")
    parser.add_argument("--log-level", type=str, choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO", help="로그 레벨 설정 (기본값: INFO)")
    
    # 테스트 관련 인자
    parser.add_argument("--test-emails", type=str, help="테스트 이메일 주소들 (쉼표로 구분)")
    parser.add_argument("--test-titles", type=str, help="테스트 제목들 (쉼표로 구분, 이메일 수와 일치해야 함)")
    parser.add_argument("--subject", type=str, help="이메일 제목 (테스트 이메일 전송 시 사용됩니다)")
    parser.add_argument("--html-content", type=str, help="테스트 이메일의 HTML 내용 (지정 시 기본값 대신 사용됩니다)")
    parser.add_argument("--text-content", type=str, help="테스트 이메일의 텍스트 내용 (지정 시 기본값 대신 사용됩니다)")
    parser.add_argument("--html-file", type=str, help="테스트 이메일의 HTML 내용이 있는 파일 경로 (지정 시 --html-content보다 우선됩니다)")
    parser.add_argument("--text-file", type=str, help="테스트 이메일의 텍스트 내용이 있는 파일 경로 (지정 시 --text-content보다 우선됩니다)")

    args = parser.parse_args()

    # 로그 레벨 설정
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    logger.info("이미 성공적으로 전송된 이메일은 항상 건너뛰는 모드로 실행합니다.")
    logger.info(f"제외 대상 상태 코드: SENT({config.EMAIL_STATUS['SENT']}), ALREADY_SENT({config.EMAIL_STATUS['ALREADY_SENT']})")

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

    # 테스트 이메일 전송 모드
    if args.test_emails:
        test_emails = [email.strip() for email in args.test_emails.split(",")]
        test_titles = None
        if args.test_titles:
            test_titles = [title.strip() for title in args.test_titles.split(",")]
        
        send_test_personalized_emails(
            test_emails=test_emails,
            test_titles=test_titles,
            subject=args.subject,
            html_content=html_content,
            text_content=text_content
        )
        return

    # 필터 설정
    email_filter = {}
    if args.include:
        email_filter["include"] = args.include
    if args.exclude:
        email_filter["exclude"] = args.exclude

    # 개인화된 이메일 전송 실행
    send_personalized_emails_for_websites(
        db_filename=args.db,
        min_date=args.date,
        email_filter=email_filter if email_filter else None
    )


if __name__ == "__main__":
    main()
