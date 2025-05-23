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
from typing import Dict, List, Set, Tuple, Optional, Any, Union

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

# 전송된 메일 개수 카운터 (클래스로 리팩토링하면서 점진적으로 제거 예정)
_sent_count = 0
_error_count = 0
_no_email_count = 0
_already_sent_count = 0
_total_count = 0

# 카운터 락
_counter_lock = threading.Lock()

# 종료 플래그
_terminate = False


class EmailSender:
    """
    이메일 발송을 담당하는 클래스입니다.
    SMTP 연결 관리, 이메일 템플릿 처리, 배치 전송 기능을 제공합니다.
    """
    
    def __init__(self, 
                 smtp_host: str = None, 
                 smtp_port: int = None, 
                 sender_email: str = None, 
                 password: str = None, 
                 use_ssl: bool = None,
                 subject: str = None,
                 html_template: str = None,
                 text_template: str = None,
                 db_filename: str = None):
        """
        EmailSender 클래스를 초기화합니다.
        
        Args:
            smtp_host: SMTP 서버 호스트 (기본값: config.EMAIL_SMTP_SERVER)
            smtp_port: SMTP 서버 포트 (기본값: config.EMAIL_SMTP_PORT)
            sender_email: 발신자 이메일 (기본값: config.EMAIL_SENDER)
            password: 이메일 계정 비밀번호 (기본값: config.EMAIL_PASSWORD)
            use_ssl: SSL 사용 여부 (기본값: config.EMAIL_SSL)
            subject: 이메일 제목 템플릿 (기본값: config.EMAIL_SUBJECT)
            html_template: HTML 템플릿 내용 (기본값: config.EMAIL_HTML_CONTENT)
            text_template: 텍스트 템플릿 내용 (기본값: config.EMAIL_TEXT_CONTENT)
            db_filename: 데이터베이스 파일명 (기본값: DB_FILENAME)
        """
        # SMTP 서버 설정
        self.smtp_host = smtp_host or config.EMAIL_SMTP_SERVER
        self.smtp_port = smtp_port or config.EMAIL_SMTP_PORT
        self.sender_email = sender_email or config.EMAIL_SENDER
        self.password = password or config.EMAIL_PASSWORD
        self.use_ssl = use_ssl if use_ssl is not None else config.EMAIL_SSL
        
        # 이메일 내용 설정
        self.subject = subject or config.EMAIL_SUBJECT
        self.html_template = html_template or config.EMAIL_HTML_CONTENT
        self.text_template = text_template or config.EMAIL_TEXT_CONTENT
        
        # 데이터베이스 설정
        self.db_filename = db_filename or DB_FILENAME
        
        # SMTP 서버 연결 객체
        self.server = None
        
        # 상태 추적
        self.sent_count = 0
        self.error_count = 0
        self.no_email_count = 0
        self.already_sent_count = 0
        self.total_count = 0
        
        # 종료 요청 플래그
        self.terminate_requested = False
        
        # 쓰레드 락
        self._lock = threading.Lock()
    
    def connect(self) -> bool:
        """
        SMTP 서버에 연결하고 로그인합니다.
        
        Returns:
            연결 성공 여부 (True/False)
        """
        if self.server:
            logger.warning("이미 SMTP 서버에 연결되어 있습니다.")
            return True
        
        try:
            logger.debug("SMTP 서버에 연결 중...")
            conn_start_time = time.perf_counter()
            
            if self.use_ssl:
                self.server = smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, timeout=30)
            else:
                self.server = smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30)
                self.server.starttls()
                
            conn_end_time = time.perf_counter()
            logger.debug(f"SMTP 서버 연결 완료 (소요 시간: {conn_end_time - conn_start_time:.4f}초). 로그인 중...")
            
            # 로그인
            login_start_time = time.perf_counter()
            self.server.login(self.sender_email, self.password)
            login_end_time = time.perf_counter()
            
            logger.debug(f"SMTP 로그인 완료 (소요 시간: {login_end_time - login_start_time:.4f}초).")
            return True
            
        except smtplib.SMTPConnectError as e:
            logger.error(f"SMTP 서버 연결 실패: {e}", exc_info=True)
            return False
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP 인증 실패: {e}", exc_info=True)
            return False
        except (smtplib.SMTPException, ConnectionResetError, TimeoutError) as e:
            logger.error(f"SMTP 연결 또는 로그인 중 오류 발생: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"SMTP 서버 연결 중 예기치 않은 오류 발생: {e}", exc_info=True)
            return False
    
    def disconnect(self) -> None:
        """
        SMTP 서버 연결을 종료합니다.
        """
        if not self.server:
            return
            
        try:
            self.server.quit()
            logger.debug("SMTP 서버 연결이 종료되었습니다.")
        except Exception as e:
            logger.error(f"SMTP 서버 연결 종료 중 오류 발생: {e}")
        finally:
            self.server = None
    
    def replace_template_variables(self, template: str, variables: Dict[str, str]) -> str:
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
    
    def _send_single_email(self, 
                          recipient_email: str,
                          variables: Dict[str, str],
                          subject: str = None,
                          html_template: str = None,
                          text_template: str = None) -> bool:
        """
        단일 이메일을 전송합니다. (이미 연결된 SMTP 서버 사용)
        
        Args:
            recipient_email: 수신자 이메일 주소
            variables: 템플릿 변수 딕셔너리
            subject: 이메일 제목 템플릿 (None인 경우 self.subject 사용)
            html_template: HTML 템플릿 내용 (None인 경우 self.html_template 사용)
            text_template: 텍스트 템플릿 내용 (None인 경우 self.text_template 사용)
            
        Returns:
            성공 여부 (True/False)
        """
        if not self.server:
            logger.error("SMTP 서버에 연결되어 있지 않습니다. connect() 메소드를 먼저 호출하세요.")
            return False
            
        if not recipient_email:
            logger.warning("수신자 이메일 주소가 비어 있습니다.")
            return False
            
        # 템플릿 및 제목 설정
        subject_template = subject or self.subject
        html_content = html_template or self.html_template
        text_content = text_template or self.text_template
        
        if not text_content or not html_content:
            logger.error(f"[{recipient_email}] 이메일 템플릿 내용이 비어 있습니다.")
            return False
            
        try:
            # 변수 치환된 제목
            personalized_subject = self.replace_template_variables(subject_template, variables)
            
            # 메시지 생성
            msg = MIMEMultipart("alternative")
            msg["From"] = self.sender_email
            msg["To"] = recipient_email
            msg["Subject"] = personalized_subject
            
            # 텍스트 버전 추가
            personalized_text = self.replace_template_variables(text_content, variables)
            text_part = MIMEText(personalized_text, "plain", "utf-8")
            msg.attach(text_part)
            
            # HTML 버전 추가
            personalized_html = self.replace_template_variables(html_content, variables)
            html_part = MIMEText(personalized_html, "html", "utf-8")
            msg.attach(html_part)
            
            # 이메일 발송
            send_start_time = time.perf_counter()
            self.server.sendmail(self.sender_email, [recipient_email], msg.as_string())
            send_end_time = time.perf_counter()
            
            logger.debug(f"[{recipient_email}] 이메일 전송 완료 (소요 시간: {send_end_time - send_start_time:.4f}초).")
            return True
            
        except smtplib.SMTPRecipientsRefused as e:
            logger.error(f"[{recipient_email}] 수신자 주소 거부됨: {e}")
            return False
        except smtplib.SMTPDataError as e:
            logger.error(f"[{recipient_email}] 데이터 전송 오류: {e}")
            return False
        except smtplib.SMTPException as e:
            logger.error(f"[{recipient_email}] SMTP 전송 오류 발생: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"[{recipient_email}] 이메일 전송 중 예기치 않은 오류 발생: {e}", exc_info=True)
            return False
    
    def send_single_email(self,
                         recipient_email: str,
                         variables: Dict[str, str],
                         subject: str = None,
                         html_template: str = None,
                         text_template: str = None) -> bool:
        """
        단일 이메일을 전송합니다. (새 SMTP 연결 생성)
        
        Args:
            recipient_email: 수신자 이메일 주소
            variables: 템플릿 변수 딕셔너리
            subject: 이메일 제목 템플릿 (None인 경우 self.subject 사용)
            html_template: HTML 템플릿 내용 (None인 경우 self.html_template 사용)
            text_template: 텍스트 템플릿 내용 (None인 경우 self.text_template 사용)
            
        Returns:
            성공 여부 (True/False)
        """
        # 서버 연결이 없으면 임시 연결 생성
        temp_connection = not self.server
        success = False
        
        try:
            if temp_connection:
                connect_success = self.connect()
                if not connect_success:
                    logger.error(f"[{recipient_email}] SMTP 서버 연결 실패로 이메일을 전송할 수 없습니다.")
                    return False
                    
            # 이메일 전송
            success = self._send_single_email(
                recipient_email=recipient_email,
                variables=variables,
                subject=subject,
                html_template=html_template,
                text_template=text_template
            )
            
            if success:
                with self._lock:
                    self.sent_count += 1
                logger.info(f"[{recipient_email}] 이메일 전송 성공.")
            else:
                with self._lock:
                    self.error_count += 1
                logger.error(f"[{recipient_email}] 이메일 전송 실패.")
                
            return success
            
        except Exception as e:
            logger.error(f"[{recipient_email}] 이메일 전송 프로세스 중 오류 발생: {e}", exc_info=True)
            with self._lock:
                self.error_count += 1
            return False
            
        finally:
            # 임시 연결이었다면 종료
            if temp_connection and self.server:
                self.disconnect()
    
    def update_email_status(self, 
                           conn: sqlite3.Connection, 
                           url: str, 
                           status: int, 
                           commit: bool = True) -> None:
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
            conn = get_db_connection(self.db_filename)

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
                
    def update_batch_email_status(self, 
                                 conn: sqlite3.Connection, 
                                 url_status_map: Dict[str, int], 
                                 commit: bool = True) -> int:
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
            conn = get_db_connection(self.db_filename)

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
        
    def display_email_summary(self, email_details: List[Dict[str, Any]], already_sent_count: int) -> Tuple[str, bool]:
        """
        이메일 발송 요약 정보를 생성하고 사용자 확인을 요청합니다.

        Args:
            email_details: 이메일 상세 정보 목록
            already_sent_count: 이미 전송된 이메일 수

        Returns:
            (요약 정보 문자열, 사용자가 발송을 확인했는지 여부) 튜플
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

        # 발송 요약 정보 생성
        summary_lines = []
        summary_lines.append("\n" + "=" * 60)
        summary_lines.append("📧 개인화 이메일 발송 요약 정보 (이미 전송된 항목 제외)")
        summary_lines.append("=" * 60)

        # 전체 처리 URL 수
        total_processed_urls = total_emails_to_send + already_sent_count
        summary_lines.append(f"전체 처리 대상 URL 수: {total_processed_urls}개")

        if already_sent_count > 0:
            summary_lines.append(f"이미 전송된 이메일(SENT/ALREADY_SENT): {already_sent_count}개 (발송 대상에서 제외됨)")

        summary_lines.append(f"실제 발송 예정 이메일 수: {total_emails_to_send}개")
        summary_lines.append(f"모든 이메일은 개별적으로 전송되며, 각 이메일 사이에 {config.EMAIL_SEND_DELAY_SECONDS}초의 지연이 있습니다.")

        # 도메인별 통계
        summary_lines.append("\n📊 도메인별 발송 통계:")
        for domain, count in sorted(domain_counts.items(), key=lambda x: x[1], reverse=True):
            percent = (count / total_emails_to_send) * 100 if total_emails_to_send > 0 else 0
            summary_lines.append(f"  - {domain}: {count}개 ({percent:.1f}%)")

        # 이메일 샘플 표시 (처음 5개)
        if email_details:
            summary_lines.append("\n📋 발송 예정 이메일 샘플 (처음 5개):")
            for i, detail in enumerate(email_details[:5], 1):
                url = detail.get("url", "N/A")
                email = detail.get("email", "N/A")
                title = detail.get("title", "N/A")
                summary_lines.append(f"  {i}. {url} -> {email} (제목: {title})")

            # 마지막 5개 (중복되지 않는 경우에만)
            if len(email_details) > 10:
                summary_lines.append("\n  ...")
                summary_lines.append("\n📋 발송 예정 이메일 샘플 (마지막 5개):")
                for i, detail in enumerate(email_details[-5:], len(email_details) - 4):
                    url = detail.get("url", "N/A")
                    email = detail.get("email", "N/A")
                    title = detail.get("title", "N/A")
                    summary_lines.append(f"  {i}. {url} -> {email} (제목: {title})")

        summary_lines.append("\n" + "=" * 60)
        
        # 전체 요약 정보 문자열 생성
        summary_text = "\n".join(summary_lines)
        
        # 표시 및 사용자 확인 요청 부분은 호출자가 담당
        return summary_text, True
    
    def send_batch_from_db(self,
                          min_date: str = None,
                          email_filter: Dict = None,
                          skip_confirm: bool = False) -> Tuple[int, int, int]:
        """
        데이터베이스의 웹사이트 정보를 기반으로 개인화된 이메일을 일괄 전송합니다.
        
        Args:
            min_date: 최소 크롤링 날짜 (None인 경우 모든 날짜 대상)
            email_filter: 이메일 필터링 조건 (None인 경우 모든 URL 대상)
            skip_confirm: 사용자 확인 단계 건너뛰기 여부 (기본값: False)
            
        Returns:
            (성공 수, 실패 수, 총 처리 URL 수) 튜플
        """
        start_time = datetime.now()
        logger.info(f"개인화된 이메일 전송 작업 시작: {start_time}")

        logger.info(f"제외 대상 상태 코드: SENT({config.EMAIL_STATUS['SENT']}), ALREADY_SENT({config.EMAIL_STATUS['ALREADY_SENT']}), ERROR({config.EMAIL_STATUS['ERROR']})")

        # 카운터 초기화
        self.sent_count = 0
        self.error_count = 0
        self.no_email_count = 0
        self.already_sent_count = 0
        self.terminate_requested = False
        
        # 데이터베이스 연결
        conn = get_db_connection(self.db_filename)
        
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
                AND (email_status = ? OR email_status = ? OR email_status = ?)
                """,
                (config.EMAIL_STATUS["SENT"], config.EMAIL_STATUS["ALREADY_SENT"], config.EMAIL_STATUS["ERROR"])
            )
            row = cursor.fetchone()
            already_sent_count = row["total"] if row else 0
            self.already_sent_count = already_sent_count

            # 처리할 대상 쿼리 작성
            base_query = """
                SELECT url, keyword, title, phone_number, email, crawled_date
                FROM websites
                WHERE email IS NOT NULL AND email != ''
                AND (email_status IS NULL OR (email_status != ? AND email_status != ? AND email_status != ?))
            """
            params = [config.EMAIL_STATUS["SENT"], config.EMAIL_STATUS["ALREADY_SENT"], config.EMAIL_STATUS["ERROR"]]
            
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
                return (0, 0, 0)
                
            logger.info(f"총 {len(email_details)}개의 이메일을 전송할 예정입니다.")
            
            # 발송 요약 정보 생성
            summary_text, _ = self.display_email_summary(email_details, already_sent_count)
            
            # 사용자 확인 과정 (호출자가 처리)
            if not skip_confirm:
                # 이 부분은 호출자가 표시하고 확인을 받아야 함
                # 여기서는 True로 가정함 (외부에서 처리하는 경우)
                # confirm = input("\n위 정보로 개인화된 이메일을 발송하시겠습니까? (y/n): ")
                # if confirm.lower() not in ("y", "yes"):
                #    logger.info("사용자가 이메일 발송을 취소했습니다. 프로그램을 종료합니다.")
                #    return (0, 0, len(email_details))
                pass
                
            logger.info("개인화된 이메일 발송을 시작합니다.")
            
            # 배치 처리를 위한 URL 상태 맵 (전역 변수)
            url_status_map = {}
            
            # 변수 추출 함수
            def get_variables_from_detail(detail, _):
                url = detail["url"]
                email = detail["email"]
                title = detail.get("title", "N/A")
                
                variables = {
                    "TITLE": title,
                    "URL": url,
                    "KEYWORD": detail.get("keyword", ""),
                    "PHONE": detail.get("phone_number", ""),
                    "DATE": detail.get("crawled_date", "")
                }
                
                return email, variables, {"url": url}
            
            # 성공 후처리 함수
            def on_success(_, __, extra_data):
                nonlocal url_status_map
                url = extra_data["url"]
                url_status_map[url] = config.EMAIL_STATUS["SENT"]
                
                # 주기적 배치 업데이트 (예: 50개마다)
                if len(url_status_map) >= 50:
                    self.update_batch_email_status(conn, url_status_map)
                    url_status_map.clear()
            
            # 오류 후처리 함수
            def on_error(_, __, extra_data, ___):
                nonlocal url_status_map
                url = extra_data["url"]
                url_status_map[url] = config.EMAIL_STATUS["ERROR"]
                
                # 주기적 배치 업데이트 (예: 50개마다)
                if len(url_status_map) >= 50:
                    self.update_batch_email_status(conn, url_status_map)
                    url_status_map.clear()
            
            # 내부 발송 메소드 호출
            sent_count, error_count = self._send_batch_internal(
                items=email_details,
                get_variables_func=get_variables_from_detail,
                on_success_func=on_success,
                on_error_func=on_error,
                description="Sending DB Emails"
            )
            
            # 남은 상태 업데이트 처리
            if url_status_map:
                self.update_batch_email_status(conn, url_status_map)
                
            # 종료 시간 및 통계 출력
            end_time = datetime.now()
            elapsed = end_time - start_time
            logger.info(f"이메일 전송 작업 완료: {end_time} (소요 시간: {elapsed})")
            logger.info(f"총 시도: {len(email_details)}, 전송 성공: {sent_count}, 오류: {error_count}")
            
            return (sent_count, error_count, len(email_details))

        except Exception as e:
            logger.error(f"이메일 전송 작업 중 주요 오류 발생: {e}", exc_info=True)
            return (self.sent_count, self.error_count, self.total_count)
            
        finally:
            # 데이터베이스 연결 종료
            if conn:
                conn.close()
                logger.info("Database connection closed.")

    def send_test_batch(self, 
                       recipients: List[str], 
                       test_variables: Dict,
                       subject_override: str = None,
                       html_template_override: str = None,
                       text_template_override: str = None,
                       items: List[Dict] = None,
                       get_variables_func: callable = None) -> Tuple[int, int]:
        """
        테스트 목적으로 동일한 내용의 이메일을 여러 수신자에게 전송합니다.
        
        Args:
            recipients: 테스트 이메일 수신자 목록
            test_variables: 이메일 템플릿에 사용할 테스트 변수
            subject_override: 이메일 제목 재정의 (None인 경우 기본값 사용)
            html_template_override: HTML 템플릿 재정의 (None인 경우 기본값 사용)
            text_template_override: 텍스트 템플릿 재정의 (None인 경우 기본값 사용)
            items: 개별 항목 처리를 위한 사용자 정의 목록 (None인 경우 recipients에서 생성)
            get_variables_func: 사용자 정의 변수 추출 함수 (None인 경우 기본 함수 사용)
            
        Returns:
            (성공 수, 실패 수) 튜플
        """
        # start_time = datetime.now()
        logger.info(f"테스트 이메일 발송 작업 시작: {datetime.now()}")

        logger.info(f"제외 대상 상태 코드: SENT({config.EMAIL_STATUS['SENT']}), ALREADY_SENT({config.EMAIL_STATUS['ALREADY_SENT']}), ERROR({config.EMAIL_STATUS['ERROR']})")

        # 카운터 초기화
        self.sent_count = 0
        self.error_count = 0
        
        # 빈 수신자 리스트 체크
        if not recipients:
            logger.warning("테스트 이메일 수신자가 지정되지 않았습니다.")
            return (0, 0)
            
        logger.info(f"테스트 이메일을 {len(recipients)}명의 수신자에게 전송합니다.")
        
        # 변수 사용 확인 및 변수 출력
        if not test_variables and not get_variables_func:
            logger.warning("테스트 변수가 지정되지 않았습니다. 기본 변수만 사용합니다.")
            test_variables = {}
            
        logger.debug(f"테스트 이메일에 사용되는 변수: {test_variables}")
        
        # 항목 목록 준비 (수신자 정보)
        if items is None:
            items = [{"email": email} for email in recipients]
        
        # 변수 추출 함수
        if get_variables_func is None:
            def get_variables_for_test(item, _):
                email = item["email"]
                return email, test_variables, {"email": email}
            get_variables_func = get_variables_for_test
        
        # 성공 처리 함수
        def on_success(_, __, ___):
            pass  # 테스트 발송에서는 별도 처리 필요 없음
        
        # 오류 처리 함수
        def on_error(_, __, ___, ____):
            pass  # 테스트 발송에서는 별도 처리 필요 없음
        
        # 주제 및 템플릿 설정
        subject = subject_override if subject_override else self.subject
        html_template = html_template_override if html_template_override else self.html_template
        text_template = text_template_override if text_template_override else self.text_template
        
        # 내부 발송 메소드 호출
        sent_count, error_count = self._send_batch_internal(
            items=items,
            get_variables_func=get_variables_func,
            on_success_func=on_success,
            on_error_func=on_error,
            subject=subject,
            html_template=html_template,
            text_template=text_template,
            description="Sending Test Emails"
        )
        
        # 종료 시간 및 통계 출력
        end_time = datetime.now()
        elapsed = end_time - start_time
        logger.info(f"테스트 이메일 전송 작업 완료: {end_time} (소요 시간: {elapsed})")
        logger.info(f"총 시도: {len(items)}, 전송 성공: {sent_count}, 오류: {error_count}")
        
        return (sent_count, error_count)

    def _send_batch_internal(self,
                           items: List[Any],
                           get_variables_func: callable,
                           on_success_func: callable = None,
                           on_error_func: callable = None,
                           subject: str = None,
                           html_template: str = None,
                           text_template: str = None,
                           description: str = "Sending Emails") -> Tuple[int, int]:
        """
        내부 헬퍼 메소드: 이메일 배치 발송 공통 로직을 처리합니다.
        
        Args:
            items: 처리할 항목 목록 (이메일 상세 정보 또는 (이메일, 제목) 튜플 등)
            get_variables_func: 각 항목에서 변수 딕셔너리를 추출하는 함수
                                signature: (item, index) -> (email, variables, extra_data)
            on_success_func: 성공 시 호출할 함수 (선택적)
                              signature: (email, variables, extra_data) -> None
            on_error_func: 오류 시 호출할 함수 (선택적)
                            signature: (email, variables, extra_data, exception) -> None
            subject: 이메일 제목 템플릿 (None인 경우 self.subject 사용)
            html_template: HTML 템플릿 내용 (None인 경우 self.html_template 사용)
            text_template: 텍스트 템플릿 내용 (None인 경우 self.text_template 사용)
            description: tqdm 진행 표시줄 설명
            
        Returns:
            (성공 수, 실패 수) 튜플
        """
        if not items:
            logger.warning("처리할 항목이 없습니다.")
            return (0, 0)
            
        # 템플릿 및 제목 설정
        subject_template = subject or self.subject
        html_content = html_template or self.html_template
        text_content = text_template or self.text_template
        
        # 로컬 카운터 (반환값용)
        sent_count = 0
        error_count = 0
        
        # 네이버 SMTP 서버 제한 대응을 위한 설정
        emails_per_connection = 25  # 한 연결당 처리할 이메일 수
        consecutive_errors = 0      # 연속 오류 발생 횟수
        max_consecutive_errors = 3  # 최대 연속 오류 허용 횟수
        
        try:
            # 이메일 발송 루프 (tqdm 적용)
            with tqdm(items, total=len(items), desc=description, unit="email") as pbar:
                for i, item in enumerate(pbar, 1):
                    if self.terminate_requested:
                        logger.info("종료 요청으로 인해 남은 이메일 처리를 중단합니다.")
                        break
                        
                    # 일정 개수마다 SMTP 서버 연결 초기화 
                    # (네이버 SMTP 서버 명령어 제한 대응)
                    if i % emails_per_connection == 1 or not self.server:
                        if self.server:
                            # 기존 연결 종료
                            try:
                                self.disconnect()
                                logger.debug(f"{emails_per_connection}개 이메일 처리 후 SMTP 연결 재설정")
                            except Exception as e:
                                logger.warning(f"SMTP 연결 종료 중 오류 (무시됨): {e}")
                        
                        # 새 연결 시도
                        connect_attempts = 0
                        while connect_attempts < 3:  # 최대 3번 시도
                            connect_success = self.connect()
                            if connect_success:
                                consecutive_errors = 0  # 연결 성공 시 오류 카운터 초기화
                                break
                            
                            connect_attempts += 1
                            if connect_attempts < 3:
                                logger.warning(f"SMTP 연결 실패 ({connect_attempts}/3), 5초 후 재시도...")
                                time.sleep(5)  # 연결 실패 시 5초 대기 후 재시도
                        
                        if not connect_success:
                            logger.error("SMTP 서버 연결에 3번 실패했습니다. 15분 대기 후 계속...")
                            time.sleep(900)  # 15분 대기
                            connect_success = self.connect()  # 마지막 시도
                            
                            if not connect_success:
                                logger.error("SMTP 서버 연결 재시도 실패. 이메일 발송을 중단합니다.")
                                break
                        
                    # 변수 추출 (이메일, 변수 딕셔너리, 추가 데이터)
                    try:
                        email, variables, extra_data = get_variables_func(item, i)
                    except Exception as e:
                        logger.error(f"항목 {i} 처리 중 변수 추출 오류: {e}", exc_info=True)
                        error_count += 1
                        continue
                        
                    # 현재 처리 정보 표시 (제목은 변수에서 추출)
                    title = variables.get("TITLE", "N/A")
                    pbar.set_postfix_str(f"(성공:{sent_count:02d}|실패:{error_count:02d}|전체:{len(items):02d}) email={email} title=\'{title}\'", refresh=False)
                    
                    # 이메일 발송
                    try:
                        success = self._send_single_email(
                            recipient_email=email,
                            variables=variables,
                            subject=subject_template,
                            html_template=html_content,
                            text_template=text_content
                        )
                        
                        if success:
                            sent_count += 1
                            consecutive_errors = 0  # 성공 시 연속 오류 카운터 초기화
                            # logger.info(f"이메일 전송 성공 ({i}/{len(items)}): {email}")
                            
                            # 성공 후처리 (제공된 경우)
                            if on_success_func:
                                try:
                                    on_success_func(email, variables, extra_data)
                                except Exception as e:
                                    logger.error(f"성공 후처리 중 오류 발생: {e}", exc_info=True)
                        else:
                            error_count += 1
                            consecutive_errors += 1
                            logger.error(f"이메일 전송 실패 ({i}/{len(items)}): {email}")
                            
                            # 실패 후처리 (제공된 경우)
                            if on_error_func:
                                try:
                                    on_error_func(email, variables, extra_data, None)
                                except Exception as e:
                                    logger.error(f"실패 후처리 중 오류 발생: {e}", exc_info=True)
                                    
                        # 진행 상황 표시 업데이트
                        pbar.set_postfix_str(f"(성공:{sent_count:02d}|실패:{error_count:02d}|전체:{len(items):02d}) email={email} title=\'{title}\'", refresh=True)
                        
                    except smtplib.SMTPSenderRefused as e:
                        error_count += 1
                        consecutive_errors += 1
                        logger.error(f"이메일 {email} 전송 중 발신자 거부 오류: {e}")
                        
                        # 발신자 거부는 속도 제한일 가능성 높음 - 연결 재설정
                        logger.info("SMTP 발신자 거부 오류 발생, 연결 재설정 및 60초 대기...")
                        try:
                            self.disconnect()
                        except Exception:
                            pass  # 연결 종료 오류는 무시
                            
                        time.sleep(60)  # 1분 대기
                        
                        # 예외 후처리 (제공된 경우)
                        if on_error_func:
                            try:
                                on_error_func(email, variables, extra_data, e)
                            except Exception as e2:
                                logger.error(f"예외 후처리 중 추가 오류 발생: {e2}", exc_info=True)
                    
                    except smtplib.SMTPServerDisconnected as e:
                        error_count += 1
                        consecutive_errors += 1
                        logger.error(f"이메일 {email} 전송 중 서버 연결 끊김: {e}")
                        
                        # 연결이 끊겼으므로 다음 반복에서 재연결 시도
                        self.server = None
                        
                        # 예외 후처리 (제공된 경우)
                        if on_error_func:
                            try:
                                on_error_func(email, variables, extra_data, e)
                            except Exception as e2:
                                logger.error(f"예외 후처리 중 추가 오류 발생: {e2}", exc_info=True)
                                
                    except Exception as e:
                        error_count += 1
                        consecutive_errors += 1
                        logger.error(f"이메일 {email} 전송 중 예외 발생: {e}", exc_info=True)
                        
                        # 예외 후처리 (제공된 경우)
                        if on_error_func:
                            try:
                                on_error_func(email, variables, extra_data, e)
                            except Exception as e2:
                                logger.error(f"예외 후처리 중 추가 오류 발생: {e2}", exc_info=True)
                                
                        # 오류 발생 시 진행 상황 업데이트
                        pbar.set_postfix_str(f"(성공:{sent_count:02d}|실패:{error_count:02d}|전체:{len(items):02d}) email={email} title=\'{title}\' Error!", refresh=True)
                    
                    # 연속 오류가 임계값 초과하면 대기 및 연결 재설정
                    if consecutive_errors >= max_consecutive_errors:
                        logger.warning(f"연속 {consecutive_errors}회 오류 발생, 2분 대기 후 연결 재설정...")
                        try:
                            self.disconnect()
                        except Exception:
                            pass  # 연결 종료 오류는 무시
                            
                        time.sleep(120)  # 2분 대기
                        consecutive_errors = 0  # 카운터 초기화
                        self.server = None  # 다음 반복에서 재연결 시도
                        
                    # 다음 이메일 전송 전에 지연
                    if i < len(items) and not self.terminate_requested:
                        # 성공한 경우는 정상 지연, 오류가 발생한 경우는 추가 지연
                        delay = config.EMAIL_SEND_DELAY_SECONDS
                        if consecutive_errors > 0:
                            delay = max(delay, 5)  # 최소 5초
                        time.sleep(delay)
                        
            # 루프 종료 후 최종 상태 표시
            final_postfix_str = f"(성공:{sent_count:02d}|실패:{error_count:02d}|전체:{len(items):02d}) Done."
            if 'pbar' in locals(): # pbar 객체가 생성되었는지 확인
                pbar.set_postfix_str(final_postfix_str, refresh=True)
            
            # 전체 카운터 업데이트
            with self._lock:
                self.sent_count += sent_count
                self.error_count += error_count
                self.total_count += len(items)
                
            return (sent_count, error_count)
            
        except Exception as e:
            logger.error(f"배치 이메일 처리 중 심각한 오류 발생: {e}", exc_info=True)
            return (sent_count, error_count)
            
        finally:
            # SMTP 연결 종료
            try:
                self.disconnect()
            except Exception as e:
                logger.error(f"SMTP 서버 연결 종료 중 오류 발생: {e}")
                self.server = None  # 서버 객체 초기화


def update_email_status(
    conn: sqlite3.Connection, url: str, status: int, commit: bool = True
) -> None:
    """
    이메일 전송 상태를 업데이트합니다. (레거시 함수)
    
    Args:
        conn: 데이터베이스 연결 객체
        url: 업데이트할 URL
        status: 새 상태 코드
        commit: 커밋 여부 (기본값: True)
    """
    # 임시 EmailSender 인스턴스 생성
    sender = EmailSender(db_filename=DB_FILENAME)
    sender.update_email_status(conn, url, status, commit)


def update_batch_email_status(
    conn: sqlite3.Connection, url_status_map: Dict[str, int], commit: bool = True
) -> int:
    """
    여러 URL의 이메일 전송 상태를 한 번에 업데이트합니다. (레거시 함수)

    Args:
        conn: 데이터베이스 연결 객체
        url_status_map: URL과 상태 코드의 매핑 딕셔너리
        commit: 커밋 여부 (기본값: True)

    Returns:
        업데이트된 레코드 수
    """
    # 임시 EmailSender 인스턴스 생성
    sender = EmailSender(db_filename=DB_FILENAME)
    return sender.update_batch_email_status(conn, url_status_map, commit)


def replace_template_variables(template: str, variables: Dict[str, str]) -> str:
    """
    템플릿 내의 변수를 실제 값으로 치환합니다. (레거시 함수)
    
    Args:
        template: 템플릿 문자열 (예: "안녕하세요, {{TITLE}}님")
        variables: 변수와 값의 딕셔너리 (예: {"TITLE": "홍길동"})
        
    Returns:
        치환된 문자열
    """
    # 임시 EmailSender 인스턴스 생성
    sender = EmailSender()
    return sender.replace_template_variables(template, variables)


def send_personalized_email(
    recipient_email: str,
    subject: str,
    variables: Dict[str, str],
    html_template: str = None,
    text_template: str = None
) -> bool:
    """
    개인화된 이메일을 특정 수신자에게 전송합니다. (레거시 함수)

    Args:
        recipient_email: 수신자 이메일 주소
        subject: 이메일 제목
        variables: 변수와 값의 딕셔너리 (예: {"TITLE": "홍길동"})
        html_template: HTML 템플릿 (None인 경우 config에서 가져옴)
        text_template: 텍스트 템플릿 (None인 경우 config에서 가져옴)

    Returns:
        성공 여부 (True/False)
    """
    # EmailSender 인스턴스 생성
    sender = EmailSender(
        subject=subject,
        html_template=html_template,
        text_template=text_template
    )
    
    # 단일 이메일 전송 (새 연결 생성)
    return sender.send_single_email(
        recipient_email=recipient_email,
        variables=variables
    )


def display_email_summary(email_details: List[Dict[str, Any]], already_sent_count: int) -> bool:
    """
    이메일 발송 요약 정보를 표시하고 사용자 확인을 요청합니다. (레거시 함수)

    Args:
        email_details: 이메일 상세 정보 목록
        already_sent_count: 이미 전송된 이메일 수

    Returns:
        사용자가 발송을 확인했는지 여부 (True/False)
    """
    # EmailSender 인스턴스 생성
    sender = EmailSender()
    
    # 요약 정보 생성
    summary_text, _ = sender.display_email_summary(email_details, already_sent_count)
    
    # 표시 및 사용자 확인
    print(summary_text)
    confirm = input("\n위 정보로 개인화된 이메일을 발송하시겠습니까? (y/n): ")
    return confirm.lower() in ("y", "yes")


def send_personalized_emails_for_websites(
    db_filename: str = None, 
    min_date: str = None,
    email_filter: Dict = None
) -> None:
    """
    데이터베이스의 웹사이트 정보를 기반으로 개인화된 이메일을 전송합니다. (레거시 함수)
    SMTP 연결을 재사용하여 효율성을 높입니다.
    이미 성공적으로 전송된 이메일(email_status=1 또는 4)은 처리 대상에서 제외됩니다.

    Args:
        db_filename: 데이터베이스 파일 경로 (None인 경우 기본값 사용)
        min_date: 최소 크롤링 날짜 (None인 경우 모든 날짜 대상)
        email_filter: 이메일 필터링 조건 (None인 경우 모든 URL 대상)
    """
    global _email_sender, _sent_count, _error_count, _total_count
    
    # 시그널 핸들러 등록 (레거시 방식)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # EmailSender 인스턴스 생성
    _email_sender = EmailSender(db_filename=db_filename)
    
    # DB 기반 이메일 발송 실행
    sent_count, error_count, total_count = _email_sender.send_batch_from_db(
        min_date=min_date,
        email_filter=email_filter
    )
    
    # 전역 변수 업데이트 (레거시 코드 호환성)
    _sent_count = sent_count
    _error_count = error_count
    _total_count = total_count


def send_test_personalized_emails(
    test_emails: List[str],
    test_titles: List[str] = None,
    subject: str = None,
    html_content: str = None,
    text_content: str = None
) -> None:
    """
    테스트 목적으로 여러 수신자에게 개인화된 이메일을 전송합니다. (레거시 함수)

    Args:
        test_emails: 테스트 이메일 주소 목록
        test_titles: 테스트 제목 목록 (None인 경우 기본값 사용)
        subject: 이메일 제목 (테스트 이메일 전송 시 사용됩니다)
        html_content: HTML 내용 (None인 경우 config에서 가져옴)
        text_content: 텍스트 내용 (None인 경우 config에서 가져옴)
    """
    global _email_sender
    
    # 시그널 핸들러 등록 (레거시 방식)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # EmailSender 인스턴스 생성
    _email_sender = EmailSender(
        subject=subject,
        html_template=html_content,
        text_template=text_content
    )
    
    # 테스트 이메일 배치 전송
    logger.info("사용자 확인 완료. 테스트 이메일 발송을 시작합니다.")
    
    # 각 이메일별로 다른 제목을 사용하기 위한 준비
    if test_titles and len(test_titles) > 0:
        # 이메일마다 다른 제목 적용을 위해 items 리스트 생성
        test_items = []
        for i, email in enumerate(test_emails):
            # 가능한 범위 내에서 제목 할당
            title = test_titles[i] if i < len(test_titles) else f"테스트 제목 {i+1}"
            test_items.append({
                "email": email,
                "title": title
            })
            
        # 다양한 제목을 사용하는 변수 추출 함수
        def get_variables_for_test_with_titles(item, _):
            email = item["email"]
            title = item["title"]
            
            # 기본 테스트 변수 복사 후 개별 제목 적용
            variables = {
                "TITLE": title,
                "URL": "https://example.com/test",
                "KEYWORD": "테스트 키워드",
                "PHONE": "010-1234-5678",
                "DATE": datetime.now().strftime("%Y-%m-%d")
            }
            
            return email, variables, {"email": email}
            
        # 개별 제목이 있는 항목으로 발송
        sent_count, error_count = _email_sender.send_test_batch(
            recipients=[item["email"] for item in test_items],
            test_variables={},  # 실제 변수는 get_variables_for_test_with_titles에서 생성
            subject_override=subject,
            html_template_override=html_content,
            text_template_override=text_content,
            items=test_items,
            get_variables_func=get_variables_for_test_with_titles
        )
    else:
        # 테스트 변수 딕셔너리 생성 (모든 이메일에 동일 변수 적용)
        test_variables = {
            "TITLE": "테스트 제목",
            "URL": "https://example.com/test",
            "KEYWORD": "테스트 키워드",
            "PHONE": "010-1234-5678",
            "DATE": datetime.now().strftime("%Y-%m-%d")
        }
        
        # 기본 방식으로 발송
        sent_count, error_count = _email_sender.send_test_batch(
            recipients=test_emails,
            test_variables=test_variables,
            subject_override=subject,
            html_template_override=html_content,
            text_template_override=text_content
        )


# 전역 이메일 발송기 인스턴스 (시그널 핸들러용)
_email_sender = None

def signal_handler(sig, frame):
    """
    SIGINT, SIGTERM 시그널 핸들러입니다.
    Ctrl+C 또는 종료 요청 시 실행됩니다.
    """
    global _email_sender
    logger.info("종료 신호를 받았습니다. 현재 작업을 완료 후 프로그램을 종료합니다.")
    
    if _email_sender:
        _email_sender.terminate_requested = True
    else:
        # 이전 버전 호환성 유지
        global _terminate
        _terminate = True


def main():
    """
    메인 함수: 커맨드 라인 인자 처리 및 이메일 전송 실행
    """
    global _email_sender
    
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
    parser.add_argument("--skip-confirm", action="store_true", help="이메일 발송 전 확인 단계 건너뛰기")
    
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
    start_time = datetime.now() 

    logger.info(f"제외 대상 상태 코드: SENT({config.EMAIL_STATUS['SENT']}), ALREADY_SENT({config.EMAIL_STATUS['ALREADY_SENT']}), ERROR({config.EMAIL_STATUS['ERROR']})")

    # 카운터 초기화
    _sent_count = 0
    _error_count = 0
    _no_email_count = 0
    _already_sent_count = 0
    _total_count = 0

    # 시그널 핸들러 등록 (Ctrl+C 및 종료 신호 처리)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
        
    # 이메일 발송기 생성
    _email_sender = EmailSender(
        smtp_host=config.EMAIL_SMTP_SERVER,
        smtp_port=config.EMAIL_SMTP_PORT,
        sender_email=config.EMAIL_SENDER,
        password=config.EMAIL_PASSWORD,
        use_ssl=config.EMAIL_SSL,
        subject=args.subject,
        html_template=args.html_content,
        text_template=args.text_content,
        db_filename=args.db
    )

    # 테스트 이메일 전송 모드
    if args.test_emails:
        test_emails = [email.strip() for email in args.test_emails.split(",")]
        test_titles = None
        if args.test_titles:
            test_titles = [title.strip() for title in args.test_titles.split(",")]
            
        # 제목 목록 준비 (test_batch 메소드 내 로직과 유사하게)
        if not test_titles or len(test_titles) < len(test_emails):
            if not test_titles:
                test_titles = []
            default_title_start_index = len(test_titles) + 1
            test_titles.extend([f"테스트 제목 {i}" for i in range(default_title_start_index, len(test_emails) + 1)])
        
        # 테스트용 email_details 생성
        test_email_details = []
        for email, title in zip(test_emails, test_titles):
            test_email_details.append({
                "url": f"test://{email}", # 가짜 URL
                "email": email,
                "title": title,
                "keyword": "테스트",
                "phone_number": "N/A",
                "crawled_date": "N/A"
            })
            
        # 요약 정보 표시 및 사용자 확인
        summary_text, _ = _email_sender.display_email_summary(test_email_details, 0) # already_sent_count는 0
        print(summary_text)
        
        confirm = input("\n위 정보로 테스트 이메일을 발송하시겠습니까? (y/n): ")
        if confirm.lower() not in ("y", "yes"):
            logger.info("사용자가 테스트 이메일 발송을 취소했습니다. 프로그램을 종료합니다.")
            return
        
        logger.info("사용자 확인 완료. 테스트 이메일 발송을 시작합니다.")
        
        # 각 이메일별로 다른 제목을 사용하기 위한 준비
        if test_titles and len(test_titles) > 0:
            # 이메일마다 다른 제목 적용을 위해 items 리스트 생성
            test_items = []
            for i, email in enumerate(test_emails):
                # 가능한 범위 내에서 제목 할당
                title = test_titles[i] if i < len(test_titles) else f"테스트 제목 {i+1}"
                test_items.append({
                    "email": email,
                    "title": title
                })
                
            # 다양한 제목을 사용하는 변수 추출 함수
            def get_variables_for_test_with_titles(item, _):
                email = item["email"]
                title = item["title"]
                
                # 기본 테스트 변수 복사 후 개별 제목 적용
                variables = {
                    "TITLE": title,
                    "URL": "https://example.com/test",
                    "KEYWORD": "테스트 키워드",
                    "PHONE": "010-1234-5678",
                    "DATE": datetime.now().strftime("%Y-%m-%d")
                }
                
                return email, variables, {"email": email}
                
            # 개별 제목이 있는 항목으로 발송
            sent_count, error_count = _email_sender.send_test_batch(
                recipients=[item["email"] for item in test_items],
                test_variables={},  # 실제 변수는 get_variables_for_test_with_titles에서 생성
                subject_override=args.subject,
                html_template_override=args.html_content,
                text_template_override=args.text_content,
                items=test_items,
                get_variables_func=get_variables_for_test_with_titles
            )
        else:
            # 테스트 변수 딕셔너리 생성 (모든 이메일에 동일 변수 적용)
            test_variables = {
                "TITLE": "테스트 제목",
                "URL": "https://example.com/test",
                "KEYWORD": "테스트 키워드",
                "PHONE": "010-1234-5678",
                "DATE": datetime.now().strftime("%Y-%m-%d")
            }
            
            # 기본 방식으로 발송
            sent_count, error_count = _email_sender.send_test_batch(
                recipients=test_emails,
                test_variables=test_variables,
                subject_override=args.subject,
                html_template_override=args.html_content,
                text_template_override=args.text_content
            )

    # 테스트 이메일 전송 모드가 종료되면 일반 배치 전송 모드로 전환
    else:
        # 일반 배치 전송 모드
        sent_count, error_count, total_count = _email_sender.send_batch_from_db(
            min_date=args.date,
            email_filter=dict(include=args.include, exclude=args.exclude) if args.include or args.exclude else None,
            skip_confirm=args.skip_confirm
        )

    # 종료 시간 및 통계 출력
    end_time = datetime.now()
    elapsed = end_time - start_time
    logger.info(f"이메일 전송 작업 완료: {end_time} (소요 시간: {elapsed})")
    logger.info(f"총 시도: {total_count}, 전송 성공: {sent_count}, 오류: {error_count}")


if __name__ == "__main__":
    main()
