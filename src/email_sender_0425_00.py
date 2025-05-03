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
        # 시작 시간 기록
        start_time = datetime.now()
        logger.info(f"개인화된 이메일 전송 작업 시작: {start_time}")
        
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
                AND (email_status = ? OR email_status = ?)
                """,
                (config.EMAIL_STATUS["SENT"], config.EMAIL_STATUS["ALREADY_SENT"])
            )
            row = cursor.fetchone()
            already_sent_count = row["total"] if row else 0
            self.already_sent_count = already_sent_count

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
            
            # SMTP 서버 연결
            if not self.connect():
                logger.error("SMTP 서버 연결 실패로 이메일을 전송할 수 없습니다.")
                return (0, 0, len(email_details))

            # 이메일 발송 시작 (tqdm 적용)
            logger.info(f"총 {len(email_details)}개의 개인화된 이메일을 전송합니다 (연결 재사용)...")
            url_status_map = {}  # 배치 업데이트용
            
            for i, detail in enumerate(tqdm(email_details, desc="Sending Emails", unit="email"), 1):
                if self.terminate_requested:
                    logger.info("종료 요청으로 인해 남은 이메일 처리를 중단합니다.")
                    break

                url = detail["url"]
                email = detail["email"]
                title = detail.get("title", "N/A")  # title이 없을 경우 대비

                # 변수 딕셔너리 구성
                variables = {
                    "TITLE": title,
                    "URL": url,
                    "KEYWORD": detail.get("keyword", ""),
                    "PHONE": detail.get("phone_number", ""),
                    "DATE": detail.get("crawled_date", "")
                }

                try:
                    # 개인화된 이메일 전송
                    success = self._send_single_email(
                        recipient_email=email,
                        variables=variables
                    )

                    # 상태 업데이트 (배치 처리를 위해 맵에 추가)
                    if success:
                        url_status_map[url] = config.EMAIL_STATUS["SENT"]
                        with self._lock:
                            self.sent_count += 1
                        logger.info(f"이메일 전송 성공 ({i}/{len(email_details)}): {email}")
                    else:
                        url_status_map[url] = config.EMAIL_STATUS["ERROR"]
                        with self._lock:
                            self.error_count += 1
                        # 로깅은 _send_single_email에서 수행됨

                    # 다음 이메일 전송 전에 지연
                    if i < len(email_details) and not self.terminate_requested:
                        time.sleep(config.EMAIL_SEND_DELAY_SECONDS)
                        
                    # 주기적 배치 업데이트 (예: 50개마다)
                    if len(url_status_map) >= 50:
                        self.update_batch_email_status(conn, url_status_map)
                        url_status_map = {}  # 맵 초기화

                except Exception as e:
                    logger.error(f"URL {url} ({email}) 처리 중 예기치 않은 오류 발생: {e}", exc_info=True)
                    url_status_map[url] = config.EMAIL_STATUS["ERROR"]
                    with self._lock:
                        self.error_count += 1

            # 남은 상태 업데이트 처리
            if url_status_map:
                self.update_batch_email_status(conn, url_status_map)

            # 종료 시간 및 통계 출력
            end_time = datetime.now()
            elapsed = end_time - start_time
            logger.info(f"이메일 전송 작업 완료: {end_time} (소요 시간: {elapsed})")
            logger.info(f"총 시도: {len(email_details)}, 전송 성공: {self.sent_count}, 오류: {self.error_count}")
            
            return (self.sent_count, self.error_count, len(email_details))

        except Exception as e:
            logger.error(f"이메일 전송 작업 중 주요 오류 발생: {e}", exc_info=True)
            return (self.sent_count, self.error_count, self.total_count)
            
        finally:
            # SMTP 연결 종료
            self.disconnect()
            
            # 데이터베이스 연결 종료
            if conn:
                conn.close()
                logger.info("Database connection closed.")

    def send_test_batch(self,
                       test_emails: List[str],
                       test_titles: List[str] = None,
                       subject: str = None,
                       html_template: str = None,
                       text_template: str = None) -> Tuple[int, int]:
        """
        테스트 이메일을 여러 수신자에게 일괄 전송합니다.
        
        Args:
            test_emails: 테스트 이메일 주소 목록
            test_titles: 테스트 제목 목록 (None인 경우 기본값 자동 생성)
            subject: 이메일 제목 템플릿 (None인 경우 self.subject 사용)
            html_template: HTML 템플릿 내용 (None인 경우 self.html_template 사용)
            text_template: 텍스트 템플릿 내용 (None인 경우 self.text_template 사용)
            
        Returns:
            (성공 수, 실패 수) 튜플
        """
        if not test_emails:
            logger.error("테스트 이메일 주소가 지정되지 않았습니다.")
            return (0, 0)
            
        # 제목 목록 준비
        if not test_titles or len(test_titles) < len(test_emails):
            if not test_titles:
                test_titles = []
            # 제목이 부족하면 기본 제목 추가
            default_title_start_index = len(test_titles) + 1
            test_titles.extend([f"테스트 제목 {i}" for i in range(default_title_start_index, len(test_emails) + 1)])
            
        # 템플릿 및 제목 설정
        subject_template = subject or self.subject
        html_content = html_template or self.html_template
        text_content = text_template or self.text_template
        
        logger.info(f"테스트 모드: {len(test_emails)}개의 이메일 주소로 개인화된 메일을 전송합니다.")
        
        # 카운터 초기화
        sent_count = 0
        error_count = 0
        
        # SMTP 서버 연결
        if not self.connect():
            logger.error("SMTP 서버 연결 실패로 테스트 이메일을 전송할 수 없습니다.")
            return (0, 0)
            
        try:
            # 이메일 발송 루프
            with tqdm(zip(test_emails, test_titles), total=len(test_emails), desc="Sending Test Emails", unit="email") as pbar:
                for i, (email, title) in enumerate(pbar, 1):
                    if self.terminate_requested:
                        logger.info("종료 요청으로 인해 남은 테스트 이메일 처리를 중단합니다.")
                        break
                        
                    # 초기 상태 표시 (현재 처리 중인 이메일)
                    pbar.set_postfix(email=email, status='Sending...')
                    
                    # 변수 딕셔너리 구성 (테스트용)
                    variables = {
                        "TITLE": title,
                        "URL": "https://example.com/test",
                        "KEYWORD": "테스트 키워드",
                        "PHONE": "010-1234-5678",
                        "DATE": datetime.now().strftime("%Y-%m-%d")
                    }
                    
                    # 이메일 발송
                    success = self._send_single_email(
                        recipient_email=email,
                        variables=variables,
                        subject=subject_template,
                        html_template=html_content,
                        text_template=text_content
                    )
                    
                    # 결과에 따라 후행 텍스트 업데이트
                    if success:
                        sent_count += 1
                        logger.info(f"테스트 이메일 {i}/{len(test_emails)} 전송 성공: {email} (제목: {title})")
                        pbar.set_postfix(email=email, status='Success ✅')
                    else:
                        error_count += 1
                        logger.error(f"테스트 이메일 {i}/{len(test_emails)} 전송 실패: {email} (제목: {title})")
                        pbar.set_postfix(email=email, status='Failed ❌')
                    
                    # 다음 이메일 전송 전에 지연
                    if i < len(test_emails) and not self.terminate_requested:
                        time.sleep(config.EMAIL_SEND_DELAY_SECONDS)
                        
            logger.info(f"테스트 이메일 전송 완료. 성공: {sent_count}, 실패: {error_count}")
            
            # 전체 카운터 업데이트
            with self._lock:
                self.sent_count += sent_count
                self.error_count += error_count
                self.total_count += len(test_emails)
                
            return (sent_count, error_count)
            
        except Exception as e:
            logger.error(f"테스트 이메일 전송 중 오류 발생: {e}", exc_info=True)
            return (sent_count, error_count)
            
        finally:
            # SMTP 연결 종료
            self.disconnect()


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
        subject: 이메일 제목 (None인 경우 config에서 가져옴)
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
    _email_sender.send_test_batch(
        test_emails=test_emails,
        test_titles=test_titles
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
        html_template=html_content,
        text_template=text_content,
        db_filename=args.db
    )

    # 테스트 이메일 전송 모드
    if args.test_emails:
        test_emails = [email.strip() for email in args.test_emails.split(",")]
        test_titles = None
        if args.test_titles:
            test_titles = [title.strip() for title in args.test_titles.split(",")]
        
        sent_count, error_count = _email_sender.send_test_batch(
            test_emails=test_emails,
            test_titles=test_titles
        )
        
        logger.info(f"테스트 이메일 발송 결과: 성공 {sent_count}, 실패 {error_count}")
        return

    # 필터 설정
    email_filter = {}
    if args.include:
        email_filter["include"] = args.include
    if args.exclude:
        email_filter["exclude"] = args.exclude
        
    # DB 발송 모드 (주 기능)
    # 이메일 발송 전 확인 단계 처리
    if not args.skip_confirm:
        # EmailSender 인스턴스를 생성하여 요약 정보만 가져옴
        temp_sender = EmailSender(db_filename=args.db)
        conn = get_db_connection(args.db)
        
        try:
            # 필요한 DB 쿼리와 요약 정보 생성
            # (실제 이메일을 보내지 않고 요약 정보만 생성)
            
            # 이미 전송된 이메일 카운트
            cursor = conn.cursor()
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
            if args.date:
                base_query += " AND crawled_date >= ?"
                params.append(args.date)
                
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
                
            # 요약 정보 생성 및 표시
            summary_text, _ = temp_sender.display_email_summary(email_details, already_sent_count)
            print(summary_text)
            
            # 사용자 확인 요청
            confirm = input("\n위 정보로 개인화된 이메일을 발송하시겠습니까? (y/n): ")
            if confirm.lower() not in ("y", "yes"):
                logger.info("사용자가 이메일 발송을 취소했습니다. 프로그램을 종료합니다.")
                return
                
        finally:
            if conn:
                conn.close()

    # 실제 이메일 발송 실행 
    sent_count, error_count, total_count = _email_sender.send_batch_from_db(
        min_date=args.date,
        email_filter=email_filter if email_filter else None,
        skip_confirm=True  # 이미 위에서 확인했으므로 중복 확인 방지
    )
    
    logger.info(f"이메일 발송 결과: 총 대상 {total_count}, 성공 {sent_count}, 실패 {error_count}")


if __name__ == "__main__":
    main()
