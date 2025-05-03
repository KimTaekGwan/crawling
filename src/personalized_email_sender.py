"""
Personalized Email Sender Script

This script sends personalized emails to recipients fetched from a database.
It uses the title field from the database to personalize the email content.
"""

import os
import time
import logging
import argparse
import sqlite3
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
from tqdm import tqdm

import src.config as config
from src.db_storage import get_db_connection
from src.email_sender import update_email_status, EMAIL_STATUS, send_bcc_batch_email

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 데이터베이스 파일명
DB_FILENAME = config.DEFAULT_DB_FILENAME


def send_single_personalized_email(
    recipient_email: str, subject: str, html_content: str, text_content: str, title: str = None
) -> bool:
    """
    단일 수신자에게 개인화된 이메일을 전송합니다.

    Args:
        recipient_email: 수신자 이메일 주소
        subject: 이메일 제목
        html_content: HTML 형식의 이메일 내용
        text_content: 텍스트 형식의 이메일 내용
        title: 개인화에 사용할 제목 (None인 경우 치환하지 않음)

    Returns:
        성공 여부 (True/False)
    """
    try:
        # SMTP 서버 설정
        smtp_server = config.EMAIL_SMTP_SERVER
        smtp_port = config.EMAIL_SMTP_PORT
        sender_email = config.EMAIL_SENDER
        password = config.EMAIL_PASSWORD

        # 개인화 처리 - {{TITLE}} 치환
        if title:
            personalized_html = html_content.replace("{{TITLE}}", title)
            personalized_text = text_content.replace("{{TITLE}}", title)
        else:
            personalized_html = html_content
            personalized_text = text_content

        # 메시지 생성
        msg = MIMEMultipart("alternative")
        msg["From"] = sender_email
        msg["To"] = recipient_email
        msg["Subject"] = subject

        # 텍스트 버전 추가
        text_part = MIMEText(personalized_text, "plain", "utf-8")
        msg.attach(text_part)

        # HTML 버전 추가
        html_part = MIMEText(personalized_html, "html", "utf-8")
        msg.attach(html_part)

        # SMTP 연결 및 메일 전송
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # TLS 보안 처리
            server.login(sender_email, password)
            server.sendmail(sender_email, recipient_email, msg.as_string())

        logger.debug(f"이메일이 {recipient_email}로 성공적으로 전송되었습니다.")
        return True

    except Exception as e:
        logger.error(f"이메일 전송 중 오류 발생: {e}")
        return False


def fetch_email_data(db_filename: str, min_date: str = None) -> List[Dict]:
    """
    데이터베이스에서 이메일 전송에 필요한 데이터를 가져옵니다.

    Args:
        db_filename: 데이터베이스 파일 경로
        min_date: 이 날짜 이후에 크롤링된 데이터만 가져옴 (YYYY-MM-DD 형식)

    Returns:
        이메일 데이터 목록 (url, keyword, title, email, phone_number 등)
    """
    conn = get_db_connection(db_filename)
    try:
        cursor = conn.cursor()
        
        query = """
        SELECT url, keyword, title, phone_number, email, crawled_date
        FROM websites
        WHERE email IS NOT NULL AND email != ''
        """
        
        params = []
        
        # 날짜 조건 추가
        if min_date:
            query += " AND crawled_date >= ?"
            params.append(min_date)
            
        # 이미 성공적으로 전송된 이메일은 제외
        query += " AND (email_status IS NULL OR (email_status != ? AND email_status != ?))"
        params.extend([config.EMAIL_STATUS["SENT"], config.EMAIL_STATUS["ALREADY_SENT"]])
        
        # 정렬
        query += " ORDER BY url"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # 결과를 딕셔너리 리스트로 변환
        email_data = []
        for row in rows:
            email_data.append({
                "url": row["url"],
                "keyword": row["keyword"],
                "title": row["title"],
                "phone_number": row["phone_number"],
                "email": row["email"],
                "crawled_date": row["crawled_date"]
            })
            
        logger.info(f"{len(email_data)}개의 이메일 데이터를 가져왔습니다.")
        return email_data
    
    except Exception as e:
        logger.error(f"데이터베이스에서 이메일 데이터 가져오기 실패: {e}")
        return []
    
    finally:
        if conn:
            conn.close()


def display_personalized_email_summary(
    email_data: List[Dict], sample_count: int = 5
) -> bool:
    """
    개인화된 이메일 발송 전 요약 정보를 표시하고 확인을 요청합니다.

    Args:
        email_data: 이메일 데이터 목록
        sample_count: 표시할 샘플 이메일 수

    Returns:
        사용자가 발송을 확인했는지 여부 (True/False)
    """
    # 발송 예정 이메일 수
    total_emails = len(email_data)
    
    if total_emails == 0:
        logger.warning("발송할 이메일이 없습니다.")
        return False
    
    # 도메인별 통계 계산
    domain_counts = {}
    for data in email_data:
        email = data.get("email", "")
        domain = email.split("@")[1] if "@" in email else "unknown"
        domain_counts[domain] = domain_counts.get(domain, 0) + 1
    
    # 발송 요약 정보 표시
    print("\n" + "=" * 60)
    print("📧 개인화된 이메일 발송 요약 정보")
    print("=" * 60)
    print(f"총 발송 예정 이메일 수: {total_emails}개")
    print(f"개인화 방식: {{TITLE}} 필드를 사용한 개인화")
    print(f"전송 간격: 1초")
    
    # 도메인별 통계
    print("\n📊 도메인별 발송 통계:")
    for domain, count in sorted(domain_counts.items(), key=lambda x: x[1], reverse=True):
        percent = (count / total_emails) * 100 if total_emails > 0 else 0
        print(f"  - {domain}: {count}개 ({percent:.1f}%)")
    
    # 샘플 데이터 표시
    if email_data:
        print("\n📋 발송 예정 이메일 샘플 (처음 5개):")
        for i, data in enumerate(email_data[:sample_count], 1):
            title = data.get("title", "(제목 없음)")
            email = data.get("email", "")
            print(f"  {i}. {email} - 제목: {title}")
        
        # 마지막 5개 (중복되지 않는 경우에만)
        if len(email_data) > 10:
            print("\n  ...")
            print(f"\n📋 발송 예정 이메일 샘플 (마지막 {sample_count}개):")
            for i, data in enumerate(email_data[-sample_count:], len(email_data) - sample_count + 1):
                title = data.get("title", "(제목 없음)")
                email = data.get("email", "")
                print(f"  {i}. {email} - 제목: {title}")
    
    print("\n" + "=" * 60)
    
    # 사용자 확인 요청
    confirm = input("\n위 정보로 개인화된 이메일을 발송하시겠습니까? (y/n): ")
    return confirm.lower() in ("y", "yes")


def load_from_text_file(file_path: str) -> List[Dict]:
    """
    텍스트 파일에서 테스트 이메일 주소를 읽어옵니다.
    형식: 이메일,제목(선택)

    Args:
        file_path: 텍스트 파일 경로

    Returns:
        이메일 데이터 목록 [{"email": "user@example.com", "title": "제목"}]
    """
    email_data = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                parts = line.split(',', 1)
                email = parts[0].strip()
                title = parts[1].strip() if len(parts) > 1 else None
                
                if '@' in email:  # 간단한 이메일 유효성 검사
                    email_data.append({
                        "email": email,
                        "title": title,
                        "url": f"test_{len(email_data)}",  # 임시 URL 생성
                    })
    except Exception as e:
        logger.error(f"텍스트 파일 읽기 실패: {e}")
    
    return email_data


def send_personalized_emails(
    db_filename: str = None,
    min_date: str = None,
    subject: str = None,
    html_content: str = None,
    text_content: str = None,
    test_file: str = None,
    test_mode: bool = False
) -> None:
    """
    데이터베이스에서 가져온 정보를 바탕으로 개인화된 이메일을 전송합니다.

    Args:
        db_filename: 데이터베이스 파일 경로 (None인 경우 기본값 사용)
        min_date: 이 날짜 이후에 크롤링된 데이터만 처리 (YYYY-MM-DD 형식)
        subject: 이메일 제목 (None인 경우 config에서 가져옴)
        html_content: HTML 형식의 이메일 내용 (None인 경우 config에서 가져옴)
        text_content: 텍스트 형식의 이메일 내용 (None인 경우 config에서 가져옴)
        test_file: 테스트 이메일 주소가 있는 파일 경로
        test_mode: 테스트 모드 여부
    """
    # 데이터베이스 파일명 설정
    if db_filename is None:
        db_filename = DB_FILENAME
    
    # 이메일 제목 설정
    if subject is None:
        subject = config.EMAIL_SUBJECT
    
    # HTML 및 텍스트 내용 설정
    if html_content is None:
        html_content = config.EMAIL_HTML_CONTENT
    
    if text_content is None:
        text_content = config.EMAIL_TEXT_CONTENT
    
    # 시작 시간 기록
    start_time = datetime.now()
    logger.info(f"개인화된 이메일 전송 작업 시작: {start_time}")
    
    # 이메일 데이터 가져오기
    email_data = []
    
    if test_file:
        # 테스트 파일에서 이메일 주소 읽기
        logger.info(f"테스트 파일 {test_file}에서 이메일 주소를 읽어옵니다.")
        email_data = load_from_text_file(test_file)
        logger.info(f"{len(email_data)}개의 테스트 이메일 주소를 가져왔습니다.")
    else:
        # 데이터베이스에서 이메일 데이터 가져오기
        email_data = fetch_email_data(db_filename, min_date)
    
    # 발송 요약 정보 표시 및 확인
    if not display_personalized_email_summary(email_data):
        logger.info("사용자가 이메일 발송을 취소했습니다. 프로그램을 종료합니다.")
        return
    
    # 데이터베이스 연결 (상태 업데이트용)
    conn = None
    if not test_mode:
        conn = get_db_connection(db_filename)
    
    # 전송 카운터 초기화
    sent_count = 0
    error_count = 0
    
    try:
        # 이메일 전송 시작
        logger.info("개인화된 이메일 전송을 시작합니다. (1초 간격으로 전송)")
        
        # TQDM으로 진행 상황 표시
        for idx, data in enumerate(tqdm(email_data, desc="이메일 발송 중")):
            email = data.get("email")
            title = data.get("title", "")
            url = data.get("url", "")
            
            # 50개마다 중간 점검
            if idx > 0 and idx % 50 == 0:
                logger.info(f"중간 점검: {idx}/{len(email_data)} 이메일 처리 완료")
                logger.info(f"성공: {sent_count}, 실패: {error_count}")
                
                # 사용자 확인 요청
                continue_sending = input("\n계속 진행하시겠습니까? (y/n): ")
                if continue_sending.lower() not in ("y", "yes"):
                    logger.info("사용자가 이메일 발송을 중단했습니다.")
                    break
            
            # 개인화된 이메일 전송
            success = send_single_personalized_email(
                recipient_email=email,
                subject=subject,
                html_content=html_content,
                text_content=text_content,
                title=title
            )
            
            # 상태 업데이트 및 카운터 증가
            if success:
                sent_count += 1
                if not test_mode and conn:
                    update_email_status(conn, url, config.EMAIL_STATUS["SENT"], commit=True)
                logger.debug(f"이메일 전송 성공: {email}")
            else:
                error_count += 1
                if not test_mode and conn:
                    update_email_status(conn, url, config.EMAIL_STATUS["ERROR"], commit=True)
                logger.error(f"이메일 전송 실패: {email}")
            
            # 1초 대기
            time.sleep(1)
        
        # 종료 시간 및 통계 출력
        end_time = datetime.now()
        elapsed = end_time - start_time
        logger.info(f"이메일 전송 작업 완료: {end_time} (소요 시간: {elapsed})")
        logger.info(f"총 처리: {len(email_data)}, 성공: {sent_count}, 실패: {error_count}")
    
    except Exception as e:
        logger.error(f"이메일 전송 작업 중 오류 발생: {e}")
    
    finally:
        # 데이터베이스 연결 종료
        if conn:
            conn.close()


def main():
    """
    메인 함수: 커맨드 라인 인자 처리 및 이메일 전송 실행
    """
    # 명령행 인자 파싱
    parser = argparse.ArgumentParser(
        description="개인화된 이메일 전송 도구"
    )
    parser.add_argument(
        "--db",
        type=str,
        default=DB_FILENAME,
        help=f"데이터베이스 파일 (기본값: {DB_FILENAME})",
    )
    parser.add_argument(
        "--min-date",
        type=str,
        help="이 날짜 이후에 크롤링된 데이터만 처리 (YYYY-MM-DD 형식)",
    )
    parser.add_argument(
        "--subject",
        type=str,
        help="이메일 제목 (지정하지 않으면 config에서 가져옴)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="로그 레벨 설정 (기본값: INFO)",
    )
    parser.add_argument(
        "--html-file",
        type=str,
        help="HTML 이메일 템플릿 파일 경로",
    )
    parser.add_argument(
        "--text-file",
        type=str,
        help="텍스트 이메일 템플릿 파일 경로",
    )
    parser.add_argument(
        "--test-file",
        type=str,
        help="테스트 이메일 주소가 있는 파일 경로 (CSV 형식: 이메일,제목)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="테스트 모드 실행 (데이터베이스 상태 업데이트 없음)",
    )

    args = parser.parse_args()

    # 로그 레벨 설정
    logging.getLogger().setLevel(getattr(logging, args.log_level))

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
    
    # 텍스트 파일에서 내용 읽기
    if args.text_file:
        try:
            with open(args.text_file, "r", encoding="utf-8") as f:
                text_content = f.read()
            logger.info(f"텍스트 내용을 파일 {args.text_file}에서 읽었습니다.")
        except Exception as e:
            logger.error(f"텍스트 파일 {args.text_file} 읽기 실패: {e}")

    # 개인화된 이메일 전송 실행
    send_personalized_emails(
        db_filename=args.db,
        min_date=args.min_date,
        subject=args.subject,
        html_content=html_content,
        text_content=text_content,
        test_file=args.test_file,
        test_mode=args.test
    )


if __name__ == "__main__":
    main() 