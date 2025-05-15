# -*- coding: utf-8 -*-
"""
Module for updating the 'title' field in the database based on the og:site_name meta tag.
특정 날짜 기준으로 수집된 URL의 웹사이트 제목(og:site_name)을 추출하여 DB에 업데이트합니다.
"""

import sqlite3
import logging
import requests
from bs4 import BeautifulSoup
import concurrent.futures
import argparse
import time
import os # os 모듈 추가
from typing import List, Dict, Optional, Any

# 내부 모듈 임포트
import src.config as config
from src.db_storage import get_db_connection, initialize_db # db_storage에서 함수 임포트

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 기본 설정값
# DEFAULT_DB_FILENAME = "crawler_data.db" # config에서 가져오므로 주석 처리
DEFAULT_TARGET_DATE = "2025-02-16" # 이 날짜는 예시이며, 실제 필요에 맞게 변경해야 합니다.
DEFAULT_DATE_COLUMN = "crawled_date" # 실제 DB 스키마에 맞게 확인 필요 (db_storage의 스키마 참조)
DEFAULT_PARALLEL_COUNT = 4
REQUESTS_TIMEOUT = 15 # 웹사이트 요청 타임아웃 (초)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

def get_urls_to_update(conn: sqlite3.Connection, target_date: str, date_column: str) -> List[Dict[str, Any]]:
    """
    지정된 날짜 혹은 그 이후에 해당하는 URL 목록을 데이터베이스에서 가져옵니다.
    이미 title이 있는 URL은 제외합니다.
    날짜 형식은 YYYY-MM-DD 이거나 YYYY-MM-DD HH:MM:SS 형식일 수 있습니다.

    Args:
        conn: SQLite 데이터베이스 연결 객체
        target_date: 조회 시작 날짜 (YYYY-MM-DD 형식)
        date_column: 날짜 정보가 저장된 컬럼명

    Returns:
        URL 정보를 담은 딕셔너리 리스트 (예: [{'url': 'http://...'}])
    """
    urls_to_update = []
    try:
        cursor = conn.cursor()
        # 날짜 컬럼 타입 확인 (TEXT 또는 TIMESTAMP)
        # 여기서는 DATE 함수를 사용하여 YYYY-MM-DD 부분만 비교
        try:
            # DATE() 함수를 사용하여 날짜 비교 (크거나 같음)
            query = f"SELECT url FROM websites WHERE DATE({date_column}) >= DATE(?) AND (title IS NULL OR title = '')"
            cursor.execute(query, (target_date,))
            rows = cursor.fetchall()
            urls_to_update = [dict(row) for row in rows]
            logger.info(f"'{target_date}' 날짜 혹은 그 이후에 해당하는 업데이트 대상 URL {len(urls_to_update)}개를 찾았습니다.")
        except sqlite3.OperationalError as op_err:
            if f"no such column: {date_column}" in str(op_err):
                logger.error(f"오류: 데이터베이스에 '{date_column}' 컬럼이 없습니다. --date-column 인자를 확인하세요.")
            elif "no such function: DATE" in str(op_err):
                logger.warning(f"SQLite DATE 함수를 사용할 수 없습니다. 날짜 컬럼({date_column}) 형식을 확인하세요. 문자열 직접 비교를 시도합니다.")
                # DATE 함수 사용 불가 시, 문자열 직접 비교 시도 (>=)
                query = f"SELECT url FROM websites WHERE {date_column} >= ? AND (title IS NULL OR title = '')"
                cursor.execute(query, (target_date,))
                rows = cursor.fetchall()
                urls_to_update = [dict(row) for row in rows]
                logger.info(f"'{target_date}' 날짜 혹은 그 이후 (문자열 비교) 업데이트 대상 URL {len(urls_to_update)}개를 찾았습니다.")
            else:
                logger.error(f"URL 조회 중 데이터베이스 오류 발생: {op_err}")
            return [] # 빈 리스트 반환

    except sqlite3.Error as e:
        logger.error(f"URL 목록 조회 중 오류 발생: {e}")
    return urls_to_update

def update_title_in_db(conn: sqlite3.Connection, url: str, title: str) -> bool:
    """
    데이터베이스에 있는 특정 URL의 title을 업데이트합니다.

    Args:
        conn: SQLite 데이터베이스 연결 객체
        url: 업데이트할 URL
        title: 새로 저장할 제목

    Returns:
        업데이트 성공 여부 (True/False)
    """
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE websites SET title = ? WHERE url = ?", (title, url))
        conn.commit()
        logger.debug(f"DB 업데이트 성공: {url} -> '{title}'")
        return True
    except sqlite3.Error as e:
        logger.error(f"DB 업데이트 실패: {url} - {e}")
        try:
            conn.rollback()
        except sqlite3.Error as rb_err:
            logger.error(f"롤백 중 오류: {rb_err}")
        return False

def fetch_and_extract_og_title(url: str) -> Optional[str]:
    """
    주어진 URL에서 HTML을 가져와 og:site_name 메타 태그의 content를 추출합니다.
    429 오류 시 5초 후 1회 재시도합니다.
    404 오류 시 "NOT_FOUND"를 반환하여 삭제를 유도합니다.

    Args:
        url: 제목을 추출할 웹 페이지 URL

    Returns:
        추출된 제목 문자열, 404 오류 시 "NOT_FOUND", 그 외 실패 시 None
    """
    headers = {'User-Agent': USER_AGENT}
    max_retries = 1 # 429 오류 시 최대 재시도 횟수 (총 2번 시도)
    retry_delay = 5 # 재시도 간격 (초)

    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=REQUESTS_TIMEOUT, allow_redirects=True)
            response.raise_for_status() # HTTP 오류 발생 시 예외 발생

            # 성공 시 (2xx 상태 코드) HTML 파싱 및 제목 추출
            try:
                response.encoding = 'utf-8'
                html_content = response.text
            except UnicodeDecodeError:
                logger.warning(f"UTF-8 디코딩 실패, 자동 감지 사용: {url}")
                html_content = response.content.decode(response.apparent_encoding, errors='replace')

            soup = BeautifulSoup(html_content, 'html.parser')
            og_site_name_tag = soup.find('meta', property='og:site_name')
            if og_site_name_tag and og_site_name_tag.get('content'):
                title = og_site_name_tag['content'].strip()
                logger.debug(f"제목 추출 성공: {url} -> '{title}'")
                return title
            else:
                # 대체 제목 추출 시도 (og:title, title 태그)
                og_title_tag = soup.find('meta', property='og:title')
                if og_title_tag and og_title_tag.get('content'):
                    title = og_title_tag['content'].strip()
                    logger.debug(f"대체 추출 (og:title): {url} -> '{title}'")
                    return title
                title_tag = soup.find('title')
                if title_tag and title_tag.string:
                    title = title_tag.string.strip()
                    logger.debug(f"대체 추출 (title 태그): {url} -> '{title}'")
                    return title

                logger.debug(f"어떤 제목 정보도 찾을 수 없음: {url}")
                return None # 제목 태그를 찾지 못함

        except requests.exceptions.HTTPError as http_err:
            status_code = http_err.response.status_code
            logger.warning(f"HTTP 오류 발생 ({attempt + 1}차 시도): {url} - Status {status_code}")

            if status_code == 404:
                logger.warning(f"404 Not Found 확인: {url}. 삭제 대상으로 표시합니다.")
                return "NOT_FOUND" # 404 오류 시 특별 값 반환

            if status_code == 429 and attempt < max_retries:
                logger.info(f"429 Too Many Requests. {retry_delay}초 후 재시도합니다... ({url})")
                time.sleep(retry_delay)
                continue # 다음 시도 진행
            else:
                # 재시도 횟수 초과 또는 404, 429 외 다른 HTTP 오류
                logger.error(f"HTTP 오류 처리 실패 (재시도 불가 또는 다른 오류): {url} - {http_err}")
                return None # 처리 불가, None 반환

        except requests.exceptions.Timeout:
            logger.warning(f"타임아웃 발생 ({attempt + 1}차 시도): {url}")
            # 타임아웃은 재시도하지 않음 (기본 설정, 필요시 변경 가능)
            return None
        except requests.exceptions.RequestException as req_err:
            logger.warning(f"요청 중 오류 발생 ({attempt + 1}차 시도): {url} - {req_err}")
            return None # 다른 요청 관련 오류
        except Exception as e:
            logger.error(f"제목 추출 중 예상치 못한 오류 ({attempt + 1}차 시도): {url} - {e}")
            return None # 예상치 못한 내부 오류

    # 모든 시도가 실패한 경우 (이론상 여기까지 오기 어려움)
    logger.error(f"모든 시도 실패 후 제목 추출 불가: {url}")
    return None

def process_single_url(url_item: Dict[str, Any], db_filename: str) -> bool:
    """
    단일 URL에 대해 제목 추출 및 DB 업데이트를 수행합니다.
    - 404 오류 발생 시 해당 URL 레코드를 DB에서 삭제합니다.
    - 429 오류는 재시도하며, 다른 오류는 건너뜁니다.
    - 제목 업데이트 실패 시 삭제하지 않고 실패로 처리합니다.
    각 스레드에서 독립적인 DB 연결을 생성하여 사용합니다.

    Args:
        url_item: URL 정보를 담은 딕셔너리 (키 'url' 포함 필수)
        db_filename: 데이터베이스 파일 이름 (경로 제외)

    Returns:
        제목 업데이트 성공 여부 (True/False). 삭제 작업은 이 반환값에 영향을 주지 않음.
    """
    url = url_item.get('url')
    if not url:
        logger.warning("URL 정보가 없는 항목은 건너뜁니다.")
        return False

    logger.debug(f"처리 시작: {url}")
    # fetch_result는 제목 문자열, "NOT_FOUND", 또는 None 일 수 있음
    fetch_result = fetch_and_extract_og_title(url)

    # 각 작업마다 새 DB 연결 생성
    conn = get_db_connection(db_filename)
    if not conn:
        logger.error(f"DB 연결 실패, 처리 불가: {url}")
        return False

    update_successful = False # 최종 제목 업데이트 성공 여부
    delete_attempted = False # 삭제 시도 여부

    try:
        if fetch_result == "NOT_FOUND":
            # 404 오류 시 삭제 시도
            logger.info(f"fetch_and_extract_og_title가 404 (NOT_FOUND) 반환: {url}. 레코드 삭제를 시도합니다.")
            delete_url_from_db(conn, url)
            delete_attempted = True
            update_successful = False # 업데이트는 실패했으므로 False
        elif fetch_result is None:
            # 제목 추출 실패 (404, 429 재시도 실패, 타임아웃, 기타 오류 등)
            logger.info(f"제목을 추출하지 못했습니다 (오류 또는 재시도 실패): {url}. 건너뜁니다.")
            # 삭제하지 않음
            update_successful = False
        else:
            # 제목 추출 성공 시 (fetch_result가 제목 문자열)
            title = fetch_result
            update_success = update_title_in_db(conn, url, title)
            if update_success:
                logger.info(f"업데이트 완료: {url} -> '{title}'")
                update_successful = True
            else:
                # 업데이트 실패 시 (DB 오류 등)
                logger.warning(f"DB 업데이트 실패: {url}. (삭제하지 않음)")
                update_successful = False

    except Exception as e:
        logger.error(f"처리 중 예기치 않은 오류 발생 ({url}): {e}")
        update_successful = False
    finally:
        if conn:
            conn.close()

    # 제목 업데이트의 성공 여부만 반환 (삭제는 부가 작업)
    return update_successful

def update_titles(
    db_filename: str,
    target_date: str,
    date_column: str,
    parallel_count: int
):
    """
    지정된 날짜의 URL들의 제목을 병렬로 업데이트합니다.

    Args:
        db_filename: 데이터베이스 파일 이름 (경로 제외)
        target_date: 처리 대상 날짜 (YYYY-MM-DD)
        date_column: 날짜 컬럼명
        parallel_count: 동시에 실행할 스레드 수
    """
    start_time = time.time()
    logger.info(f"'{target_date}' 날짜 데이터 제목 업데이트 시작...")
    logger.info(f"데이터베이스 파일: {db_filename}")
    logger.info(f"데이터베이스 경로: {os.path.join(config.DATA_DIR, db_filename)}") # 실제 경로 로깅
    logger.info(f"날짜 컬럼: {date_column}")
    logger.info(f"병렬 처리 수: {parallel_count}")

    # 데이터베이스 초기화 (테이블 및 컬럼 확인/생성)
    # db_storage의 initialize_db는 DATA_DIR 내의 파일에 대해 작동
    initialize_db(db_filename)

    # 이제 DB 연결 시도
    conn = get_db_connection(db_filename)
    if not conn:
        logger.error("데이터베이스 연결에 실패하여 작업을 중단합니다.")
        return

    total_items = 0
    items_to_process = []
    try:
        # title 컬럼 확인 및 추가는 initialize_db가 처리함
        # add_title_column_if_not_exists(conn) # 제거

        # 업데이트할 URL 목록 가져오기
        items_to_process = get_urls_to_update(conn, target_date, date_column)
        total_items = len(items_to_process)

        if total_items == 0:
            logger.info("업데이트할 URL이 없습니다.")
            return

        logger.info(f"총 {total_items}개의 URL에 대해 제목 업데이트를 시작합니다.")

    finally:
        # URL 목록 조회 후 메인 연결은 닫음
        if conn:
            conn.close()

    # 병렬 처리 실행
    success_count = 0
    fail_count = 0
    processed_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_count) as executor:
        # 각 URL 처리를 future로 제출 (db_filename 전달)
        future_to_url = {
            executor.submit(process_single_url, item, db_filename): item.get('url')
            for item in items_to_process
        }

        # 완료되는 future 순서대로 결과 처리
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            processed_count += 1
            try:
                result = future.result() # True 또는 False 반환
                if result:
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as exc:
                logger.error(f"URL 처리 중 예외 발생: {url} - {exc}")
                fail_count += 1

            # 진행 상황 로깅 (예: 매 10개마다 또는 일정 비율마다)
            if processed_count % 10 == 0 or processed_count == total_items:
                logger.info(f"진행状況: {processed_count}/{total_items} 처리 완료 (성공: {success_count}, 실패: {fail_count})")

    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info("===== 제목 업데이트 작업 완료 ====")
    logger.info(f"총 처리 시간: {elapsed_time:.2f} 초")
    logger.info(f"총 시도 URL: {total_items}")
    logger.info(f"업데이트 성공: {success_count}")
    logger.info(f"업데이트 실패/스킵: {fail_count}")
    logger.info("=================================")

def delete_url_from_db(conn: sqlite3.Connection, url: str) -> bool:
    """
    데이터베이스에서 특정 URL의 레코드를 삭제합니다.

    Args:
        conn: SQLite 데이터베이스 연결 객체
        url: 삭제할 URL

    Returns:
        삭제 성공 여부 (True/False)
    """
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM websites WHERE url = ?", (url,))
        conn.commit()
        # 삭제된 행이 있는지 확인 (선택적)
        if cursor.rowcount > 0:
            logger.info(f"레코드 삭제 성공: {url}")
            return True
        else:
            logger.warning(f"삭제할 레코드를 찾지 못함: {url}")
            return False # 실제 삭제된 행이 없는 경우
    except sqlite3.Error as e:
        logger.error(f"DB 레코드 삭제 실패: {url} - {e}")
        try:
            conn.rollback() # 오류 발생 시 롤백
        except sqlite3.Error as rb_err:
            logger.error(f"삭제 작업 롤백 중 오류: {rb_err}")
        return False

def main():
    """스크립트 실행을 위한 메인 함수 및 CLI 인자 처리"""
    parser = argparse.ArgumentParser(description="데이터베이스 내 URL의 웹사이트 제목(og:site_name) 업데이트 도구")

    # --db 인자 도움말 수정 (파일 이름만 받도록)
    parser.add_argument("--db", default=config.DEFAULT_DB_FILENAME, help=f"데이터베이스 파일 이름 (경로 제외, 기본값: {config.DEFAULT_DB_FILENAME})")
    # --date 인자 도움말 수정
    parser.add_argument("--date", default=DEFAULT_TARGET_DATE, help=f"업데이트 시작 날짜 (YYYY-MM-DD 형식, 이 날짜 포함 이후 데이터 처리, 기본값: {DEFAULT_TARGET_DATE})")
    parser.add_argument("--date-column", default=DEFAULT_DATE_COLUMN, help=f"DB 테이블의 날짜 컬럼명 (기본값: {DEFAULT_DATE_COLUMN})")
    parser.add_argument("--parallel", type=int, default=DEFAULT_PARALLEL_COUNT, help=f"병렬 처리 스레드 수 (기본값: {DEFAULT_PARALLEL_COUNT})")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="로깅 레벨 설정 (기본값: INFO)"
    )

    args = parser.parse_args()

    # 로그 레벨 설정
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logger.setLevel(log_level)
    # 루트 로거 레벨도 설정 (필요시)
    logging.getLogger().setLevel(log_level)

    # 제목 업데이트 함수 호출 (db 파일 이름만 전달)
    update_titles(
        db_filename=args.db,
        target_date=args.date,
        date_column=args.date_column,
        parallel_count=args.parallel
    )

if __name__ == "__main__":
    main() 