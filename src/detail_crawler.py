"""
Module for crawling detailed information from previously crawled URLs.
"""

import os
import re
import time
import logging
import concurrent.futures
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, urlunparse, parse_qs, unquote
import sqlite3
from playwright.sync_api import sync_playwright, Page
import src.config as config
from src.scraper import initialize_browser, close_browser
from src.db_storage import (
    initialize_db,
    get_processed_urls as get_processed_urls_db,
    save_to_db,
    read_urls_from_db,
    normalize_field_name,
    get_db_connection,
)
import argparse

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 병렬 처리 수 설정
_parallel_count = 4


def set_parallel_count(count=4):
    """
    병렬 처리 수를 설정합니다.

    Args:
        count: 동시에 처리할 URL 수 (기본값: 4)
    """
    global _parallel_count
    _parallel_count = max(1, count)  # 최소 1 이상
    logger.info(f"병렬 처리 수가 {_parallel_count}로 설정되었습니다.")


def get_processed_urls(db_filename: str) -> Set[str]:
    """
    이미 처리된 URL 목록을 가져옵니다.
    (SQLite DB에서 읽습니다)

    Args:
        db_filename: 데이터베이스 파일명

    Returns:
        이미 처리된 URL 집합 (Set)
    """
    return get_processed_urls_db(db_filename)


def extract_modoo_url(url: str) -> Optional[str]:
    """
    네이버 인플로우 URL에서 modoo.at 도메인 URL을 추출합니다.

    Args:
        url: 원본 URL

    Returns:
        추출된 modoo.at URL 또는 None
    """
    if not url:
        return None

    try:
        # 네이버 인플로우 URL인지 확인
        if "inflow.pay.naver.com" in url:
            # URL 파싱
            parsed = urlparse(url)
            # 쿼리 파라미터 파싱
            query_params = parse_qs(parsed.query)

            # retUrl 파라미터가 있는지 확인
            if "retUrl" in query_params:
                encoded_url = query_params["retUrl"][0]
                # URL 디코딩
                decoded_url = unquote(encoded_url)

                # modoo.at 도메인이 포함되어 있는지 확인
                if "modoo.at" in decoded_url:
                    logger.info(f"모두 URL 추출: {decoded_url} (원본: {url})")
                    return decoded_url

        # modoo.at 도메인인지 직접 확인
        parsed = urlparse(url)
        if "modoo.at" in parsed.netloc:
            return url

        return None
    except Exception as e:
        logger.error(f"모두 URL 추출 중 오류: {url} - {e}")
        return None


def normalize_url(url: str) -> str:
    """
    URL을 정규화합니다 - 쿼리 파라미터와 프래그먼트를 제거합니다.
    네이버 인플로우 URL에서 modoo.at URL을 추출합니다.

    Args:
        url: 정규화할 URL

    Returns:
        정규화된 URL 또는 빈 문자열
    """
    if not url:
        return ""

    try:
        # 네이버 인플로우 URL에서 모두 URL 추출 시도
        modoo_url = extract_modoo_url(url)
        if modoo_url:
            url = modoo_url

        # URL 파싱
        parsed = urlparse(url)

        # 쿼리 파라미터와 프래그먼트 제거
        normalized = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                "",  # params
                "",  # query
                "",  # fragment
            )
        )

        # 경로가 비어있으면 '/'를 추가
        if parsed.netloc and not normalized.endswith("/"):
            normalized += "/"

        return normalized
    except Exception as e:
        logger.error(f"URL 정규화 중 오류: {url} - {e}")
        return url


def clean_database_urls(db_filename: str) -> int:
    """
    데이터베이스의 URL을 정규화하고 중복을 제거합니다.
    또한 모두(modoo.at) 도메인 URL만 유지하고 나머지는 삭제합니다.

    Args:
        db_filename: 데이터베이스 파일명

    Returns:
        처리된 URL 수
    """
    logger.info("데이터베이스 URL 정규화 및 중복 제거 시작...")

    conn = get_db_connection(db_filename)
    if not conn:
        logger.error("데이터베이스 연결 실패")
        return 0

    try:
        cursor = conn.cursor()

        # 모든 URL 가져오기
        cursor.execute("SELECT url FROM websites")
        rows = cursor.fetchall()

        if not rows:
            logger.info("처리할 URL이 없습니다.")
            return 0

        logger.info(f"총 {len(rows)}개의 URL을 처리합니다.")

        # URL 정규화 및 중복 확인을 위한 딕셔너리
        normalized_urls = {}  # {normalized_url: original_url}
        duplicate_urls = []  # 중복으로 제거할 URL 목록
        update_pairs = []  # 업데이트할 (original_url, normalized_url) 쌍
        delete_urls = []  # 삭제할 URL 목록 (modoo.at이 아닌 URL)

        # 모든 URL을 정규화하고 중복 확인
        for row in rows:
            original_url = row["url"]

            # URL 정규화 (모두 URL 추출 포함)
            normalized_url = normalize_url(original_url)

            # modoo.at URL 확인
            modoo_url = extract_modoo_url(original_url)

            # modoo.at URL이 아니면 삭제 목록에 추가
            if not modoo_url and "modoo.at" not in original_url:
                delete_urls.append(original_url)
                logger.debug(f"모두 URL이 아님 (삭제 예정): {original_url}")
                continue

            # 정규화된 URL이 비어있으면 건너뜀
            if not normalized_url:
                delete_urls.append(original_url)
                logger.debug(f"정규화 후 빈 URL (삭제 예정): {original_url}")
                continue

            # 이미 같은 정규화된 URL이 있는지 확인
            if normalized_url in normalized_urls:
                # 중복으로 표시
                duplicate_urls.append(original_url)
                logger.debug(f"중복 URL 발견: {original_url} -> {normalized_url}")
            else:
                # 새로운 정규화된 URL 추가
                normalized_urls[normalized_url] = original_url
                # 원본 URL과 정규화된 URL이 다르면 업데이트 목록에 추가
                if original_url != normalized_url:
                    update_pairs.append((original_url, normalized_url))

        # URL 정규화 및 데이터 이동
        processed_count = 0

        # 트랜잭션 시작
        conn.execute("BEGIN TRANSACTION")

        # modoo.at이 아닌 URL 삭제
        for url in delete_urls:
            cursor.execute("DELETE FROM websites WHERE url = ?", (url,))
            processed_count += 1
            logger.debug(f"모두 URL이 아니어서 삭제: {url}")

        # 정규화된 URL로 업데이트 (중복이 아닌 URL만)
        for original_url, new_url in update_pairs:
            if original_url not in duplicate_urls:
                # 새 URL이 이미 존재하는지 확인
                cursor.execute("SELECT 1 FROM websites WHERE url = ?", (new_url,))
                if cursor.fetchone():
                    # 이미 존재하면 원본 삭제
                    cursor.execute(
                        "DELETE FROM websites WHERE url = ?", (original_url,)
                    )
                    logger.debug(f"삭제: {original_url} (이미 {new_url}이 존재함)")
                else:
                    # 존재하지 않으면 URL 업데이트
                    cursor.execute(
                        "UPDATE websites SET url = ? WHERE url = ?",
                        (new_url, original_url),
                    )
                    logger.debug(f"업데이트: {original_url} -> {new_url}")
                processed_count += 1

        # 중복 URL 제거
        for url in duplicate_urls:
            cursor.execute("DELETE FROM websites WHERE url = ?", (url,))
            processed_count += 1
            logger.debug(f"중복 삭제: {url}")

        # 트랜잭션 커밋
        conn.commit()

        logger.info(f"URL 정규화 및 중복 제거 완료: {processed_count}개 처리됨")
        logger.info(f"- URL 업데이트: {len(update_pairs)}개")
        logger.info(f"- 중복 URL 제거: {len(duplicate_urls)}개")
        logger.info(f"- 모두 URL이 아니어서 삭제: {len(delete_urls)}개")

        return processed_count

    except Exception as e:
        logger.error(f"URL 정규화 및 중복 제거 중 오류: {e}")
        conn.rollback()
        return 0
    finally:
        conn.close()


def fetch_and_extract_og_title(page: Page) -> Optional[str]:
    """
    웹페이지에서 제목을 추출합니다.
    og:site_name, og:title, 일반 title 태그 순서로 확인합니다.

    Args:
        page: Playwright 페이지 객체

    Returns:
        추출된 제목 문자열 또는 None
    """
    try:
        # og:site_name 메타 태그 확인
        og_site_name = page.query_selector('meta[property="og:site_name"]')
        if og_site_name:
            content = og_site_name.get_attribute('content')
            if content:
                title = content.strip()
                logger.debug(f"og:site_name 제목 추출: {title}")
                return title

        # og:title 메타 태그 확인
        og_title = page.query_selector('meta[property="og:title"]')
        if og_title:
            content = og_title.get_attribute('content')
            if content:
                title = content.strip()
                logger.debug(f"og:title 제목 추출: {title}")
                return title

        # 일반 title 태그 확인
        title_tag = page.query_selector('title')
        if title_tag:
            title = title_tag.inner_text().strip()
            if title:
                logger.debug(f"title 태그 제목 추출: {title}")
                return title

        logger.debug("어떤 제목 정보도 찾을 수 없음")
        return None

    except Exception as e:
        logger.error(f"제목 추출 중 오류: {e}")
        return None


def extract_footer_info(page: Page) -> Dict[str, str]:
    """
    웹페이지의 푸터에서 기업 정보를 추출합니다.

    Args:
        page: Playwright 페이지 객체

    Returns:
        추출된 기업 정보가 담긴 딕셔너리
    """
    info = {"company": "", "phone_number": "", "email": "", "address": "", "title": ""}

    try:
        # 제목 추출 시도 (og:site_name, og:title, title 태그 순서)
        title = fetch_and_extract_og_title(page)
        if title:
            info["title"] = title

        # 푸터 영역이 존재하는지 확인
        footer_selector = (
            "#main > div.footer._footer > div.section_footer > div > div.area_info"
        )
        if not page.query_selector(footer_selector):
            logger.debug("푸터 영역을 찾을 수 없습니다.")
            return info

        # 정보 목록 확인
        list_items = page.query_selector_all(f"{footer_selector} ul.list_info > li")
        if not list_items:
            logger.debug("푸터 정보 목록을 찾을 수 없습니다.")
            return info

        # 각 항목 처리
        for item in list_items:
            text = item.inner_text().strip()

            # 전화번호 추출
            if "전화번호" in text:
                phone_match = re.search(r"전화번호\s*:?\s*([0-9\-]+)", text)
                if phone_match:
                    info["phone_number"] = phone_match.group(1)

            # 이메일 추출
            elif "이메일" in text:
                email_match = re.search(
                    r"이메일\s*:?\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",
                    text,
                )
                if email_match:
                    info["email"] = email_match.group(1)

            # 주소 추출 (주소 형태를 가진 텍스트로 판단)
            elif (
                "광역시" in text
                or "시 " in text
                or "군 " in text
                or "도 " in text
                or "구 " in text
            ):
                if "사업자등록번호" not in text and "연락처" not in text:
                    info["address"] = text

            # 기업명 추출 (첫 번째 항목으로 가정)
            elif (
                info["company"] == ""
                and "사업자등록번호" not in text
                and "대표" not in text
            ):
                if len(text) < 30:  # 길이 제한으로 주소가 아닌 항목 구분
                    info["company"] = text

    except Exception as e:
        logger.error(f"푸터 정보 추출 중 오류: {e}")

    return info


def extract_talk_link(page: Page) -> str:
    """
    웹페이지에서 네이버 톡톡 링크를 추출합니다.

    Args:
        page: Playwright 페이지 객체

    Returns:
        추출된 톡톡 링크
    """
    talk_link = ""

    try:
        # 스크립트 태그 내용 가져오기
        script_content = page.content()

        # 정규식으로 톡톡 링크 추출 시도
        talk_pattern = r"https://talk\.naver\.com/[a-zA-Z0-9/]+"
        matches = re.findall(talk_pattern, script_content)

        if matches:
            # 중복 제거 및 "ct/" 또는 "wc" 포함된 링크 우선
            for match in matches:
                if "/ct/" in match or "/wc" in match:
                    talk_link = match.split("'")[0].split('"')[0].split("?")[0]
                    break

            # 우선 조건 없으면 첫 번째 링크 사용
            if not talk_link and matches:
                talk_link = matches[0].split("'")[0].split('"')[0].split("?")[0]

    except Exception as e:
        logger.error(f"톡톡 링크 추출 중 오류: {e}")

    return talk_link


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
        # 삭제된 행이 있는지 확인
        if cursor.rowcount > 0:
            logger.info(f"레코드 삭제 성공: {url}")
            return True
        else:
            logger.warning(f"삭제할 레코드를 찾지 못함: {url}")
            return False
    except sqlite3.Error as e:
        logger.error(f"DB 레코드 삭제 실패: {url} - {e}")
        try:
            conn.rollback()
        except sqlite3.Error as rb_err:
            logger.error(f"삭제 작업 롤백 중 오류: {rb_err}")
        return False


def crawl_detail_page(url: str) -> Dict[str, str]:
    """
    특정 URL에서 상세 정보를 크롤링합니다.

    Args:
        url: 크롤링할 URL

    Returns:
        추출된 상세 정보가 담긴 딕셔너리
    """
    details = {
        "url": url,
        "company": "",
        "phone_number": "",
        "email": "",
        "address": "",
        "talk_link": "",
        "title": "",
    }

    # 브라우저 초기화
    playwright, browser, context, page = initialize_browser()

    try:
        # URL로 이동
        logger.info(f"URL 접속 중: {url}")
        page.goto(url, timeout=30000)

        # 페이지 로딩 대기
        page.wait_for_load_state("networkidle", timeout=10000)

        # 404 또는 "페이지를 찾을 수 없습니다" 확인
        # page_content = page.content()
        # if "요청하신 페이지를 찾을 수 없습니다" in page_content or "404" in page_content:
        #     logger.warning(f"404 또는 페이지를 찾을 수 없음: {url}")
        #     # DB에서 URL 삭제
        #     conn = get_db_connection(config.DEFAULT_DB_FILENAME)
        #     if conn:
        #         try:
        #             delete_url_from_db(conn, url)
        #         finally:
        #             conn.close()
        #     return None

        # 푸터 정보 추출 (title 포함)
        footer_info = extract_footer_info(page)
        details.update(footer_info)

        # 톡톡 링크 추출
        details["talk_link"] = extract_talk_link(page)

        logger.info(f"크롤링 완료: {url}")
        logger.debug(f"- 기업명: {details['company']}")
        logger.debug(f"- 전화번호: {details['phone_number']}")
        logger.debug(f"- 이메일: {details['email']}")
        logger.debug(f"- 주소: {details['address']}")
        logger.debug(f"- 톡톡링크: {details['talk_link']}")
        logger.debug(f"- 제목: {details['title']}")

    except Exception as e:
        logger.error(f"상세 페이지 크롤링 중 오류: {url} - {e}")

    finally:
        # 브라우저 종료
        close_browser(playwright, browser, context)

    return details


def save_intermediate_results(results: List[Dict[str, str]], db_filename: str) -> None:
    """
    중간 결과를 데이터베이스에 저장합니다.

    Args:
        results: 저장할 결과 목록
        db_filename: 데이터베이스 파일명
    """
    if not results:
        logger.warning("저장할 중간 결과가 없습니다.")
        return

    try:
        # 데이터베이스에 중간 결과 저장
        saved_count = save_to_db(results, db_filename)
        logger.info(f"중간 결과 {saved_count}개를 데이터베이스에 저장했습니다.")
    except Exception as e:
        logger.error(f"중간 결과 저장 중 오류: {e}")


def process_url(item, i, total_items):
    """
    단일 URL을 처리하고 결과를 반환합니다.

    Args:
        item: 처리할 URL 정보
        i: 인덱스
        total_items: 전체 URL 수

    Returns:
        처리된 URL 상세 정보
    """
    # URL 추출
    url = item.get("url", "")
    if not url:
        logger.warning(f"[{i+1}/{total_items}] URL이 누락된 항목을 건너뜁니다.")
        return None

    # Name 정보가 있으면 함께 저장
    name = item.get("name", "")

    # 상세 정보 크롤링 결과 키워드 추가
    try:
        logger.info(f"[{i+1}/{total_items}] 처리 중: {url}")
        details = crawl_detail_page(url)

        # 404 또는 페이지를 찾을 수 없는 경우 None 반환
        if details is None:
            return None

        # Name 필드 추가
        if name:
            details["name"] = name

        # Keyword 필드가 있으면 추가
        keyword = item.get("keyword", "")
        if keyword:
            details["keyword"] = keyword

        return details
    except Exception as e:
        logger.error(f"URL 처리 중 오류: {url} - {e}")
        return None


def filter_urls_by_keywords(
    items: List[Dict[str, str]],
    include_keywords: List[str] = None,
    exclude_keywords: List[str] = None,
    case_sensitive: bool = False,
) -> Tuple[List[Dict[str, str]], int, int, int]:
    """
    키워드 조건에 맞는 URL만 필터링합니다.

    Args:
        items: 필터링할 URL 항목 목록
        include_keywords: 포함해야 하는 키워드 리스트 (OR 조건)
        exclude_keywords: 제외해야 하는 키워드 리스트 (OR 조건)
        case_sensitive: 대소문자 구분 여부

    Returns:
        필터링된 URL 목록과 통계 정보 (필터링된 항목 수, 포함 키워드로 필터링된 수, 제외 키워드로 필터링된 수)
    """
    if not items:
        return [], 0, 0, 0

    # 키워드 리스트가 모두 비어있으면 필터링하지 않음
    if not include_keywords and not exclude_keywords:
        return items, 0, 0, 0

    # 키워드 리스트 정규화
    include_keywords = include_keywords or []
    exclude_keywords = exclude_keywords or []

    # 대소문자 구분이 없는 경우 모든 키워드를 소문자로 변환
    if not case_sensitive:
        include_keywords = [kw.lower() for kw in include_keywords]
        exclude_keywords = [kw.lower() for kw in exclude_keywords]

    filtered_items = []
    include_filtered_count = 0
    exclude_filtered_count = 0
    both_filtered_count = 0

    for item in items:
        # URL과 키워드 가져오기
        url = item.get("url", "")
        keyword = item.get("keyword", "")
        name = item.get("name", "")

        # 검색 문자열 (URL, 키워드, 이름 포함)
        search_text = f"{url} {keyword} {name}"
        if not case_sensitive:
            search_text = search_text.lower()

        # 포함 키워드 확인
        has_include_keyword = False
        if not include_keywords:
            has_include_keyword = True  # 포함 키워드가 없으면 항상 True
        else:
            for kw in include_keywords:
                if kw in search_text:
                    has_include_keyword = True
                    break

        # 제외 키워드 확인
        has_exclude_keyword = False
        for kw in exclude_keywords:
            if kw in search_text:
                has_exclude_keyword = True
                break

        # 조건에 따른 필터링:
        # 1. 포함 키워드가 있고 제외 키워드가 없는 경우
        # 2. 포함 키워드가 없고 제외 키워드가 있는 경우에는 제외 키워드가 없는 항목만 포함
        if has_include_keyword and not has_exclude_keyword:
            filtered_items.append(item)
        elif not include_keywords and not has_exclude_keyword:
            filtered_items.append(item)
        else:
            # 필터링 통계
            if has_include_keyword and has_exclude_keyword:
                both_filtered_count += 1
            elif has_include_keyword:
                include_filtered_count += 1
            elif has_exclude_keyword:
                exclude_filtered_count += 1

    total_filtered = len(items) - len(filtered_items)
    logger.info(
        f"키워드 필터링 결과: 총 {len(items)}개 중 {len(filtered_items)}개 선택 ({total_filtered}개 필터링됨)"
    )
    logger.info(f"- 포함 키워드: {include_keywords if include_keywords else '없음'}")
    logger.info(f"- 제외 키워드: {exclude_keywords if exclude_keywords else '없음'}")

    return (
        filtered_items,
        total_filtered,
        include_filtered_count,
        exclude_filtered_count,
    )


def crawl_details_from_db(
    db_filename: str,
    save_interval: int = 10,
    resume: bool = True,
    include_keywords: List[str] = None,
    exclude_keywords: List[str] = None,
    case_sensitive: bool = False,
):
    """
    SQLite 데이터베이스에서 URL을 읽어와 상세 정보를 크롤링합니다.
    결과는 SQLite 데이터베이스에 저장됩니다.

    Args:
        db_filename: 데이터베이스 파일명
        save_interval: 중간 저장 간격
        resume: 이어서 작업할지 여부
        include_keywords: 포함해야 하는 키워드 리스트
        exclude_keywords: 제외해야 하는 키워드 리스트
        case_sensitive: 키워드 대소문자 구분 여부
    """
    # 데이터베이스 초기화
    initialize_db(db_filename)

    # 입력 데이터 가져오기 (DB에서)
    items = read_urls_from_db(db_filename)
    if not items:
        logger.error(
            f"처리할 URL이 없습니다. {db_filename} 데이터베이스를 확인해주세요."
        )
        return

    # 키워드 필터링 적용
    if include_keywords or exclude_keywords:
        items, total_filtered, include_filtered, exclude_filtered = (
            filter_urls_by_keywords(
                items, include_keywords, exclude_keywords, case_sensitive
            )
        )
        if not items:
            logger.info("키워드 필터링 후 처리할 URL이 없습니다.")
            return

    # 이미 상세 정보가 있는 URL 목록 가져오기
    processed_urls = set()
    if resume:
        # 상세 필드 중 하나라도 값이 있는 URL 찾기
        conn = get_db_connection(db_filename)
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT url FROM websites 
                WHERE company != '' OR phone_number != '' OR 
                      email != '' OR address != '' OR talk_link != ''
                """
            )
            rows = cursor.fetchall()
            for row in rows:
                if row["url"]:
                    processed_urls.add(row["url"])
            logger.info(f"이미 상세 정보가 있는 URL: {len(processed_urls)}개")
        except Exception as e:
            logger.error(f"상세 정보가 있는 URL 조회 중 오류: {e}")
        finally:
            conn.close()

    # 처리할 URL 필터링
    filtered_items = []
    for item in items:
        url = item.get("url", "")
        if not url:
            continue
        if resume and url in processed_urls:
            logger.debug(f"이미 상세 정보가 있는 URL 건너뜀: {url}")
            continue
        filtered_items.append(item)

    total_items = len(filtered_items)
    logger.info(f"처리할 URL: {total_items}개")

    if not filtered_items:
        logger.info("모든 URL이 이미 처리되었습니다.")
        return

    # 병렬 처리
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=_parallel_count) as executor:
        # 작업 제출
        future_to_url = {
            executor.submit(process_url, item, i, total_items): (i, item)
            for i, item in enumerate(filtered_items)
        }

        # 결과 처리
        for i, future in enumerate(concurrent.futures.as_completed(future_to_url)):
            idx, item = future_to_url[future]
            url = item.get("url", "")

            try:
                details = future.result()
                if details:
                    results.append(details)
                    logger.info(f"[{i+1}/{total_items}] 완료: {url}")
                else:
                    logger.warning(f"[{i+1}/{total_items}] 실패: {url}")
            except Exception as e:
                logger.error(f"[{i+1}/{total_items}] 오류: {url} - {e}")

            # 중간 저장
            if (i + 1) % save_interval == 0 or (i + 1) == total_items:
                save_intermediate_results(results, db_filename)
                # 저장 후 리스트 비우기
                results = []

    logger.info("모든 URL 처리가 완료되었습니다.")


def main():
    """모듈 테스트용 메인 함수"""
    parser = argparse.ArgumentParser(description="URL 상세 정보 크롤링 도구")
    parser.add_argument("--db", default="crawler_data.db", help="데이터베이스 파일명")
    parser.add_argument("--interval", type=int, default=10, help="중간 저장 간격")
    parser.add_argument("--new", action="store_true", help="처음부터 다시 크롤링")
    parser.add_argument("--parallel", type=int, default=4, help="병렬 처리 수")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="로깅 레벨",
    )
    parser.add_argument(
        "--skip-url-cleaning",
        action="store_true",
        help="URL 정규화 및 중복 제거 건너뛰기",
    )
    parser.add_argument(
        "--test-url", help="URL 추출 및 정규화 테스트를 위한 URL (테스트 모드)"
    )
    parser.add_argument(
        "--include",
        nargs="+",
        help="포함해야 하는 키워드 리스트 (여러 개 지정 가능, 공백으로 구분)",
    )
    parser.add_argument(
        "--exclude",
        nargs="+",
        help="제외해야 하는 키워드 리스트 (여러 개 지정 가능, 공백으로 구분)",
    )
    parser.add_argument(
        "--case-sensitive", action="store_true", help="키워드 대소문자 구분 사용"
    )
    args = parser.parse_args()

    # 로그 레벨 설정
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # URL 테스트 모드
    if args.test_url:
        test_url = args.test_url
        print(f"\n===== URL 테스트 모드 =====")
        print(f"원본 URL: {test_url}")

        # 모두 URL 추출 테스트
        modoo_url = extract_modoo_url(test_url)
        print(f"모두 URL 추출 결과: {modoo_url}")

        # URL 정규화 테스트
        normalized_url = normalize_url(test_url)
        print(f"URL 정규화 결과: {normalized_url}")
        print("=========================\n")
        return

    # 병렬 처리 수 설정
    set_parallel_count(args.parallel)

    # 데이터베이스 초기화
    initialize_db(args.db)

    # URL 정규화 및 중복 제거 (선택적으로 건너뛸 수 있음)
    if not args.skip_url_cleaning:
        clean_database_urls(args.db)

    # 키워드 필터링 옵션 처리
    # config.py에서 기본값 가져오기
    include_keywords = config.DETAIL_INCLUDE_KEYWORDS
    exclude_keywords = config.DETAIL_EXCLUDE_KEYWORDS
    case_sensitive = config.DETAIL_CASE_SENSITIVE

    # 명령행 인자로 기본값 재정의
    if args.include:
        include_keywords = args.include
    if args.exclude:
        exclude_keywords = args.exclude
    if args.case_sensitive:
        case_sensitive = True

    # 키워드 필터링 옵션 출력
    if include_keywords or exclude_keywords:
        logger.info("키워드 필터링 옵션 설정:")
        if include_keywords:
            logger.info(f"- 포함 키워드: {include_keywords}")
        if exclude_keywords:
            logger.info(f"- 제외 키워드: {exclude_keywords}")
        if case_sensitive:
            logger.info("- 대소문자 구분: 사용")

    # 크롤링 시작
    crawl_details_from_db(
        args.db,
        args.interval,
        not args.new,
        include_keywords,
        exclude_keywords,
        case_sensitive,
    )


if __name__ == "__main__":
    main()
