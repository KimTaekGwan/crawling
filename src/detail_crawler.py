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
import json

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


def _extract_floating_data(
    page: Page,
    data_type: str,  # 'phone' or 'email' for logging
    primary_script_pattern: Optional[re.Pattern],
    primary_script_group: int,
    secondary_script_pattern: Optional[re.Pattern],
    secondary_script_group: int,
    button_selector: Optional[str],
    button_attribute: str = "data-data",
) -> Optional[str]:
    """
    Helper function to extract data (phone or email) from script content or floating button attributes.
    Tries primary script pattern, then secondary script pattern, then button element.

    Args:
        page: Playwright Page object.
        data_type: Type of data being extracted ('phone' or 'email').
        primary_script_pattern: High-priority regex pattern for script content.
        primary_script_group: Regex group index for primary pattern.
        secondary_script_pattern: Lower-priority regex pattern for script content.
        secondary_script_group: Regex group index for secondary pattern.
        button_selector: CSS selector for the button element.
        button_attribute: Attribute to extract data from the button element.

    Returns:
        Extracted data string or None.
    """
    data = None
    source = None  # 어디서 찾았는지 기록

    try:
        # 스크립트 내용 한 번만 가져오기
        script_content = page.content()

        # 1. 우선 순위 높은 스크립트 패턴 검색
        if primary_script_pattern:
            match = primary_script_pattern.search(script_content)
            if match:
                data = match.group(primary_script_group).strip()
                if data:
                    source = f"스크립트 ({primary_script_pattern.pattern[:20]}...)"  # 패턴 일부 표시

        # 2. 우선 순위 낮은 스크립트 패턴 검색 (데이터가 아직 없을 경우)
        if not data and secondary_script_pattern:
            match = secondary_script_pattern.search(script_content)
            if match:
                data = match.group(secondary_script_group).strip()
                if data:
                    source = f"스크립트 ({secondary_script_pattern.pattern[:20]}...)"

        # 3. 버튼 요소 검색 (데이터가 아직 없을 경우)
        if not data and button_selector:
            button_element = page.query_selector(button_selector)
            if button_element:
                data = button_element.get_attribute(button_attribute)
                if data:
                    data = data.strip()
                    source = f"버튼 속성 ({button_selector})"

        # 최종 결과 로그
        if data:
            logger.debug(f"{source}에서 {data_type} 발견: {data}")
            return data
        else:
            logger.debug(f"스크립트 및 플로팅 버튼에서 {data_type}을(를) 찾을 수 없음")
            return None

    except Exception as e:
        logger.error(f"플로팅 데이터 ({data_type}) 추출 중 오류: {e}")
        return None


def extract_floating_button_phone(page: Page) -> Optional[str]:
    """
    페이지 내 스크립트에서 전화번호를 추출합니다.
    phone 객체 또는 contractData/pcContractData를 확인합니다.

    Args:
        page: Playwright 페이지 객체

    Returns:
        추출된 전화번호 또는 None
    """
    # 패턴 정의
    phone_obj_pattern = re.compile(
        r'"phone"\s*:\s*\{"name"\s*:\s*"([^"]+)",\s*"checked"\s*:\s*1\}'
    )
    contract_data_pattern = re.compile(
        r"""
        (?:contractData|pcContractData)\s*:\s*\[\s*
        \{.*?
        "protocol"\s*:\s*"tel"
        .*?
        "link"\s*:\s*"([^"]+)"
        .*?\}
        """,
        re.VERBOSE | re.DOTALL,
    )

    return _extract_floating_data(
        page=page,
        data_type="phone",
        primary_script_pattern=phone_obj_pattern,
        primary_script_group=1,
        secondary_script_pattern=contract_data_pattern,
        secondary_script_group=1,
        button_selector=None,  # 전화번호는 버튼 속성에 직접 없음
    )


def extract_floating_button_email(page: Page) -> Optional[str]:
    """
    페이지 내 스크립트 또는 플로팅 버튼 요소에서 이메일 주소를 추출합니다.
    스크립트 내 email 객체 (checked:1) 또는 버튼의 data-data 속성을 확인합니다.

    Args:
        page: Playwright 페이지 객체

    Returns:
        추출된 이메일 주소 또는 None
    """
    # 패턴 정의
    email_obj_pattern = re.compile(
        r'"email"\s*:\s*\{"name"\s*:\s*"([^"]+)",\s*"checked"\s*:\s*1\}'
    )
    button_selector = 'a._btnFloating[data-type="email"]'

    return _extract_floating_data(
        page=page,
        data_type="email",
        primary_script_pattern=email_obj_pattern,
        primary_script_group=1,
        secondary_script_pattern=None,  # 이메일은 contractData에 없음
        secondary_script_group=1,
        button_selector=button_selector,
        button_attribute="data-data",
    )


def extract_footer_info(
    page: Page, phone_found_elsewhere: bool = False, email_found_elsewhere: bool = False
) -> Dict[str, str]:
    """
    웹페이지의 푸터에서 기업 정보를 추출합니다.
    phone_found_elsewhere가 True이면 전화번호는 추출하지 않습니다.
    email_found_elsewhere가 True이면 이메일은 추출하지 않습니다.

    Args:
        page: Playwright 페이지 객체
        phone_found_elsewhere: 다른 곳에서 전화번호를 이미 찾았는지 여부
        email_found_elsewhere: 다른 곳에서 이메일을 이미 찾았는지 여부

    Returns:
        추출된 기업 정보가 담긴 딕셔너리
    """
    info = {"company": "", "phone_number": "", "email": "", "address": ""}

    try:
        # 푸터 영역이 존재하는지 확인
        footer_selector = (
            "#main > div.footer._footer > div.section_footer > div > div.area_info"
        )
        footer_element = page.query_selector(footer_selector)
        if not footer_element:
            logger.debug("푸터 영역을 찾을 수 없습니다.")
            return info

        # 정보 목록 확인
        list_items = footer_element.query_selector_all("ul.list_info > li")
        if not list_items:
            logger.debug("푸터 정보 목록(li)을 찾을 수 없습니다.")
            # 푸터 영역 전체 텍스트에서 시도
            footer_text = footer_element.inner_text()

            # 전화번호 추출 (필요한 경우)
            if not phone_found_elsewhere:
                phone_match = re.search(
                    r"(?:전화번호|연락처)\s*:?\s*([0-9\-]+(?: داخلی\s*\d+)?|[0-9]{2,4}\s*-\s*[0-9]{3,4}\s*-\s*[0-9]{4})",
                    footer_text,
                )
                if phone_match:
                    info["phone_number"] = phone_match.group(1).strip()

            # 이메일 추출 (필요한 경우)
            if not email_found_elsewhere:
                email_match = re.search(
                    r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", footer_text
                )
                if email_match:
                    info["email"] = email_match.group(1)

            # 주소 추출
            address_match = re.search(
                r"([^\n]*(?:시|도|군|구|길|로)\s+[^\n]+)", footer_text
            )
            if address_match and "사업자등록번호" not in address_match.group(1):
                info["address"] = address_match.group(1).strip()

            # 회사명 추출
            lines = footer_text.split("\n")
            if lines:
                first_line = lines[0].strip()
                if first_line and not any(
                    kw in first_line
                    for kw in [
                        "사업자등록번호",
                        "대표",
                        "전화번호",
                        "이메일",
                        "주소",
                        "팩스",
                    ]
                ):
                    info["company"] = first_line

            return info

        # 각 항목 처리 (li 태그가 있는 경우)
        for item in list_items:
            text = item.inner_text().strip()

            # 전화번호 추출 (필요한 경우)
            if not phone_found_elsewhere and ("전화번호" in text or "연락처" in text):
                phone_match = re.search(
                    r"(?:전화번호|연락처)\s*:?\s*([0-9\-]+(?: داخلی\s*\d+)?|[0-9]{2,4}\s*-\s*[0-9]{3,4}\s*-\s*[0-9]{4})",
                    text,
                )
                if phone_match:
                    info["phone_number"] = phone_match.group(1).strip()

            # 이메일 추출 (필요한 경우)
            elif not email_found_elsewhere and "이메일" in text:
                email_match = re.search(
                    r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",
                    text,
                )
                if email_match:
                    info["email"] = email_match.group(1)

            # 주소 추출
            elif (
                "주소" in text
                or "광역시" in text
                or "시 " in text
                or "군 " in text
                or "도 " in text
                or "구 " in text
                or "길 " in text
                or "로 " in text
            ):
                if not any(
                    kw in text
                    for kw in [
                        "사업자등록번호",
                        "연락처",
                        "전화번호",
                        "이메일",
                        "대표자",
                        "팩스",
                        "통신판매업",
                    ]
                ):
                    address_text = re.sub(r"^주소\s*:\s*", "", text).strip()
                    if address_text:
                        info["address"] = address_text

            # 기업명 추출
            elif info["company"] == "" and not any(
                kw in text
                for kw in [
                    "사업자등록번호",
                    "대표",
                    "전화번호",
                    "이메일",
                    "주소",
                    "팩스",
                    "통신판매업",
                    "연락처",
                ]
            ):
                if len(text) < 50:
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


def crawl_detail_page(url: str) -> Dict[str, str]:
    """
    특정 URL에서 상세 정보를 크롤링합니다.
    플로팅 버튼의 전화번호와 이메일을 우선적으로 사용합니다.

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
    }

    # 브라우저 초기화
    playwright, browser, context, page = initialize_browser()
    phone_extracted_from_floating = False
    email_extracted_from_floating = False  # 이메일 플래그 추가

    try:
        # URL로 이동
        logger.info(f"URL 접속 중: {url}")
        context.set_extra_http_headers(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
        )
        page.goto(url, timeout=45000, wait_until="domcontentloaded")

        # 페이지 로딩 대기
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception as e:
            logger.warning(f"네트워크 안정화 대기 시간 초과 (계속 진행): {url} - {e}")
            time.sleep(3)

        # "페이지 없음" 오류 확인
        error_locator = page.locator(
            "h1:has-text('요청하신 페이지를 찾을 수 없습니다'), h1:has-text('서비스 점검 중')"
        )
        if error_locator.count() > 0:
            error_text = error_locator.first.inner_text()
            logger.warning(f"'{error_text}' 감지: {url}")
            return {"error": "page_not_found", "url": url}

        # 플로팅 버튼 전화번호 추출 시도
        floating_phone = extract_floating_button_phone(page)
        if floating_phone:
            details["phone_number"] = floating_phone
            phone_extracted_from_floating = True
            logger.info(f"플로팅 버튼에서 전화번호 추출 성공: {floating_phone}")
        else:
            logger.info("플로팅 버튼에서 전화번호를 찾지 못했습니다.")

        # 플로팅 버튼 이메일 추출 시도
        floating_email = extract_floating_button_email(page)
        if floating_email:
            details["email"] = floating_email
            email_extracted_from_floating = True
            logger.info(f"플로팅 버튼/스크립트에서 이메일 추출 성공: {floating_email}")
        else:
            logger.info("플로팅 버튼/스크립트에서 이메일을 찾지 못했습니다.")

        # 푸터 정보 추출 (플로팅 버튼에서 찾았는지 여부 전달)
        footer_info = extract_footer_info(
            page,
            phone_found_elsewhere=phone_extracted_from_floating,
            email_found_elsewhere=email_extracted_from_floating,
        )  # 이메일 플래그 전달

        # 플로팅에서 못 찾았거나 값이 비었으면 푸터 정보 사용 (전화번호)
        if not phone_extracted_from_floating or not details["phone_number"]:
            details["phone_number"] = footer_info.get("phone_number", "")

        # 플로팅에서 못 찾았거나 값이 비었으면 푸터 정보 사용 (이메일)
        if not email_extracted_from_floating or not details["email"]:
            details["email"] = footer_info.get("email", "")

        # 다른 푸터 정보 업데이트 (전화번호, 이메일 제외하고 덮어쓰기)
        details["company"] = footer_info.get("company", "")
        details["address"] = footer_info.get("address", "")

        # 톡톡 링크 추출
        details["talk_link"] = extract_talk_link(page)

        logger.info(f"크롤링 완료: {url}")
        logger.debug(f"- 기업명: {details['company']}")
        logger.debug(
            f"- 전화번호: {details['phone_number']} {'(플로팅)' if phone_extracted_from_floating else '(푸터)' if details['phone_number'] else ''}"
        )
        logger.debug(
            f"- 이메일: {details['email']} {'(플로팅/스크립트)' if email_extracted_from_floating else '(푸터)' if details['email'] else ''}"
        )  # 로그 수정
        logger.debug(f"- 주소: {details['address']}")
        logger.debug(f"- 톡톡링크: {details['talk_link']}")

    except Exception as e:
        # 타임아웃 오류 구분
        if "Timeout" in str(e):
            logger.error(f"상세 페이지 크롤링 중 타임아웃 오류: {url} - {e}")
            # 타임아웃 시 특정 오류 반환 가능
            # return {"error": "timeout", "url": url}
        else:
            logger.error(f"상세 페이지 크롤링 중 오류: {url} - {e}")

    finally:
        # 브라우저 종료
        close_browser(playwright, browser, context)

    return details


def delete_urls_from_db(urls_to_delete: List[str], db_filename: str) -> int:
    """
    데이터베이스에서 지정된 URL 목록을 삭제합니다.

    Args:
        urls_to_delete: 삭제할 URL 목록
        db_filename: 데이터베이스 파일명

    Returns:
        삭제된 행의 수
    """
    if not urls_to_delete:
        return 0

    conn = get_db_connection(db_filename)
    if not conn:
        logger.error("데이터베이스 연결 실패 (삭제)")
        return 0

    deleted_count = 0
    try:
        cursor = conn.cursor()
        # 트랜잭션 시작
        conn.execute("BEGIN TRANSACTION")
        for url in urls_to_delete:
            cursor.execute("DELETE FROM websites WHERE url = ?", (url,))
            deleted_count += cursor.rowcount  # 삭제된 행 수 누적
            if cursor.rowcount > 0:
                logger.info(f"데이터베이스에서 삭제됨 (페이지 없음): {url}")
            else:
                logger.warning(
                    f"데이터베이스에서 URL을 찾을 수 없어 삭제하지 못함: {url}"
                )
        # 트랜잭션 커밋
        conn.commit()
        logger.info(
            f"총 {deleted_count}개의 '페이지 없음' URL을 데이터베이스에서 삭제했습니다."
        )
    except Exception as e:
        logger.error(f"데이터베이스에서 URL 삭제 중 오류: {e}")
        conn.rollback()
        deleted_count = 0  # 롤백 시 삭제 카운트 초기화
    finally:
        conn.close()

    return deleted_count


def save_intermediate_results(results: List[Dict[str, str]], db_filename: str) -> None:
    """
    중간 결과를 데이터베이스에 저장합니다. (오류 결과는 제외)

    Args:
        results: 저장할 결과 목록 (오류가 아닌 결과만 포함)
        db_filename: 데이터베이스 파일명
    """
    valid_results = [res for res in results if "error" not in res]  # 오류 결과 필터링
    if not valid_results:
        logger.debug("저장할 유효한 중간 결과가 없습니다.")
        return

    try:
        # 데이터베이스에 중간 결과 저장
        saved_count = save_to_db(valid_results, db_filename)
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

        # crawl_detail_page에서 오류 반환 시 그대로 반환
        if details and "error" in details:
            return details

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
    urls_to_delete = []  # 삭제할 URL 목록 추가
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
                    # 오류 결과 처리
                    if "error" in details and details["error"] == "page_not_found":
                        urls_to_delete.append(details["url"])
                        logger.warning(
                            f"[{i+1}/{total_items}] '페이지 없음' 오류로 처리 건너뜀: {url}"
                        )
                    else:
                        results.append(details)
                        logger.info(f"[{i+1}/{total_items}] 완료: {url}")
                else:
                    logger.warning(f"[{i+1}/{total_items}] 실패 (결과 없음): {url}")
            except Exception as e:
                logger.error(f"[{i+1}/{total_items}] 오류: {url} - {e}")

            # 중간 저장 및 삭제
            if (i + 1) % save_interval == 0 or (i + 1) == total_items:
                # 삭제할 URL 처리
                if urls_to_delete:
                    delete_urls_from_db(urls_to_delete, db_filename)
                    urls_to_delete = []  # 삭제 후 리스트 비우기

                # 결과 저장
                save_intermediate_results(results, db_filename)
                # 저장 후 리스트 비우기
                results = []

    # 루프 종료 후 남은 URL 삭제 처리 (필요한 경우)
    if urls_to_delete:
        delete_urls_from_db(urls_to_delete, db_filename)

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
