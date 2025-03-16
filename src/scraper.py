"""
Module for scraping Naver search results using Playwright.
"""

from playwright.sync_api import sync_playwright
from typing import List, Dict, Optional
import src.config as config
import logging


def initialize_browser():
    """Initialize and return a Playwright browser instance."""
    playwright = sync_playwright().start()
    # browser = playwright.chromium.launch(headless=False)
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    return playwright, browser, context, page


def close_browser(playwright, browser, context):
    """Close the browser and Playwright instances."""
    context.close()
    browser.close()
    playwright.stop()


def get_search_page(page, search_query: str, page_num: int) -> bool:
    """
    Navigate to a specific page of Naver search results.

    Args:
        page: Playwright page object
        search_query: The search query string
        page_num: The page number to navigate to

    Returns:
        bool: True if navigation was successful
    """
    start_index = (page_num - 1) * 15 + 1
    url = f"{config.BASE_URL}?query={search_query}&page={page_num}&start={start_index}&where=web"

    try:
        # 페이지 이동
        response = page.goto(url, timeout=15000)

        # 응답 상태 확인
        if not response:
            logging.warning(f"페이지 {page_num}로 이동 중 응답이 없습니다: {url}")
            return False

        if not response.ok:
            logging.warning(
                f"페이지 {page_num} 이동 실패, HTTP 상태 코드: {response.status}: {url}"
            )
            return False

        # 검색 결과 존재 여부 확인
        try:
            page.wait_for_selector(config.RESULTS_SELECTOR, timeout=10000)

            # 추가로 "찾을 수 없습니다" 또는 "검색결과가 없습니다" 등의 텍스트 확인
            no_results_text = page.query_selector(
                "div.not_found"
            ) or page.query_selector("div.api_noresult_wrap")
            if no_results_text:
                logging.warning(f"검색어 '{search_query}'에 대한 결과가 없습니다.")
                return False

            return True

        except Exception as e:
            if "Timeout" in str(e):
                logging.warning(
                    f"페이지 {page_num}에서 검색 결과를 찾을 수 없습니다: {url}"
                )
            else:
                logging.error(f"페이지 {page_num}에서 검색 결과 확인 중 오류: {e}")
            return False

    except Exception as e:
        logging.error(f"페이지 {page_num}로 이동 중 오류: {e}")
        return False


def scrape_search_results(page):
    """
    현재 페이지에서 검색 결과를 스크랩합니다.

    Args:
        page: Playwright 페이지 객체

    Returns:
        List[Dict[str, str]]: 검색 결과 데이터 목록 (title, url 키를 포함한 딕셔너리 리스트)
    """
    results = []

    try:
        # 검색 결과 요소 대기
        page.wait_for_selector(config.RESULTS_SELECTOR, timeout=10000)

        # 검색 결과 요소 목록 가져오기
        result_elements = page.query_selector_all(config.RESULTS_SELECTOR)

        logging.info(f"검색 결과 요소 {len(result_elements)}개 발견")

        # 각 결과 요소에서 데이터 추출
        for element in result_elements:
            # 타이틀과 링크 추출
            title_elem = element.query_selector(config.TITLE_LINK_SELECTOR)
            if not title_elem:
                logging.debug("타이틀 엘리먼트를 찾을 수 없음, 항목 건너뜀")
                continue

            title = title_elem.inner_text().strip()
            link = title_elem.get_attribute("href")

            # 결과에 추가
            if title and link:
                item = {"title": title, "url": link}
                results.append(item)
                logging.debug(f"검색 결과 추가: {title[:30]}... - {link}")
            else:
                logging.debug(
                    f"타이틀 또는 링크가 없음: title={bool(title)}, link={bool(link)}"
                )

        logging.info(f"검색 결과 {len(results)}개 추출 완료")
        return results

    except Exception as e:
        if "Timeout" in str(e):
            logging.warning(f"검색 결과를 찾을 수 없습니다: {str(e)}")
            return []  # 타임아웃인 경우 빈 결과 반환
        else:
            logging.error(f"검색 결과 스크랩 중 오류: {str(e)}")
            raise  # 다른 예외는 그대로 전파
