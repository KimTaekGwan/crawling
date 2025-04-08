"""
Main module to orchestrate the Naver search result scraping process.
"""

import time
import itertools
import os
import logging
import concurrent.futures
from typing import List, Dict, Optional, Tuple
import src.config as config
from src.scraper import (
    initialize_browser,
    close_browser,
    get_search_page,
    scrape_search_results,
)
from src.storage import save_page_data
from src.db_storage import initialize_db, get_db_connection

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 강제 실행 여부 플래그
_force_run = False

# 기존 키워드 건너뛰기 여부 플래그
_skip_existing = True

# 병렬 처리 수 설정
_parallel_count = 6

# 데이터베이스 파일명
DB_FILENAME = config.DEFAULT_DB_FILENAME


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
            "강제 실행 모드가 활성화되었습니다. 작업 이력이 없어도 크롤링을 실행합니다."
        )


def set_skip_existing(skip=True):
    """
    기존 키워드 건너뛰기 여부를 설정합니다.

    Args:
        skip: 기존 키워드 건너뛰기 활성화 여부 (기본값: True)
    """
    global _skip_existing
    _skip_existing = skip
    if not _skip_existing:
        logger.info(
            "모든 키워드 재크롤링 모드가 활성화되었습니다. 이미 크롤링된 키워드도 다시 크롤링합니다."
        )


def set_parallel_count(count=4):
    """
    병렬 처리 수를 설정합니다.

    Args:
        count: 동시에 처리할 검색어 수 (기본값: 4)
    """
    global _parallel_count
    _parallel_count = max(1, count)  # 최소 1 이상
    logger.info(f"병렬 처리 수가 {_parallel_count}로 설정되었습니다.")


def generate_keyword_combinations():
    """
    모든 가능한 키워드 조합을 생성합니다.

    마지막 타입은 필수로 포함되며, 다른 타입들은 선택적으로 포함됩니다.

    Returns:
        List[Tuple]: 조합된 키워드 리스트
    """
    combinations = []

    # 타입 순서 가져오기
    type_order = config.SEARCH_TYPE_ORDER

    # 필수 타입 (마지막 타입)
    required_type = type_order[-1]
    required_values = config.SEARCH_KEYWORD_TYPES[required_type]

    # 선택적 타입들 (마지막 타입 제외)
    optional_types = type_order[:-1]

    # 각 선택적 타입별로 None을 포함한 값 리스트 생성
    optional_values_lists = []
    for opt_type in optional_types:
        values = config.SEARCH_KEYWORD_TYPES[opt_type]
        # None은 해당 타입을 사용하지 않는 경우를 의미
        optional_values_lists.append([None] + values)

    # 선택적 타입들의 모든 조합 생성
    optional_combinations = list(itertools.product(*optional_values_lists))

    # 각 필수 값에 대해
    for req_value in required_values:
        # 선택적 조합이 없는 경우
        if not optional_combinations:
            combinations.append(((required_type, req_value),))
            continue

        # 각 선택적 조합에 대해
        for opt_combo in optional_combinations:
            keyword_combo = []

            # 선택적 타입 값 추가 (None이 아닌 경우에만)
            for i, opt_value in enumerate(opt_combo):
                if opt_value is not None:
                    keyword_combo.append((optional_types[i], opt_value))

            # 필수 타입 값 추가
            keyword_combo.append((required_type, req_value))

            # 튜플로 변환하여 추가
            combinations.append(tuple(keyword_combo))

    return combinations


def combine_keywords(keyword_combo):
    """
    키워드 조합을 하나의 검색어로 만듭니다.
    중복되는 키워드는 제거합니다.

    Args:
        keyword_combo: 조합할 키워드 튜플의 리스트 [(타입1, 값1), (타입2, 값2), ...]

    Returns:
        조합된 검색어
    """
    if not keyword_combo:
        return ""

    # 키워드 값만 추출
    values = [item[1] for item in keyword_combo]

    # 중복 키워드 제거 (대소문자 구분 없이)
    unique_values = []
    seen_lower = set()

    for value in values:
        # 공백 제거 후 소문자로 변환하여 비교
        value_lower = value.strip().lower()

        # 이미 처리된 키워드인지 확인
        if value_lower in seen_lower:
            logger.debug(f"중복 키워드 제거: {value}")
            continue

        # 새로운 키워드 추가
        seen_lower.add(value_lower)
        unique_values.append(value)

    # 로그 출력 (중복이 있는 경우)
    if len(unique_values) < len(values):
        logger.debug(f"원본 키워드: {values}")
        logger.debug(f"중복 제거 후: {unique_values}")

    # config의 SEARCH_JOINER를 사용하여 키워드 조합
    combined = config.SEARCH_JOINER.join(unique_values)

    # 접미사 추가
    if config.SEARCH_SUFFIX:
        combined += config.SEARCH_SUFFIX

    return combined


def format_combo_for_display(keyword_combo):
    """
    키워드 조합을 표시용 문자열로 변환합니다.

    Args:
        keyword_combo: 조합할 키워드 튜플의 리스트 [(타입1, 값1), (타입2, 값2), ...]

    Returns:
        표시용 문자열
    """
    return " + ".join([f"{item[0]}:{item[1]}" for item in keyword_combo])


def scrape_page(page, search_query: str, page_num: int):
    """
    Scrape a single page of search results using an existing browser page.

    Args:
        page: Playwright page object
        search_query: The search query to use
        page_num: The page number to scrape

    Returns:
        bool: Success status
    """
    logger.info(f"Scraping page {page_num} for query '{search_query}'...")

    try:
        # Navigate to the page
        success = get_search_page(page, search_query, page_num)

        if success:
            # Scrape data from the page
            results = scrape_search_results(page)

            # Save the data
            save_page_data(search_query, page_num, results)

            logger.info(
                f"Completed scraping page {page_num} for query '{search_query}', found {len(results)} results."
            )
            return True
        else:
            logger.warning(
                f"Failed to navigate to page {page_num} for query '{search_query}'"
            )
            return False

    except Exception as e:
        logger.error(
            f"Error processing page {page_num} for query '{search_query}': {e}"
        )
        return False


def scrape_all_pages_for_query(search_query: str):
    """
    Scrape all pages in the configured range for a specific query.
    Uses a single browser instance for all pages of this query.

    Args:
        search_query: The search query to use
    """
    logger.info(f"Starting to scrape Naver search results for '{search_query}'")
    logger.info(f"Pages to scrape: {config.START_PAGE} to {config.END_PAGE}")

    # 하나의 브라우저 인스턴스 초기화
    playwright, browser, context, page = initialize_browser()

    try:
        # 연속 실패 감지를 위한 변수
        consecutive_failures = 0
        max_consecutive_failures = 2  # 연속 2번 실패하면 중단

        for page_num in range(config.START_PAGE, config.END_PAGE + 1):
            # 페이지 스크래핑
            success = scrape_page(page, search_query, page_num)

            if success:
                # 성공 시 연속 실패 카운트 초기화
                consecutive_failures = 0

                # 페이지 간 딜레이
                if page_num < config.END_PAGE:
                    logger.info(f"Waiting 2 seconds before next page...")
                    time.sleep(2)
            else:
                # 실패 시 연속 실패 카운트 증가
                consecutive_failures += 1
                logger.warning(
                    f"페이지 {page_num} 크롤링 실패. 연속 실패: {consecutive_failures}/{max_consecutive_failures}"
                )

                # 연속 실패 횟수가 임계값을 초과하면 중단
                if consecutive_failures >= max_consecutive_failures:
                    logger.warning(
                        f"검색어 '{search_query}'에 대한 결과가 더 이상 없는 것으로 판단됩니다. 다음 키워드로 넘어갑니다."
                    )
                    break

    except Exception as e:
        logger.error(f"Error during scraping process for query '{search_query}': {e}")

    finally:
        # 작업 완료 후 브라우저 종료
        close_browser(playwright, browser, context)

    logger.info(f"Scraping process completed for query '{search_query}'.")


def process_keyword_combo(i, keyword_combo, total_combos):
    """
    단일 키워드 조합에 대한 처리를 수행합니다.
    병렬 처리를 위해 분리된 함수입니다.

    Args:
        i: 키워드 인덱스
        keyword_combo: 키워드 조합
        total_combos: 전체 조합 수

    Returns:
        str: 처리 결과 메시지
    """
    # 키워드 조합
    search_query = combine_keywords(keyword_combo)
    if not search_query:
        return f"[{i+1}/{total_combos}] 검색어 변환 실패: {format_combo_for_display(keyword_combo)}"

    # 작업 이력 확인 - 이미 작업된 경우 건너뜀
    if check_keyword_work_history(search_query) and not _force_run:
        return f"[{i+1}/{total_combos}] 검색어 '{search_query}'({format_combo_for_display(keyword_combo)}) - 이미 크롤링됨, 건너뜀"

    # 시작 로그
    logger.info(f"{'='*50}")
    logger.info(
        f"[{i+1}/{total_combos}] 검색어 조합: {format_combo_for_display(keyword_combo)}"
    )
    logger.info(f"변환된 검색어: {search_query}")
    logger.info(f"{'='*50}")

    # 해당 검색어로 모든 페이지 크롤링
    start_time = time.time()
    scrape_all_pages_for_query(search_query)
    elapsed_time = time.time() - start_time

    return f"[{i+1}/{total_combos}] 검색어 '{search_query}' 완료 (소요시간: {elapsed_time:.1f}초)"


def main():
    """Main entry point for the scraper."""
    # 데이터베이스 초기화
    initialize_db(DB_FILENAME)

    # 모든 키워드 조합 생성
    keyword_combinations = generate_keyword_combinations()

    # 조합 수 출력
    total_combos = len(keyword_combinations)
    logger.info(f"총 {total_combos}개의 검색어 조합이 생성되었습니다.")
    logger.info(f"병렬 처리 수: {_parallel_count}")

    # 크롤링할 키워드 목록 준비
    tasks = []
    for i, keyword_combo in enumerate(keyword_combinations):
        # 기본 정보 확인
        search_query = combine_keywords(keyword_combo)
        if not search_query:
            continue

        # 이미 작업된 경우 건너뜀 (여기서는 메시지만 출력)
        if check_keyword_work_history(search_query) and not _force_run:
            logger.info(
                f"[{i+1}/{total_combos}] 검색어 '{search_query}' - 이미 크롤링됨, 건너뜀"
            )
            continue

        # 작업 목록에 추가
        tasks.append((i, keyword_combo, total_combos))

    # 실제 크롤링할 작업 수 출력
    logger.info(f"실제 크롤링할 검색어: {len(tasks)}개")

    if not tasks:
        logger.info("크롤링할 검색어가 없습니다. 종료합니다.")
        return

    # 병렬 처리 실행
    start_time = time.time()

    # 중간 요약 정보
    logger.info(f"총 {total_combos}개의 검색어 조합이 생성되었습니다.")
    logger.info(f"병렬 처리 수: {_parallel_count}")
    logger.info(f"실제 크롤링할 검색어: {len(tasks)}개")

    # 사용자 입력 받기
    user_input = input("계속 진행하시겠습니까? (y/n): ")
    if user_input != "y":
        logger.info("프로그램을 종료합니다.")
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=_parallel_count) as executor:
        # 작업 제출
        futures = [
            executor.submit(process_keyword_combo, i, kw, total_combos)
            for i, kw, total_combos in tasks
        ]

        # 작업 완료 대기 및 결과 수집
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                logger.info(result)
            except Exception as e:
                logger.error(f"작업 처리 중 오류 발생: {e}")

    # 실행 시간 계산
    total_time = time.time() - start_time
    logger.info(f"모든 검색어 조합에 대한 크롤링이 완료되었습니다.")
    logger.info(f"총 소요 시간: {total_time:.1f}초 ({total_time/60:.1f}분)")


def check_keyword_work_history(search_query):
    """
    특정 키워드에 대한 크롤링 작업 이력이 있는지 확인합니다.
    SQLite 데이터베이스에서 검색어로 조회합니다.

    Args:
        search_query: 확인할 검색어

    Returns:
        bool: 해당 키워드에 대한 작업 이력이 있으면 True, 없으면 False
    """
    # 기존 키워드 건너뛰기 옵션이 비활성화된 경우
    if not _skip_existing:
        return False

    # 데이터베이스에서 해당 키워드 검색
    conn = get_db_connection(DB_FILENAME)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) as count FROM websites WHERE keyword = ?", (search_query,)
        )
        result = cursor.fetchone()

        # 결과가 있으면 작업 이력이 있는 것으로 간주
        return result and result["count"] > 0

    except Exception as e:
        logger.error(f"키워드 작업 이력 확인 중 오류: {e}")
        return False

    finally:
        conn.close()


if __name__ == "__main__":
    main()
