"""
Main module to orchestrate the Naver search result scraping process.
"""

import time
import itertools
import os
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

# 강제 실행 여부 플래그
_force_run = False

# 기존 키워드 건너뛰기 여부 플래그
_skip_existing = True

# 병렬 처리 수 설정
_parallel_count = 4


def set_force_run(force=False):
    """
    강제 실행 여부를 설정합니다.

    Args:
        force: 강제 실행 활성화 여부
    """
    global _force_run
    _force_run = force
    if _force_run:
        print(
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
        print(
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
    print(f"병렬 처리 수가 {_parallel_count}로 설정되었습니다.")


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

    Args:
        keyword_combo: 조합할 키워드 튜플의 리스트 [(타입1, 값1), (타입2, 값2), ...]

    Returns:
        조합된 검색어
    """
    if not keyword_combo:
        return ""

    # 키워드 값만 추출
    values = [item[1] for item in keyword_combo]

    # config의 SEARCH_JOINER를 사용하여 키워드 조합
    combined = config.SEARCH_JOINER.join(values)

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
    print(f"Scraping page {page_num} for query '{search_query}'...")

    try:
        # Navigate to the page
        success = get_search_page(page, search_query, page_num)

        if success:
            # Scrape data from the page
            results = scrape_search_results(page)

            # Save the data
            save_page_data(search_query, page_num, results)

            print(
                f"Completed scraping page {page_num} for query '{search_query}', found {len(results)} results."
            )
            return True
        else:
            print(f"Failed to navigate to page {page_num} for query '{search_query}'")
            return False

    except Exception as e:
        print(f"Error processing page {page_num} for query '{search_query}': {e}")
        return False


def scrape_all_pages_for_query(search_query: str):
    """
    Scrape all pages in the configured range for a specific query.
    Uses a single browser instance for all pages of this query.

    Args:
        search_query: The search query to use
    """
    print(f"Starting to scrape Naver search results for '{search_query}'")
    print(f"Pages to scrape: {config.START_PAGE} to {config.END_PAGE}")

    # 하나의 브라우저 인스턴스 초기화
    playwright, browser, context, page = initialize_browser()

    try:
        for page_num in range(config.START_PAGE, config.END_PAGE + 1):
            # 페이지 스크래핑
            success = scrape_page(page, search_query, page_num)

            # 페이지 간 딜레이
            if page_num < config.END_PAGE and success:
                print(f"Waiting 2 seconds before next page...")
                time.sleep(2)

    except Exception as e:
        print(f"Error during scraping process for query '{search_query}': {e}")

    finally:
        # 작업 완료 후 브라우저 종료
        close_browser(playwright, browser, context)

    print(f"Scraping process completed for query '{search_query}'.")


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
    print(f"\n{'='*50}")
    print(
        f"[{i+1}/{total_combos}] 검색어 조합: {format_combo_for_display(keyword_combo)}"
    )
    print(f"변환된 검색어: {search_query}")
    print(f"{'='*50}\n")

    # 해당 검색어로 모든 페이지 크롤링
    start_time = time.time()
    scrape_all_pages_for_query(search_query)
    elapsed_time = time.time() - start_time

    return f"[{i+1}/{total_combos}] 검색어 '{search_query}' 완료 (소요시간: {elapsed_time:.1f}초)"


def main():
    """Main entry point for the scraper."""
    # 모든 키워드 조합 생성
    keyword_combinations = generate_keyword_combinations()

    # 조합 수 출력
    total_combos = len(keyword_combinations)
    print(f"총 {total_combos}개의 검색어 조합이 생성되었습니다.")
    print(f"병렬 처리 수: {_parallel_count}")

    # 크롤링할 키워드 목록 준비
    tasks = []
    for i, keyword_combo in enumerate(keyword_combinations):
        # 기본 정보 확인
        search_query = combine_keywords(keyword_combo)
        if not search_query:
            continue

        # 이미 작업된 경우 건너뜀 (여기서는 메시지만 출력)
        if check_keyword_work_history(search_query) and not _force_run:
            print(
                f"[{i+1}/{total_combos}] 검색어 '{search_query}' - 이미 크롤링됨, 건너뜀"
            )
            continue

        # 작업 목록에 추가
        tasks.append((i, keyword_combo, total_combos))

    # 실제 크롤링할 작업 수 출력
    print(f"실제 크롤링할 검색어: {len(tasks)}개")

    if not tasks:
        print("크롤링할 검색어가 없습니다. 종료합니다.")
        return

    # 병렬 처리 실행
    start_time = time.time()

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
                print(result)
            except Exception as e:
                print(f"작업 처리 중 오류 발생: {e}")

    # 실행 시간 계산
    total_time = time.time() - start_time
    print(f"\n모든 검색어 조합에 대한 크롤링이 완료되었습니다.")
    print(f"총 소요 시간: {total_time:.1f}초 ({total_time/60:.1f}분)")


def check_work_history():
    """
    크롤링 작업 이력이 있는지 확인합니다.
    이 함수는 하위 호환성을 위해 유지됩니다.

    Returns:
        bool: 작업 이력이 있으면 True, 없으면 False
    """
    # 강제 실행 모드인 경우 이력 확인 없이 실행
    global _force_run
    if _force_run:
        print("강제 실행 모드: 작업 이력 확인을 건너뜁니다.")
        return True

    # 모든 키워드 조합을 생성해서 적어도 하나의 파일이 있는지 확인
    try:
        keyword_combinations = generate_keyword_combinations()
        for keyword_combo in keyword_combinations:
            search_query = combine_keywords(keyword_combo)
            if check_keyword_work_history(search_query):
                print("일부 키워드에 대한 작업 이력이 확인되었습니다.")
                return True
    except Exception as e:
        print(f"작업 이력 확인 중 오류 발생: {e}")

    # 데이터 디렉토리 존재 여부 확인
    data_dir = config.DATA_DIR
    if not os.path.exists(data_dir):
        print(f"데이터 디렉토리가 존재하지 않습니다: {data_dir}")
        return False

    print("작업 이력이 없습니다.")
    return False


def check_keyword_work_history(search_query):
    """
    특정 키워드에 대한 크롤링 작업 이력이 있는지 확인합니다.

    Args:
        search_query: 확인할 검색어

    Returns:
        bool: 해당 키워드에 대한 작업 이력이 있으면 True, 없으면 False
    """
    # 강제 실행 모드인 경우 항상 False 반환 (모든 키워드 재크롤링)
    global _force_run, _skip_existing

    # 기존 키워드 건너뛰기 옵션이 비활성화된 경우
    if not _skip_existing:
        return False

    # 데이터 디렉토리 경로
    data_dir = config.DATA_DIR

    # 데이터 디렉토리가 없는 경우
    if not os.path.exists(data_dir):
        return False

    # 키워드별 파일명 생성
    output_filename = config.OUTPUT_FILE_NAME_TEMPLATE.format(
        search_query.replace("@", "")
    )
    keyword_file = os.path.join(data_dir, output_filename)

    # 파일 존재 여부 확인
    if not os.path.exists(keyword_file):
        return False

    # 파일은 있지만 내용이 비어있는지 확인
    try:
        with open(keyword_file, "r", encoding="utf-8") as f:
            # 첫 줄은 헤더이므로 두 줄 이상 있어야 데이터가 있는 것으로 간주
            lines = f.readlines()
            if len(lines) <= 1:  # 헤더만 있거나 비어있는 경우
                return False

        return True

    except Exception as e:
        print(f"파일 확인 중 오류 발생: {e}")
        return False


if __name__ == "__main__":
    main()
