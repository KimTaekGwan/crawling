"""
Main module to orchestrate the Naver search result scraping process using Naver Search API.
"""

import time
import itertools
import os
import logging
import concurrent.futures
from typing import List, Dict, Optional, Tuple
import src.config as config
from src.naver_api import (
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

# 병렬 처리 수 설정 (API 호출의 경우 낮게 설정)
_parallel_count = config.SEARCH_PARALLEL_COUNT

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
            "강제 실행 모드가 활성화되었습니다. 작업 이력이 없어도 수집을 실행합니다."
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
            "모든 키워드 재수집 모드가 활성화되었습니다. 이미 수집된 키워드도 다시 수집합니다."
        )


def set_parallel_count(count=3):
    """
    병렬 처리 수를 설정합니다.

    Args:
        count: 동시에 처리할 검색어 수 (기본값: 3)
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


def scrape_page(api_client, search_query: str, page_num: int) -> Tuple[bool, int]:
    """
    API를 통해 검색 결과 한 페이지를 가져옵니다.

    Args:
        api_client: API 클라이언트 객체
        search_query: 검색어
        page_num: 페이지 번호

    Returns:
        Tuple[bool, int]: (성공 여부, 결과 개수)
    """
    logger.info(f"API로 페이지 {page_num} 검색 중 '{search_query}'...")
    results_count = 0
    success = False

    try:
        # API로 페이지 요청 유효성 검사 및 설정
        page_valid = get_search_page(api_client, search_query, page_num)

        if page_valid:
            # API로 결과 가져오기
            results = scrape_search_results(api_client)
            results_count = len(results)
            success = True  # API 호출 자체는 성공했을 수 있음 (결과가 0개여도)

            if results_count > 0:
                # 데이터 저장
                save_page_data(search_query, page_num, results)
                logger.info(
                    f"검색어 '{search_query}'의 페이지 {page_num} 수집 완료, {results_count}개 결과 발견 및 저장."
                )
            else:
                # 결과가 0개인 경우
                logger.info(
                    f"검색어 '{search_query}'의 페이지 {page_num} 수집 완료, 결과 없음."
                )

        else:
            logger.warning(
                f"검색어 '{search_query}'의 페이지 {page_num} 요청 유효성 검사 실패 또는 API 제한 도달"
            )
            # success는 False로 유지

    except Exception as e:
        logger.error(f"검색어 '{search_query}'의 페이지 {page_num} 처리 중 오류: {e}")
        success = False  # 에러 발생 시 실패로 간주

    return success, results_count


def scrape_all_pages_for_query(search_query: str):
    """
    특정 검색어에 대해 설정된 범위의 모든 페이지를 검색합니다.
    하나의 API 클라이언트 인스턴스를 사용합니다.

    Args:
        search_query: 검색어
    """
    START_PAGE = 1
    END_PAGE = 10
    logger.info(f"'{search_query}' 검색 결과 수집 시작")
    logger.info(f"수집할 페이지: {START_PAGE}~{END_PAGE}")

    # API 클라이언트 초기화
    api_client, _, _, _ = initialize_browser()

    try:
        # ----- 수정된 부분 시작 -----
        for page_num in range(START_PAGE, END_PAGE + 1):
            # 페이지 검색 및 결과 개수 확인
            success, results_count = scrape_page(api_client, search_query, page_num)

            # API 호출 실패 또는 결과 없음 시 해당 키워드 처리 중단
            # 또는 결과 수가 기대한 수(display count)보다 적을 경우 중단
            expected_count = config.NAVER_API_DISPLAY_COUNT
            if not success or results_count < expected_count:
                log_level = logging.WARNING if not success else logging.INFO
                reason = (
                    "오류 발생"
                    if not success
                    else f"결과 부족 ({results_count}/{expected_count})"
                )
                logger.log(
                    log_level,
                    f"검색어 '{search_query}'의 페이지 {page_num}에서 {reason}. 해당 키워드 처리 중단.",
                )
                break  # 현재 키워드의 페이지 반복 중단

            # API 호출 간 딜레이 (성공하고 결과가 있을 때만)
            # 위 조건문에서 이미 결과 수가 충분한 경우만 여기까지 오므로, END_PAGE 체크는 불필요할 수 있음
            # 하지만 명확성을 위해 유지하거나, 필요시 제거 가능
            if page_num < config.END_PAGE:
                time.sleep(config.NAVER_API_CALL_DELAY)
        # ----- 수정된 부분 끝 -----

    except Exception as e:
        logger.error(f"검색어 '{search_query}' 처리 중 오류: {e}")

    finally:
        # API 클라이언트 종료
        close_browser(api_client, None, None)

    logger.info(f"검색어 '{search_query}' 처리 완료.")


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
    if check_keyword_work_history([search_query]) and not _force_run:
        return f"[{i+1}/{total_combos}] 검색어 '{search_query}'({format_combo_for_display(keyword_combo)}) - 이미 수집됨, 건너뜀"

    # 시작 로그
    logger.info(f"{'='*50}")
    logger.info(
        f"[{i+1}/{total_combos}] 검색어 조합: {format_combo_for_display(keyword_combo)}"
    )
    logger.info(f"변환된 검색어: {search_query}")
    logger.info(f"{'='*50}")

    # 해당 검색어로 모든 페이지 수집
    start_time = time.time()
    scrape_all_pages_for_query(search_query)
    elapsed_time = time.time() - start_time

    return f"[{i+1}/{total_combos}] 검색어 '{search_query}' 완료 (소요시간: {elapsed_time:.1f}초)"


def check_keyword_work_history(search_queries: List[str]) -> set:
    """
    주어진 검색어 목록 중 데이터베이스에 이미 수집 이력이 있는 키워드를 확인합니다.
    SQLite 데이터베이스에서 IN 절을 사용하여 한 번에 조회합니다.

    Args:
        search_queries: 확인할 검색어 리스트

    Returns:
        set: 데이터베이스에 존재하는 검색어들의 집합
    """
    # 기존 키워드 건너뛰기 옵션이 비활성화된 경우 빈 집합 반환
    if not _skip_existing or not search_queries:
        return set()

    existing_keywords = set()
    conn = get_db_connection(DB_FILENAME)
    try:
        cursor = conn.cursor()
        # 한 번에 처리할 최대 IN 절 크기 (SQLite 제한 고려)
        chunk_size = 900
        for i in range(0, len(search_queries), chunk_size):
            chunk = search_queries[i : i + chunk_size]
            placeholders = ", ".join("?" * len(chunk))
            query = f"SELECT DISTINCT keyword FROM websites WHERE keyword IN ({placeholders})"
            cursor.execute(query, chunk)
            results = cursor.fetchall()
            for row in results:
                existing_keywords.add(row["keyword"])

    except Exception as e:
        logger.error(f"키워드 작업 이력 확인 중 오류: {e}")
        # 오류 발생 시 안전하게 빈 집합 반환 (또는 다른 처리 방식 선택 가능)
        return set()

    finally:
        if conn:
            conn.close()

    return existing_keywords


def main():
    """메인 진입점입니다."""
    # 환경 변수 확인
    if not os.getenv("NAVER_CLIENT_ID") and not hasattr(config, "NAVER_API_CLIENT_ID"):
        logger.error(
            "NAVER_CLIENT_ID 환경 변수 또는 config.py의 NAVER_API_CLIENT_ID 설정이 필요합니다."
        )
        return

    if not os.getenv("NAVER_CLIENT_SECRET") and not hasattr(
        config, "NAVER_API_CLIENT_SECRET"
    ):
        logger.error(
            "NAVER_CLIENT_SECRET 환경 변수 또는 config.py의 NAVER_API_CLIENT_SECRET 설정이 필요합니다."
        )
        return

    # 데이터베이스 초기화
    initialize_db(DB_FILENAME)

    # 모든 키워드 조합 생성
    keyword_combinations = generate_keyword_combinations()
    total_combos = len(keyword_combinations)
    logger.info(f"총 {total_combos}개의 검색어 조합이 생성되었습니다.")

    # 모든 잠재적 검색어 생성
    all_search_queries = []
    for combo in keyword_combinations:
        query = combine_keywords(combo)
        if query:
            all_search_queries.append(query)

    # 이미 처리된 키워드 한 번에 확인
    existing_keywords = set()
    if _skip_existing and not _force_run:
        logger.info("데이터베이스에서 기존 수집 이력 확인 중...")
        existing_keywords = check_keyword_work_history(all_search_queries)
        logger.info(f"확인 완료. 기존 수집 이력 {len(existing_keywords)}건 발견.")

    # 수집할 키워드 목록 준비
    tasks = []
    skipped_count = 0
    for i, keyword_combo in enumerate(keyword_combinations):
        search_query = combine_keywords(keyword_combo)
        if not search_query:
            continue

        # 작업 이력 확인하여 건너뛸지 결정
        if not _force_run and search_query in existing_keywords:
            # logger.info(
            #     f"[{i+1}/{total_combos}] 검색어 '{search_query}' - 이미 수집됨, 건너뜀"
            # ) # 로그가 너무 많아질 수 있으므로 주석 처리
            skipped_count += 1
            continue

        # 작업 목록에 추가
        tasks.append((i, keyword_combo, total_combos))

    # 건너뛴 개수 로그 출력
    if skipped_count > 0:
        logger.info(f"{skipped_count}개의 검색어는 이미 수집되어 건너뜀니다.")

    # 실제 수집할 작업 수 출력
    logger.info(f"실제 수집할 검색어: {len(tasks)}개")

    if not tasks:
        logger.info("수집할 검색어가 없습니다. 종료합니다.")
        return

    # 중간 요약 정보
    logger.info(f"총 {total_combos}개의 검색어 조합이 생성되었습니다.")
    logger.info(f"병렬 처리 수: {_parallel_count}")
    logger.info(f"실제 수집할 검색어: {len(tasks)}개")
    logger.info(f"API 호출 제한: 하루 {config.NAVER_API_DAILY_LIMIT}회")
    logger.info(f"한 번에 요청할 결과 수: {config.NAVER_API_DISPLAY_COUNT}개")

    # 사용자 입력 받기
    user_input = input("계속 진행하시겠습니까? (y/n): ")
    if user_input != "y":
        logger.info("프로그램을 종료합니다.")
        return

    # 병렬 처리 시작
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
                logger.info(result)
            except Exception as e:
                logger.error(f"작업 처리 중 오류 발생: {e}")

    # 실행 시간 계산
    total_time = time.time() - start_time
    logger.info(f"모든 검색어 조합에 대한 수집이 완료되었습니다.")
    logger.info(f"총 소요 시간: {total_time:.1f}초 ({total_time/60:.1f}분)")


if __name__ == "__main__":
    main()
