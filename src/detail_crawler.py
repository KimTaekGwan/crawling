"""
Module for crawling detailed information from previously crawled URLs.
"""

import csv
import os
import re
import time
import concurrent.futures
from typing import Dict, List, Optional, Set
from playwright.sync_api import sync_playwright, Page
import src.config as config
from src.scraper import initialize_browser, close_browser
from src.storage import ensure_data_dir
from src.db_storage import (
    initialize_db,
    get_processed_urls as get_processed_urls_db,
    save_to_db,
    read_urls_from_db,
)

# 병렬 처리 수 설정
_parallel_count = 4

# 데이터베이스 파일명 설정
_db_filename = "crawler_data.db"


def set_parallel_count(count=4):
    """
    병렬 처리 수를 설정합니다.

    Args:
        count: 동시에 처리할 URL 수 (기본값: 4)
    """
    global _parallel_count
    _parallel_count = max(1, count)  # 최소 1 이상
    print(f"병렬 처리 수가 {_parallel_count}로 설정되었습니다.")


def read_urls_from_csv(filename: str) -> List[Dict[str, str]]:
    """
    CSV 파일에서 이전에 크롤링한 URL 및 정보를 읽어옵니다.
    (CSV 호환성을 위해 유지)

    Args:
        filename: 읽을 CSV 파일 이름

    Returns:
        URL 정보가 담긴 딕셔너리 리스트
    """
    urls = []
    filepath = os.path.join(config.DATA_DIR, filename)

    if not os.path.exists(filepath):
        print(f"파일이 존재하지 않습니다: {filepath}")
        return urls

    try:
        with open(filepath, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                urls.append(row)
    except Exception as e:
        print(f"파일 읽기 오류: {e}")

    return urls


def get_processed_urls(output_filename: str) -> Set[str]:
    """
    이미 처리된 URL 목록을 가져옵니다.
    (SQLite DB에서 읽습니다)

    Args:
        output_filename: 결과 CSV 파일명 (레거시 호환성 유지, 무시됨)

    Returns:
        이미 처리된 URL 집합 (Set)
    """
    return get_processed_urls_db(_db_filename)


def extract_footer_info(page: Page) -> Dict[str, str]:
    """
    웹페이지의 푸터에서 기업 정보를 추출합니다.

    Args:
        page: Playwright 페이지 객체

    Returns:
        추출된 기업 정보가 담긴 딕셔너리
    """
    info = {"Company": "", "PhoneNumber": "", "Email": "", "Address": ""}

    try:
        # 푸터 영역이 존재하는지 확인
        footer_selector = (
            "#main > div.footer._footer > div.section_footer > div > div.area_info"
        )
        if not page.query_selector(footer_selector):
            print("푸터 영역을 찾을 수 없습니다.")
            return info

        # 정보 목록 확인
        list_items = page.query_selector_all(f"{footer_selector} ul.list_info > li")
        if not list_items:
            print("푸터 정보 목록을 찾을 수 없습니다.")
            return info

        # 각 항목 처리
        for item in list_items:
            text = item.inner_text().strip()

            # 전화번호 추출
            if "전화번호" in text:
                phone_match = re.search(r"전화번호\s*:?\s*([0-9\-]+)", text)
                if phone_match:
                    info["PhoneNumber"] = phone_match.group(1)

            # 이메일 추출
            elif "이메일" in text:
                email_match = re.search(
                    r"이메일\s*:?\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",
                    text,
                )
                if email_match:
                    info["Email"] = email_match.group(1)

            # 주소 추출 (주소 형태를 가진 텍스트로 판단)
            elif (
                "광역시" in text
                or "시 " in text
                or "군 " in text
                or "도 " in text
                or "구 " in text
            ):
                if "사업자등록번호" not in text and "연락처" not in text:
                    info["Address"] = text

            # 기업명 추출 (첫 번째 항목으로 가정)
            elif (
                info["Company"] == ""
                and "사업자등록번호" not in text
                and "대표" not in text
            ):
                if len(text) < 30:  # 길이 제한으로 주소가 아닌 항목 구분
                    info["Company"] = text

    except Exception as e:
        print(f"푸터 정보 추출 중 오류: {e}")

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
        print(f"톡톡 링크 추출 중 오류: {e}")

    return talk_link


def crawl_detail_page(url: str) -> Dict[str, str]:
    """
    특정 URL에서 상세 정보를 크롤링합니다.

    Args:
        url: 크롤링할 URL

    Returns:
        추출된 상세 정보가 담긴 딕셔너리
    """
    details = {
        "URL": url,
        "Company": "",
        "PhoneNumber": "",
        "Email": "",
        "Address": "",
        "TalkLink": "",
    }

    # 브라우저 초기화
    playwright, browser, context, page = initialize_browser()

    try:
        # URL로 이동
        print(f"URL 접속 중: {url}")
        page.goto(url, timeout=30000)

        # 페이지 로딩 대기
        page.wait_for_load_state("networkidle", timeout=10000)

        # 푸터 정보 추출
        footer_info = extract_footer_info(page)
        details.update(footer_info)

        # 톡톡 링크 추출
        details["TalkLink"] = extract_talk_link(page)

        print(f"크롤링 완료: {url}")
        print(f"- 기업명: {details['Company']}")
        print(f"- 전화번호: {details['PhoneNumber']}")
        print(f"- 이메일: {details['Email']}")
        print(f"- 주소: {details['Address']}")
        print(f"- 톡톡링크: {details['TalkLink']}")

    except Exception as e:
        print(f"상세 페이지 크롤링 중 오류: {url} - {e}")

    finally:
        # 브라우저 종료
        close_browser(playwright, browser, context)

    return details


def save_intermediate_results(
    results: List[Dict[str, str]], output_filename: str, headers: List[str] = None
) -> None:
    """
    중간 결과를 저장합니다. (SQLite DB에 저장)

    Args:
        results: 저장할 결과 데이터
        output_filename: 저장할 파일명 (레거시 호환성 유지, CSV 파일도 생성)
        headers: CSV 헤더 (없으면 results의 키를 사용)
    """
    if not results:
        print("저장할 결과가 없습니다.")
        return

    # SQLite DB에 저장
    saved_count = save_to_db(results, _db_filename)
    print(f"중간 결과 {saved_count}개 항목이 SQLite 데이터베이스에 저장되었습니다.")

    # 이전 버전과의 호환성을 위해 CSV도 생성
    if headers is None:
        headers = list(results[0].keys())

    # 파일을 덮어쓰기 모드로 직접 저장
    ensure_data_dir()
    filepath = os.path.join(config.DATA_DIR, output_filename)

    try:
        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(results)

        print(
            f"중간 결과 {len(results)}개 항목이 {output_filename}에도 백업되었습니다."
        )
    except Exception as e:
        print(f"CSV 결과 저장 중 오류 발생: {e}")


def process_url(item, i, total_items):
    """
    단일 URL에 대한 처리를 수행합니다.
    병렬 처리를 위해 분리된 함수입니다.

    Args:
        item: 처리할 항목 정보
        i: 항목 인덱스
        total_items: 전체 항목 수

    Returns:
        Dict: 상세 정보를 포함한 결과 딕셔너리
    """
    print(f"\n항목 {i+1}/{total_items} 처리 중...")
    url = item["URL"]

    # 기존 정보 복사
    detail_item = item.copy()

    # 상세 정보 크롤링
    start_time = time.time()
    details = crawl_detail_page(url)
    elapsed_time = time.time() - start_time

    # 기존 정보와 병합
    detail_item.update(details)

    # 처리 결과 로그
    print(f"항목 {i+1}/{total_items} 완료 (소요시간: {elapsed_time:.1f}초)")
    print(f"- 기업명: {detail_item['Company']}")
    print(f"- 전화번호: {detail_item['PhoneNumber']}")
    print(f"- 이메일: {detail_item['Email']}")

    # 과도한 요청 방지를 위한 딜레이
    time.sleep(3)

    return detail_item


def crawl_details_from_csv(
    input_filename: str,
    output_filename: str,
    save_interval: int = 10,
    resume: bool = True,
):
    """
    CSV 파일에서 URL 목록을 읽어와 각 URL의 상세 정보를 크롤링합니다.
    결과는 SQLite DB에 저장됩니다.

    Args:
        input_filename: 입력 CSV 파일명
        output_filename: 결과를 저장할 CSV 파일명 (백업용)
        save_interval: 중간 저장 간격 (기본값: 10개 URL마다)
        resume: 이전에 처리된 URL은 건너뛸지 여부 (기본값: True)
    """
    print(f"CSV 파일에서 URL 읽기: {input_filename}")
    items = read_urls_from_csv(input_filename)

    if not items:
        print("크롤링할 URL이 없습니다.")
        return

    # 데이터베이스 초기화
    initialize_db(_db_filename)

    # 이미 처리된 URL 목록 가져오기
    processed_urls = set()
    if resume:
        processed_urls = get_processed_urls(_db_filename)

    # 처리해야 할 URL 필터링
    items_to_process = []
    for item in items:
        if "URL" in item and item["URL"]:
            if not resume or item["URL"] not in processed_urls:
                items_to_process.append(item)

    total_items = len(items_to_process)
    print(f"총 {total_items}개의 URL에 대해 상세 정보 크롤링을 시작합니다.")
    print(f"병렬 처리 수: {_parallel_count}")

    if not items_to_process:
        print("처리할 URL이 없습니다. 종료합니다.")
        return

    # 기존 결과를 URL을 키로 하는 딕셔너리로 변환
    results_dict = {}
    if resume and processed_urls:
        existing_results = read_urls_from_db(_db_filename)
        for item in existing_results:
            if "url" in item and item["url"]:
                # SQLite에서는 컬럼명이 소문자로 저장되므로 일관성을 위해 변환
                item_with_uppercase_url = item.copy()
                item_with_uppercase_url["URL"] = item["url"]
                results_dict[item["url"]] = item_with_uppercase_url

    # 크롤링 시작 시간 기록
    start_time = time.time()

    # 중간 저장을 위한 동기화 변수
    completed_count = 0
    all_processed_items = 0

    # 스레드 안전한 카운터 및 락 생성
    from threading import Lock

    result_lock = Lock()

    try:
        # 병렬 처리 시작
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=_parallel_count
        ) as executor:
            # 작업 제출
            futures = {
                executor.submit(process_url, item, i, total_items): i
                for i, item in enumerate(items_to_process)
            }

            # 작업 완료 대기 및 결과 수집
            for future in concurrent.futures.as_completed(futures):
                try:
                    # 결과 수집
                    result = future.result()
                    url = result["URL"]

                    # 스레드 안전하게 결과 추가
                    with result_lock:
                        results_dict[url] = result
                        completed_count += 1
                        all_processed_items += 1

                        # 일정 간격마다 중간 결과 저장
                        if (
                            completed_count % save_interval == 0
                            or all_processed_items == total_items
                        ):
                            # 딕셔너리에서 리스트로 변환하여 저장
                            results_list = list(results_dict.values())
                            headers = (
                                list(results_list[0].keys()) if results_list else []
                            )
                            save_intermediate_results(
                                results_list, output_filename, headers
                            )

                            # 진행 상황 및 예상 완료 시간 계산
                            elapsed_time = time.time() - start_time
                            avg_time_per_item = elapsed_time / all_processed_items
                            remaining_items = total_items - all_processed_items
                            estimated_remaining_time = (
                                avg_time_per_item * remaining_items
                            )

                            print(f"\n===== 중간 저장 완료 =====")
                            print(
                                f"진행 상황: {all_processed_items}/{total_items} ({all_processed_items/total_items*100:.1f}%)"
                            )
                            print(f"경과 시간: {elapsed_time/60:.1f}분")
                            print(
                                f"예상 남은 시간: {estimated_remaining_time/60:.1f}분"
                            )
                            print(f"현재까지 총 {len(results_dict)}개 URL 정보 저장됨")
                            print(f"===========================\n")

                            # 완료된 항목 카운터 초기화 (save_interval 단위로만 사용됨)
                            completed_count = 0

                except Exception as e:
                    print(f"URL 처리 중 오류 발생: {e}")

    except KeyboardInterrupt:
        print("\n사용자에 의해 크롤링이 중단되었습니다.")
        # 중단된 시점까지의 결과 저장
        if results_dict:
            results_list = list(results_dict.values())
            headers = list(results_list[0].keys()) if results_list else []
            save_intermediate_results(results_list, output_filename, headers)

    except Exception as e:
        print(f"\n크롤링 중 오류 발생: {e}")
        # 오류 발생 시점까지의 결과 저장
        if results_dict:
            results_list = list(results_dict.values())
            headers = list(results_list[0].keys()) if results_list else []
            save_intermediate_results(results_list, output_filename, headers)

    # 최종 결과 출력
    total_time = time.time() - start_time
    total_processed = all_processed_items
    print(f"\n크롤링 완료: {total_processed}/{total_items} 항목 처리됨.")
    print(f"총 소요 시간: {total_time:.1f}초 ({total_time/60:.1f}분)")
    if results_dict:
        print(
            f"결과가 SQLite DB와 {output_filename}에 저장되었습니다. 총 {len(results_dict)}개 URL 정보"
        )


def main():
    """메인 함수"""
    # 모든 검색어에 대한 통합 파일에서 URL 읽기
    input_filename = config.ALL_DATA_FILE_NAME
    output_filename = "details_" + config.ALL_DATA_FILE_NAME

    # 기본 중간 저장 간격 (10 항목마다)
    save_interval = 10

    # 작업 모드 선택
    resume_mode = True

    # 작업 모드 출력
    mode_description = "이어서 작업" if resume_mode else "처음부터 작업"
    print(
        f"상세 정보 크롤링을 {mode_description}합니다. 중간 저장 간격: {save_interval}개 URL마다"
    )

    # 크롤링 시작
    crawl_details_from_csv(input_filename, output_filename, save_interval, resume_mode)


if __name__ == "__main__":
    main()
