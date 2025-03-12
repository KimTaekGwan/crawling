"""
네이버 검색 결과 크롤링 모듈
"""

import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session
from playwright.sync_api import sync_playwright, Page

from db.database import SessionLocal
from db.models import CrawlTask, SearchResult
import src.config as config
from src.scraper import initialize_browser, close_browser
from src.main import generate_keyword_combinations, combine_keywords


def scrape_page(
    page: Page, keyword: str, page_num: int, db: Session
) -> List[Dict[str, str]]:
    """
    특정 페이지의 검색 결과를 스크래핑합니다.

    Args:
        page: Playwright 페이지 객체
        keyword: 검색 키워드
        page_num: 페이지 번호
        db: 데이터베이스 세션

    Returns:
        스크래핑된 결과 목록
    """
    search_url = f"https://search.naver.com/search.naver?query={keyword}&start={(page_num - 1) * 10 + 1}"
    print(f"URL 접속 중: {search_url}")

    page.goto(search_url, timeout=30000)

    # 페이지 로딩 대기
    page.wait_for_load_state("networkidle", timeout=10000)

    results = []

    # 검색 결과 아이템 선택자
    items = page.query_selector_all("li.bx._svp_item")

    if not items:
        print(f"페이지 {page_num}에서 결과를 찾을 수 없습니다.")
        return results

    print(f"페이지 {page_num}에서 {len(items)}개의 결과를 찾았습니다.")

    # 각 결과 아이템 처리
    for i, item in enumerate(items):
        # 제목과 URL 추출
        title_link = item.query_selector("div.total_tit a")

        if not title_link:
            continue

        title = title_link.inner_text().strip()
        url = title_link.get_attribute("href")

        if not url:
            continue

        # URL 유효성 검사
        if not url.startswith("http"):
            continue

        # 이미 처리된 URL인지 확인
        existing = db.query(SearchResult).filter(SearchResult.url == url).first()
        if existing:
            print(f"이미 존재하는 URL: {url}")
            continue

        # 결과 추가
        print(f"결과 {i+1}: {title} - {url}")

        result = {"url": url, "title": title, "keyword": keyword}

        # 데이터베이스에 저장
        search_result = SearchResult(
            url=url, title=title, keyword=keyword, created_at=datetime.now()
        )

        db.add(search_result)
        results.append(result)

    # 한 번에 커밋
    db.commit()

    return results


def scrape_all_pages_for_query(
    keyword: str, start_page: int, end_page: int, db: Session, force: bool = False
) -> List[Dict[str, str]]:
    """
    지정된 범위의 모든 페이지에서 검색 결과를 스크래핑합니다.

    Args:
        keyword: 검색 키워드
        start_page: 시작 페이지 번호
        end_page: 종료 페이지 번호
        db: 데이터베이스 세션
        force: 이미 크롤링한 키워드도 강제로 다시 크롤링할지 여부

    Returns:
        모든 페이지에서 스크래핑된 결과 목록
    """
    # 이미 크롤링 된 키워드인지 확인
    if (
        not force
        and db.query(SearchResult).filter(SearchResult.keyword == keyword).first()
    ):
        print(f"키워드 '{keyword}'는 이미 크롤링되었습니다. 건너뜁니다.")
        return []

    # 브라우저 초기화
    playwright, browser, context, page = initialize_browser()

    all_results = []

    try:
        print(f"키워드 '{keyword}'에 대한 크롤링 시작 (페이지 {start_page}-{end_page})")

        # 각 페이지 크롤링
        for page_num in range(start_page, end_page + 1):
            page_results = scrape_page(page, keyword, page_num, db)
            all_results.extend(page_results)

            # 과도한 요청 방지를 위한 딜레이
            if page_num < end_page:
                print(f"다음 페이지로 이동하기 전 대기 중...")
                time.sleep(3)

    except Exception as e:
        print(f"크롤링 중 오류 발생: {e}")

    finally:
        # 브라우저 종료
        close_browser(playwright, browser, context)

    return all_results


def start_search_crawling(
    task_id: int, parallel: int = 4, force: bool = False, skip_existing: bool = True
):
    """
    검색 크롤링 작업을 시작합니다.

    Args:
        task_id: 작업 ID
        parallel: 병렬 처리 수(현재는 사용하지 않음)
        force: 이미 크롤링한 키워드도 강제로 다시 크롤링할지 여부
        skip_existing: 이미 존재하는 URL 건너뛰기 여부
    """
    db = SessionLocal()

    try:
        # 작업 정보 업데이트
        task = db.query(CrawlTask).filter(CrawlTask.id == task_id).first()
        if not task:
            print(f"작업 ID {task_id}를 찾을 수 없습니다.")
            return

        task.status = "running"
        task.started_at = datetime.now()
        db.commit()

        print(f"검색 크롤링 작업 시작 (작업 ID: {task_id})")
        print(f"강제 크롤링: {force}")

        # 키워드 조합 생성
        keyword_combos = generate_keyword_combinations()
        total_combos = len(keyword_combos)
        task.total_urls = total_combos
        db.commit()

        print(f"총 {total_combos}개의 키워드 조합이 생성되었습니다.")

        # 크롤링 시작 시간 기록
        start_time = time.time()
        processed_count = 0

        for i, combo in enumerate(keyword_combos):
            # 키워드 조합을 문자열로 변환
            keyword = combine_keywords(combo)

            print(f"\n조합 {i+1}/{total_combos} 처리 중: {keyword}")

            # 특정 페이지 범위에서 검색 결과 크롤링
            results = scrape_all_pages_for_query(
                keyword=keyword,
                start_page=1,  # 첫 페이지부터
                end_page=config.MAX_SEARCH_PAGES,
                db=db,
                force=force,
            )

            processed_count += 1
            task.processed_urls = processed_count
            db.commit()

            # 진행 상황 및 예상 완료 시간 계산
            elapsed_time = time.time() - start_time
            avg_time_per_item = elapsed_time / processed_count
            remaining_items = total_combos - processed_count
            estimated_remaining_time = avg_time_per_item * remaining_items

            print(f"\n===== 진행 상황 =====")
            print(
                f"처리: {processed_count}/{total_combos} ({processed_count/total_combos*100:.1f}%)"
            )
            print(f"경과 시간: {elapsed_time/60:.1f}분")
            print(f"예상 남은 시간: {estimated_remaining_time/60:.1f}분")
            print(f"===================\n")

        # 작업 완료
        total_time = time.time() - start_time
        print(f"\n크롤링 완료: {processed_count}/{total_combos} 키워드 조합 처리됨.")
        print(f"총 소요 시간: {total_time:.1f}초 ({total_time/60:.1f}분)")

        task.processed_urls = processed_count
        task.completed_at = datetime.now()
        task.status = "completed"
        db.commit()

    except KeyboardInterrupt:
        print("\n사용자에 의해 크롤링이 중단되었습니다.")
        task.status = "interrupted"
        db.commit()

    except Exception as e:
        print(f"작업 처리 중 오류 발생: {e}")
        task.status = "failed"
        task.error_message = str(e)
        db.commit()

    finally:
        # 세션 종료
        db.close()
