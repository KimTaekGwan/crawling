"""
URL 상세 정보 크롤링 모듈
"""

import concurrent.futures
import json
import re
import time
from datetime import datetime
from typing import Dict, List, Set, Optional, Tuple

from sqlalchemy.orm import Session
from playwright.sync_api import sync_playwright, Page

from db.database import SessionLocal
from db.models import CrawlTask, SearchResult, DetailResult
import src.config as config
from src.scraper import initialize_browser, close_browser

# 스레드별로 데이터베이스 세션을 관리하기 위한 딕셔너리
_db_sessions = {}


def get_db_session() -> Session:
    """
    현재 스레드를 위한 데이터베이스 세션을 반환합니다.
    """
    import threading

    thread_id = threading.get_ident()

    if thread_id not in _db_sessions:
        _db_sessions[thread_id] = SessionLocal()

    return _db_sessions[thread_id]


def close_db_sessions():
    """
    모든 데이터베이스 세션을 닫습니다.
    """
    for session in _db_sessions.values():
        session.close()
    _db_sessions.clear()


def extract_footer_info(page: Page) -> Dict[str, str]:
    """
    웹페이지의 푸터에서 기업 정보를 추출합니다.

    Args:
        page: Playwright 페이지 객체

    Returns:
        추출된 기업 정보가 담긴 딕셔너리
    """
    info = {"company": "", "phone_number": "", "email": "", "address": ""}

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
        "url": url,
        "company": "",
        "phone_number": "",
        "email": "",
        "address": "",
        "talk_link": "",
        "success": True,
        "error_message": None,
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
        details["talk_link"] = extract_talk_link(page)

        print(f"크롤링 완료: {url}")
        print(f"- 기업명: {details['company']}")
        print(f"- 전화번호: {details['phone_number']}")
        print(f"- 이메일: {details['email']}")
        print(f"- 주소: {details['address']}")
        print(f"- 톡톡링크: {details['talk_link']}")

    except Exception as e:
        print(f"상세 페이지 크롤링 중 오류: {url} - {e}")
        details["success"] = False
        details["error_message"] = str(e)

    finally:
        # 브라우저 종료
        close_browser(playwright, browser, context)

    return details


def process_url(search_result_id: int, url: str, i: int, total: int) -> Dict[str, str]:
    """
    단일 URL에 대한 상세 정보를 크롤링하고 DB에 저장합니다.

    Args:
        search_result_id: 검색 결과 ID
        url: 처리할 URL
        i: 인덱스
        total: 전체 URL 수

    Returns:
        처리 결과
    """
    db = get_db_session()

    print(f"\n항목 {i+1}/{total} 처리 중: {url}")

    start_time = time.time()

    # 이미 처리된 URL인지 확인
    existing = db.query(DetailResult).filter(DetailResult.url == url).first()
    if existing:
        print(f"이미 처리된 URL: {url}")
        elapsed_time = time.time() - start_time
        print(f"항목 {i+1}/{total} 완료 (소요시간: {elapsed_time:.1f}초)")
        return {
            "url": url,
            "company": existing.company,
            "phone_number": existing.phone_number,
            "email": existing.email,
            "address": existing.address,
            "talk_link": existing.talk_link,
            "success": existing.success,
            "error_message": existing.error_message,
            "already_exists": True,
        }

    # URL 크롤링
    details = crawl_detail_page(url)
    elapsed_time = time.time() - start_time

    # DB에 저장
    detail_result = DetailResult(
        url=url,
        company=details["company"],
        phone_number=details["phone_number"],
        email=details["email"],
        address=details["address"],
        talk_link=details["talk_link"],
        success=details["success"],
        error_message=details["error_message"],
        processed_at=datetime.now(),
    )

    try:
        db.add(detail_result)
        db.commit()
        db.refresh(detail_result)
    except Exception as e:
        db.rollback()
        print(f"데이터베이스 저장 중 오류: {e}")
        detail_result.success = False
        detail_result.error_message = f"데이터베이스 저장 오류: {str(e)}"

    print(f"항목 {i+1}/{total} 완료 (소요시간: {elapsed_time:.1f}초)")
    print(f"- 기업명: {details['company']}")
    print(f"- 전화번호: {details['phone_number']}")
    print(f"- 이메일: {details['email']}")

    # 과도한 요청 방지를 위한 딜레이
    time.sleep(3)

    details["id"] = detail_result.id
    details["already_exists"] = False

    return details


def start_detail_crawling(
    task_id: int, parallel: int = 4, resume: bool = True, save_interval: int = 10
):
    """
    상세 정보 크롤링 작업을 시작합니다.

    Args:
        task_id: 작업 ID
        parallel: 병렬 처리 수
        resume: 이전에 처리된 URL은 건너뛸지 여부
        save_interval: 진행 상황 업데이트 간격
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

        print(f"상세 정보 크롤링 작업 시작 (작업 ID: {task_id})")
        print(f"병렬 처리 수: {parallel}")
        print(f"이어서 작업: {resume}")
        print(f"진행 상황 업데이트 간격: {save_interval}")

        # 처리할 URL 목록 가져오기
        if resume:
            # 이미 처리된 URL 제외
            processed_urls = {url[0] for url in db.query(DetailResult.url).all()}
            print(f"이미 처리된 URL: {len(processed_urls)}개")

            query = db.query(SearchResult).filter(
                ~SearchResult.url.in_(processed_urls) if processed_urls else True
            )
        else:
            # 모든 URL 처리
            query = db.query(SearchResult)

        search_results = query.all()

        urls_to_process = [
            (r.id, r.url, i, len(search_results)) for i, r in enumerate(search_results)
        ]

        total_urls = len(urls_to_process)
        task.total_urls = total_urls
        db.commit()

        print(f"총 {total_urls}개의 URL에 대해 상세 정보 크롤링을 시작합니다.")

        if not urls_to_process:
            print("처리할 URL이 없습니다. 작업을 종료합니다.")
            task.status = "completed"
            task.completed_at = datetime.now()
            db.commit()
            return

        # 크롤링 시작 시간 기록
        start_time = time.time()
        processed_count = 0

        try:
            # 병렬 처리 시작
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=parallel
            ) as executor:
                # 작업 제출
                futures = {
                    executor.submit(process_url, result_id, url, i, total_urls): url
                    for result_id, url, i, _ in urls_to_process
                }

                # 작업 완료 대기
                for i, future in enumerate(concurrent.futures.as_completed(futures)):
                    try:
                        result = future.result()
                        processed_count += 1

                        # 진행 상황 업데이트
                        if (
                            processed_count % save_interval == 0
                            or processed_count == total_urls
                        ):
                            # 작업 정보 업데이트
                            task.processed_urls = processed_count
                            db.commit()

                            # 진행 상황 및 예상 완료 시간 계산
                            elapsed_time = time.time() - start_time
                            avg_time_per_item = elapsed_time / processed_count
                            remaining_items = total_urls - processed_count
                            estimated_remaining_time = (
                                avg_time_per_item * remaining_items
                            )

                            print(f"\n===== 진행 상황 =====")
                            print(
                                f"처리: {processed_count}/{total_urls} ({processed_count/total_urls*100:.1f}%)"
                            )
                            print(f"경과 시간: {elapsed_time/60:.1f}분")
                            print(
                                f"예상 남은 시간: {estimated_remaining_time/60:.1f}분"
                            )
                            print(f"===================\n")

                    except Exception as e:
                        print(f"URL 처리 중 오류 발생: {e}")

        except KeyboardInterrupt:
            print("\n사용자에 의해 크롤링이 중단되었습니다.")
            task.status = "interrupted"

        except Exception as e:
            print(f"\n크롤링 중 오류 발생: {e}")
            task.status = "failed"
            task.error_message = str(e)

        finally:
            # 모든 데이터베이스 세션 정리
            close_db_sessions()

        # 작업 완료
        total_time = time.time() - start_time
        print(f"\n크롤링 완료: {processed_count}/{total_urls} 항목 처리됨.")
        print(f"총 소요 시간: {total_time:.1f}초 ({total_time/60:.1f}분)")

        task.processed_urls = processed_count
        task.completed_at = datetime.now()
        if task.status == "running":  # 중단되지 않았을 경우만
            task.status = "completed"
        db.commit()

    except Exception as e:
        print(f"작업 처리 중 오류 발생: {e}")
        task.status = "failed"
        task.error_message = str(e)
        db.commit()

    finally:
        # 세션 종료
        db.close()
