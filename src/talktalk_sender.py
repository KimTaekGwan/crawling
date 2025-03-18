"""
Module for automatically sending messages through Naver Talk.
"""

import os
import time
import logging
import sqlite3
import concurrent.futures
import signal
import sys
import threading
from datetime import datetime
from typing import Dict, List, Set, Tuple
from playwright.sync_api import sync_playwright, Page

import src.config as config
from src.db_storage import get_db_connection, filter_urls_by_keywords, initialize_db

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def initialize_browser(playwright):
    """
    Playwright 객체를 사용하여 브라우저를 초기화합니다.

    Args:
        playwright: Playwright 객체

    Returns:
        초기화된 브라우저 인스턴스
    """
    # 헤드리스 모드로 브라우저 실행 (GUI 없음)
    # browser = playwright.chromium.launch(headless=True)

    # 헤드리스 모드 해제 (GUI 있음)
    browser = playwright.chromium.launch(headless=False)

    return browser


def update_talk_message_status(
    conn: sqlite3.Connection, url: str, status: int, commit: bool = True
) -> None:
    """
    톡톡 메시지 전송 상태를 업데이트합니다.

    Args:
        conn: 데이터베이스 연결 객체
        url: 업데이트할 URL
        status: 새 상태 코드
        commit: 커밋 여부 (기본값: True)
    """
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE websites 
            SET talk_message_status = ?, talk_message_date = CURRENT_TIMESTAMP
            WHERE url = ?
            """,
            (status, url),
        )
        if commit:
            conn.commit()
        logger.debug(f"URL {url}의 톡톡 메시지 상태가 {status}로 업데이트되었습니다.")
    except sqlite3.Error as e:
        logger.error(f"데이터베이스 업데이트 오류: {e}")
        if commit:
            conn.rollback()


def send_talktalk_message(page, message: str) -> bool:
    """
    네이버 톡톡 페이지에서 메시지를 전송합니다.

    Args:
        page: Playwright 페이지 객체
        message: 전송할 메시지

    Returns:
        성공 여부 (True/False)
    """
    try:
        # 여러 가능한 입력 필드 선택자
        input_selectors = [
            config.TALKTALK_INPUT_SELECTOR,  # config.py에 정의된 셀렉터 (최우선)
            "textarea",  # 간단한 태그 셀렉터
            "div.textarea_input__1PNKR",
            "div[role='textbox']",
            "div.textarea_input",
            "div[contenteditable='true']",
            "div.chat_input_area textarea",
            "div.chat-textarea",
            "div.inputArea textarea",
        ]

        # 모든 가능한 선택자를 시도
        input_selector = None
        for selector in input_selectors:
            logger.info(f"입력창 선택자 시도: {selector}")
            try:
                # 짧은 타임아웃으로 빠르게 확인
                if page.wait_for_selector(selector, timeout=3000, state="visible"):
                    logger.info(f"입력창 발견: {selector}")
                    input_selector = selector
                    break
            except Exception:
                logger.debug(f"선택자 실패: {selector}")
                continue

        # 입력창을 찾지 못한 경우
        if input_selector is None:
            # 페이지 HTML 구조 로깅 (디버깅 용도)
            html_content = page.content()
            logger.warning("입력창을 찾지 못했습니다. 페이지 HTML 일부:")
            logger.warning(html_content[:500] + "...")  # 처음 500자만 로깅

            # 사용자에게 실패 알림
            logger.error(
                "입력창을 찾지 못했습니다. 페이지 구조가 변경되었을 수 있습니다."
            )
            return False

        # 메시지 입력 (여러 줄의 메시지를 위해 줄바꿈 처리)
        input_elem = page.locator(input_selector)
        lines = message.strip().split("\n")
        input_elem.fill(lines[0])

        # 두 번째 줄부터는 Shift+Enter로 줄바꿈 입력
        for line in lines[1:]:
            input_elem.press("Shift+Enter")
            time.sleep(0.2)  # 줄바꿈 입력 후 약간의 지연
            input_elem.type(line)

        # 전송 버튼 선택자도 여러 가능성 시도
        send_button_selectors = [
            config.TALKTALK_SUBMIT_SELECTOR,  # config.py에 정의된 셀렉터 (최우선)
            "button[type='submit']",
            "button.button_button__1hvQx.button_primary__2lxrm.button_large__2i1BE.button_left_icon__3ksm8",
            "button.send_button",
            "button.chatting_send_button",
            "button.send_icon",
            "button[aria-label='전송']",
            "button.button_primary",
        ]

        # 모든 가능한 전송 버튼 선택자 시도
        send_button_selector = None
        for selector in send_button_selectors:
            try:
                if page.wait_for_selector(selector, timeout=3000, state="visible"):
                    logger.info(f"전송 버튼 발견: {selector}")
                    send_button_selector = selector
                    break
            except Exception:
                continue

        # 전송 버튼을 찾지 못한 경우
        if send_button_selector is None:
            logger.error(
                "전송 버튼을 찾지 못했습니다. 페이지 구조가 변경되었을 수 있습니다."
            )
            return False

        # 버튼이 활성화될 때까지 잠시 대기
        time.sleep(1)

        # 전송 버튼 확인 및 클릭
        submit_button = page.locator(send_button_selector)

        # 버튼이 활성화되어 있는지 확인 (aria-disabled 속성 체크)
        disabled = submit_button.get_attribute("aria-disabled")
        if disabled and disabled.lower() == "true":
            logger.warning("메시지 전송 버튼이 활성화되지 않았습니다.")
            return False

        # 전송 버튼 클릭
        submit_button.click()

        # 전송 후 지연 (설정에 따라)
        if hasattr(config, "TALKTALK_BETWEEN_MSG_DELAY"):
            time.sleep(config.TALKTALK_BETWEEN_MSG_DELAY)
        else:
            time.sleep(1)  # 기본 지연값

        # 전송 확인에도 여러 선택자 시도
        confirmation_selectors = [
            ".message_time__1tXuH",
            ".message_time",
            ".message-time-stamp",
            ".sent-message-time",
            ".message-status-sent",
        ]

        # 확인 메시지 선택자 확인
        confirmation_found = False
        for selector in confirmation_selectors:
            try:
                if page.wait_for_selector(
                    selector, timeout=config.TALKTALK_MESSAGE_TIMEOUT * 1000
                ):
                    logger.info(f"전송 확인 표시 발견: {selector}")
                    confirmation_found = True
                    break
            except Exception:
                continue

        if not confirmation_found:
            # 최소 5초는 기다려서 전송 여부 확인
            time.sleep(5)
            # 전송은 시도했지만 확인 메시지는 찾지 못함
            logger.warning(
                "전송 확인 표시를 찾지 못했지만, 메시지가 전송되었을 수 있습니다."
            )

        return True
    except Exception as e:
        logger.error(f"메시지 전송 중 오류 발생: {e}")
        return False


def get_unsent_talk_links(db_filename: str) -> List[Dict[str, str]]:
    """
    메시지가 전송되지 않은 톡톡 링크 목록을 조회합니다.

    Args:
        db_filename: 데이터베이스 파일명

    Returns:
        미전송 항목 목록
    """
    conn = get_db_connection(db_filename)
    cursor = conn.cursor()

    try:
        # 컬럼 존재 여부 확인
        cursor.execute("PRAGMA table_info(websites)")
        columns = [row["name"] for row in cursor.fetchall()]

        if "talk_message_status" in columns:
            # talk_message_status 컬럼이 있는 경우
            cursor.execute(
                """
                SELECT url, talk_link, name, company, keyword 
                FROM websites 
                WHERE talk_link IS NOT NULL 
                  AND talk_link != '' 
                  AND (talk_message_status = ? OR talk_message_status IS NULL)
                """,
                (config.TALKTALK_STATUS["NOT_SENT"],),
            )
        else:
            # talk_message_status 컬럼이 없는 경우
            logger.warning(
                "talk_message_status 컬럼이 없습니다. 모든 톡톡 링크를 미전송으로 간주합니다."
            )
            cursor.execute(
                """
                SELECT url, talk_link, name, company, keyword 
                FROM websites 
                WHERE talk_link IS NOT NULL 
                  AND talk_link != ''
                """
            )

        results = [dict(row) for row in cursor.fetchall()]
        logger.info(f"{len(results)}개의 미전송 톡톡 링크를 찾았습니다.")
        return results
    except sqlite3.Error as e:
        logger.error(f"데이터베이스 조회 오류: {e}")
        return []
    finally:
        conn.close()


def display_sample_targets(items: List[Dict[str, str]], count: int = 5) -> None:
    """
    메시지 전송 대상 회사 정보를 샘플로 보여줍니다.

    Args:
        items: 메시지 전송 대상 항목 리스트
        count: 보여줄 샘플 수 (기본값: 5)
    """
    if not items:
        print("전송 대상 항목이 없습니다.")
        return

    print(f"\n[전송 대상 샘플 ({min(count, len(items))}개)]")
    print("-" * 50)

    for i, item in enumerate(items[:count]):
        name = item.get("name", "이름 없음")
        company = item.get("company", "회사명 없음")
        url = item.get("url", "링크 없음")

        print(f"{i+1}. 이름: {name}")
        print(f"   회사: {company}")
        print(f"   URL: {url}")
        if i < min(count, len(items)) - 1:
            print("-" * 25)

    if len(items) > count:
        print(f"\n... 외 {len(items) - count}개 항목")

    print("-" * 50)


class ProgressTracker:
    """
    병렬 처리 진행 상황을 추적하는 클래스
    """

    def __init__(self, total_count):
        self.total_count = total_count
        self.completed_count = 0
        self.results = {"sent": 0, "error": 0, "no_link": 0, "already_sent": 0}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.display_thread = None

    def start_display(self):
        """디스플레이 스레드 시작"""
        self.display_thread = threading.Thread(target=self._display_progress)
        self.display_thread.daemon = True
        self.display_thread.start()

    def stop_display(self):
        """디스플레이 스레드 중지"""
        if self.display_thread:
            self.stop_event.set()
            self.display_thread.join()

    def update(self, result_status):
        """결과 업데이트"""
        with self.lock:
            self.completed_count += 1
            if result_status in self.results:
                self.results[result_status] += 1

    def _display_progress(self):
        """진행 상황 표시 스레드"""
        while not self.stop_event.is_set():
            self._print_progress()
            time.sleep(0.5)

        # 마지막으로 한번 더 업데이트하여 최종 상태 표시
        self._print_progress()

    def _print_progress(self):
        """현재 진행 상황 출력"""
        with self.lock:
            completed = self.completed_count
            total = self.total_count
            percent = (completed / total * 100) if total > 0 else 0

            # 프로그레스 바 생성
            bar_length = 40
            filled_length = int(bar_length * completed // total)
            bar = "█" * filled_length + "░" * (bar_length - filled_length)

            # 결과 통계
            results_str = f"성공: {self.results['sent']}, 실패: {self.results['error']}, 링크없음: {self.results['no_link']}, 이미전송: {self.results['already_sent']}"

            # 출력 메시지
            message = f"\r진행률: |{bar}| {percent:.1f}% ({completed}/{total}) - {results_str}"

            # 터미널 너비에 맞게 출력
            sys.stdout.write(message)
            sys.stdout.flush()


def send_talktalk_messages(
    db_filename: str,
    include_keywords: List[str] = None,
    exclude_keywords: List[str] = None,
    case_sensitive: bool = False,
    parallel_count: int = None,
    message: str = None,
    maintain_session: bool = True,
    login_first: bool = True,
):
    """
    데이터베이스에서 톡톡 링크를 조회하여 메시지를 일괄 전송합니다.

    Args:
        db_filename: 데이터베이스 파일명
        include_keywords: 포함해야 할 키워드 목록
        exclude_keywords: 제외해야 할 키워드 목록
        case_sensitive: 대소문자 구분 여부
        parallel_count: 병렬 처리 수 (기본값: 설정 파일의 값)
        message: 전송할 메시지 (기본값: 설정 파일의 메시지)
        maintain_session: 세션을 유지할지 여부 (기본값: True)
        login_first: 처리 시작 전 로그인할 기회를 제공할지 여부 (기본값: True)
    """
    # Ctrl+C 핸들러 등록
    original_sigint_handler = signal.getsignal(signal.SIGINT)

    def sigint_handler(sig, frame):
        print("\n\n메시지 전송이 사용자에 의해 중단되었습니다.")
        signal.signal(signal.SIGINT, original_sigint_handler)
        sys.exit(0)

    signal.signal(signal.SIGINT, sigint_handler)

    # 기본값 설정
    if parallel_count is None:
        parallel_count = config.TALKTALK_PARALLEL_COUNT

    if message is None:
        message = config.TALKTALK_MESSAGE

    # 데이터베이스 초기화 (필요한 경우 스키마 업데이트)
    print("데이터베이스 확인 중...")
    initialize_db(db_filename)

    # 미전송 항목 조회
    items = get_unsent_talk_links(db_filename)
    total_items = len(items)

    if total_items == 0:
        logger.info("전송할 메시지가 없습니다.")
        return

    logger.info(f"총 {total_items}개의 미전송 톡톡 링크를 발견했습니다.")

    # 키워드 필터링
    if include_keywords or exclude_keywords:
        filtered_items, included, excluded, total = filter_urls_by_keywords(
            items, include_keywords, exclude_keywords, case_sensitive
        )

        logger.info(
            f"필터링 결과: 포함 {included}, 제외 {excluded}, 총 {total}개 항목 중 {len(filtered_items)}개 처리 예정"
        )
        items = filtered_items

    # 샘플 대상 표시
    display_sample_targets(items)

    # 전송 전 설정값 확인 및 사용자 승인 요청
    print("\n" + "=" * 50)
    print("톡톡 메시지 전송 설정 확인")
    print("=" * 50)
    print(f"데이터베이스 파일: {db_filename}")
    print(f"처리 대상 항목 수: {len(items)}개")
    print(f"병렬 처리 수: {parallel_count if not maintain_session else 1}")
    print(f"포함 키워드: {include_keywords if include_keywords else '없음'}")
    print(f"제외 키워드: {exclude_keywords if exclude_keywords else '없음'}")
    print(f"대소문자 구분: {'예' if case_sensitive else '아니오'}")
    print(f"세션 유지: {'예' if maintain_session else '아니오'}")
    print("\n[전송할 메시지]")
    print("-" * 50)
    print(message)
    print("-" * 50)
    print("\n※ 메시지 전송 중 언제든지 Ctrl+C를 눌러 취소할 수 있습니다.")

    # 사용자 확인 요청
    confirmation = input("\n위 설정으로 메시지를 전송하시겠습니까? (y/n): ")
    if confirmation.lower() not in ["y", "yes"]:
        print("메시지 전송이 취소되었습니다.")
        return

    # 세션 유지 모드일 경우 공유 브라우저 생성
    if maintain_session:
        with sync_playwright() as playwright:
            print("브라우저를 초기화하는 중...")
            shared_browser = initialize_browser(playwright)
            shared_context = shared_browser.new_context(
                viewport={"width": 1280, "height": 800}
            )

            if login_first:
                print("\n" + "=" * 50)
                print("네이버 로그인")
                print("=" * 50)
                print("로그인을 위해 새 브라우저 창이 열립니다.")
                print("로그인 후 브라우저를 닫지 말고, 아래에서 계속 진행하세요.")

                # 로그인 페이지로 이동
                page = shared_context.new_page()
                page.goto(
                    config.NAVER_LOGIN_URL,
                    timeout=config.TALKTALK_PAGE_LOAD_TIMEOUT * 1000,
                )

                # 사용자가 로그인할 때까지 대기
                input("\n로그인이 완료되면 Enter 키를 눌러 진행하세요: ")

                # 새 페이지를 닫지 않고 그대로 사용
                # page.close()

            else:
                # 로그인 단계를 건너뛰는 경우 새 페이지 생성
                page = shared_context.new_page()

            print("메시지 전송을 시작합니다...\n")

            # 진행 상황 추적기 초기화
            progress = ProgressTracker(len(items))
            progress.start_display()

            try:
                # 세션 유지 모드에서는 순차 처리
                for item in items:
                    # 단일 페이지를 재사용하는 방식으로 변경
                    url = item.get("url", "")
                    talk_link = item.get("talk_link", "")

                    # 톡톡 링크가 없는 경우
                    if not talk_link:
                        conn = get_db_connection(db_filename)
                        update_talk_message_status(
                            conn, url, config.TALKTALK_STATUS["NO_TALK_LINK"]
                        )
                        conn.close()
                        progress.update("no_link")
                        continue

                    # 이미 처리된 항목 확인
                    conn = get_db_connection(db_filename)
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT talk_message_status FROM websites WHERE url = ?", (url,)
                    )
                    result = cursor.fetchone()

                    if result and result["talk_message_status"] in [
                        config.TALKTALK_STATUS["SENT"],
                        config.TALKTALK_STATUS["ALREADY_SENT"],
                    ]:
                        conn.close()
                        progress.update("already_sent")
                        continue

                    # 상태 업데이트: 처리 중
                    update_talk_message_status(
                        conn, url, config.TALKTALK_STATUS["NOT_SENT"], commit=True
                    )
                    conn.close()

                    try:
                        # 같은 탭에서 URL 이동
                        page.goto(
                            talk_link, timeout=config.TALKTALK_PAGE_LOAD_TIMEOUT * 1000
                        )

                        # 페이지 로드 대기
                        page.wait_for_load_state("networkidle")

                        # 메시지 전송
                        success = send_talktalk_message(page, message)

                        # 결과 업데이트
                        conn = get_db_connection(db_filename)
                        if success:
                            update_talk_message_status(
                                conn, url, config.TALKTALK_STATUS["SENT"]
                            )
                            conn.close()
                            progress.update("sent")
                        else:
                            update_talk_message_status(
                                conn, url, config.TALKTALK_STATUS["ERROR"]
                            )
                            conn.close()
                            progress.update("error")
                    except Exception as e:
                        logger.error(f"톡톡 메시지 전송 중 오류 발생 ({url}): {e}")
                        conn = get_db_connection(db_filename)
                        update_talk_message_status(
                            conn, url, config.TALKTALK_STATUS["ERROR"]
                        )
                        conn.close()
                        progress.update("error")
            finally:
                # 진행 상황 표시 중지
                progress.stop_display()
                print("\n")  # 줄바꿈으로 프로그레스 바와 분리

                # 페이지 및 브라우저 정리
                if page:
                    page.close()
                if shared_context:
                    shared_context.close()
                if shared_browser:
                    shared_browser.close()

                # 결과 요약
                logger.info(f"톡톡 메시지 전송 완료:")
                logger.info(f"  - 성공: {progress.results['sent']}개")
                logger.info(f"  - 실패: {progress.results['error']}개")
                logger.info(f"  - 링크 없음: {progress.results['no_link']}개")
                logger.info(f"  - 이미 전송됨: {progress.results['already_sent']}개")
    else:
        # 병렬 처리 모드 (세션 유지 안함)
        print("메시지 전송을 시작합니다...\n")

        # 진행 상황 추적기 초기화
        progress = ProgressTracker(len(items))
        progress.start_display()

        # 병렬 처리 함수 정의
        def process_item(item):
            """단일 아이템을 처리하는 함수"""
            url = item.get("url", "")
            talk_link = item.get("talk_link", "")

            # 톡톡 링크가 없는 경우
            if not talk_link:
                conn = get_db_connection(db_filename)
                update_talk_message_status(
                    conn, url, config.TALKTALK_STATUS["NO_TALK_LINK"]
                )
                conn.close()
                return {"status": config.TALKTALK_STATUS["NO_TALK_LINK"]}

            # 이미 처리된 항목 확인
            conn = get_db_connection(db_filename)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT talk_message_status FROM websites WHERE url = ?", (url,)
            )
            result = cursor.fetchone()

            if result and result["talk_message_status"] in [
                config.TALKTALK_STATUS["SENT"],
                config.TALKTALK_STATUS["ALREADY_SENT"],
            ]:
                conn.close()
                return {"status": config.TALKTALK_STATUS["ALREADY_SENT"]}

            # 상태 업데이트: 처리 중
            update_talk_message_status(
                conn, url, config.TALKTALK_STATUS["NOT_SENT"], commit=True
            )
            conn.close()

            try:
                # 새 브라우저 세션 생성
                with sync_playwright() as playwright:
                    browser = initialize_browser(playwright)
                    context = browser.new_context(
                        viewport={"width": 1280, "height": 800}
                    )
                    page = context.new_page()

                    # 톡톡 페이지 로드
                    page.goto(
                        talk_link, timeout=config.TALKTALK_PAGE_LOAD_TIMEOUT * 1000
                    )

                    # 페이지 로드 대기
                    page.wait_for_load_state("networkidle")

                    # 메시지 전송
                    success = send_talktalk_message(page, message)

                    # 페이지와 브라우저 정리
                    page.close()
                    context.close()
                    browser.close()

                    # 결과 업데이트
                    conn = get_db_connection(db_filename)
                    if success:
                        update_talk_message_status(
                            conn, url, config.TALKTALK_STATUS["SENT"]
                        )
                        conn.close()
                        return {"status": config.TALKTALK_STATUS["SENT"]}
                    else:
                        update_talk_message_status(
                            conn, url, config.TALKTALK_STATUS["ERROR"]
                        )
                        conn.close()
                        return {"status": config.TALKTALK_STATUS["ERROR"]}
            except Exception as e:
                logger.error(f"톡톡 메시지 전송 중 오류 발생 ({url}): {e}")
                conn = get_db_connection(db_filename)
                update_talk_message_status(conn, url, config.TALKTALK_STATUS["ERROR"])
                conn.close()
                return {"status": config.TALKTALK_STATUS["ERROR"]}

        # 병렬 처리
        try:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=parallel_count
            ) as executor:
                futures = []

                for item in items:
                    future = executor.submit(process_item, item)
                    futures.append(future)

                # 결과 수집
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    status_key = None

                    if result["status"] == config.TALKTALK_STATUS["SENT"]:
                        status_key = "sent"
                    elif result["status"] == config.TALKTALK_STATUS["ERROR"]:
                        status_key = "error"
                    elif result["status"] == config.TALKTALK_STATUS["NO_TALK_LINK"]:
                        status_key = "no_link"
                    elif result["status"] == config.TALKTALK_STATUS["ALREADY_SENT"]:
                        status_key = "already_sent"

                    progress.update(status_key)
        finally:
            # 스레드 종료 및 최종 결과 출력
            progress.stop_display()
            print("\n")  # 줄바꿈으로 프로그레스 바와 분리

            # 결과 요약
            logger.info(f"톡톡 메시지 전송 완료:")
            logger.info(f"  - 성공: {progress.results['sent']}개")
            logger.info(f"  - 실패: {progress.results['error']}개")
            logger.info(f"  - 링크 없음: {progress.results['no_link']}개")
            logger.info(f"  - 이미 전송됨: {progress.results['already_sent']}개")


def main():
    """
    메인 함수
    """
    import argparse

    parser = argparse.ArgumentParser(description="네이버 톡톡 메시지 자동 전송 도구")

    parser.add_argument(
        "--db",
        type=str,
        default=config.DEFAULT_DB_FILENAME,
        help=f"데이터베이스 파일명 (기본값: {config.DEFAULT_DB_FILENAME})",
    )

    parser.add_argument(
        "--parallel",
        type=int,
        default=config.TALKTALK_PARALLEL_COUNT,
        help=f"병렬 처리 수 (기본값: {config.TALKTALK_PARALLEL_COUNT})",
    )

    parser.add_argument(
        "--include",
        type=str,
        nargs="+",
        default=config.DETAIL_INCLUDE_KEYWORDS,
        help="포함해야 할 키워드 목록",
    )

    parser.add_argument(
        "--exclude",
        type=str,
        nargs="+",
        default=config.DETAIL_EXCLUDE_KEYWORDS,
        help="제외해야 할 키워드 목록",
    )

    parser.add_argument(
        "--case-sensitive",
        action="store_true",
        default=config.DETAIL_CASE_SENSITIVE,
        help="대소문자 구분 여부",
    )

    parser.add_argument(
        "--message",
        type=str,
        default=None,
        help="전송할 메시지 (기본값: 설정 파일의 메시지)",
    )

    parser.add_argument(
        "--no-session",
        action="store_true",
        help="세션을 유지하지 않고 각 메시지마다 새 브라우저를 사용 (기본: 세션 유지)",
    )

    parser.add_argument(
        "--no-login",
        action="store_true",
        help="시작 시 로그인 과정을 건너뜀 (기본: 로그인 요청)",
    )

    parser.add_argument(
        "--init-db-only",
        action="store_true",
        help="데이터베이스 초기화만 수행하고 종료",
    )

    args = parser.parse_args()

    # 데이터베이스 초기화
    db_filename = args.db
    print(f"데이터베이스 {db_filename} 초기화 중...")
    initialize_db(db_filename)

    # --init-db-only 옵션이 주어진 경우 초기화 후 종료
    if args.init_db_only:
        print("데이터베이스 초기화 완료. 프로그램을 종료합니다.")
        return

    # 메시지 전송 시작
    send_talktalk_messages(
        db_filename=db_filename,
        include_keywords=args.include,
        exclude_keywords=args.exclude,
        case_sensitive=args.case_sensitive,
        parallel_count=args.parallel,
        message=args.message,
        maintain_session=not args.no_session,
        login_first=not args.no_login,
    )


if __name__ == "__main__":
    main()
