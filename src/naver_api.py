"""
네이버 API를 사용하여 검색 결과를 가져오는 모듈입니다.
기존 scraper.py 로직을 활용하며 크롤링 대신 API 호출을 사용합니다.
"""

import requests
import logging
from typing import List, Dict, Optional, Tuple, Any
import src.config as config
import os
import time
import math
from dotenv import load_dotenv

load_dotenv()


class NaverSearchAPI:
    """네이버 검색 API를 사용하여 검색 결과를 가져오는 클래스"""

    def __init__(self):
        """클라이언트 ID와 시크릿을 초기화합니다."""
        self.client_id = os.getenv("NAVER_CLIENT_ID", os.getenv("NAVER_API_CLIENT_ID"))
        self.client_secret = os.getenv(
            "NAVER_CLIENT_SECRET", os.getenv("NAVER_API_CLIENT_SECRET")
        )

        if not self.client_id or not self.client_secret:
            raise ValueError(
                "NAVER_CLIENT_ID와 NAVER_CLIENT_SECRET 환경 변수 또는 config 설정이 필요합니다."
            )

        self.base_url = config.NAVER_API_BASE_URL
        self.headers = {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
        }
        self.daily_limit = config.NAVER_API_DAILY_LIMIT
        self.call_count = 0
        self.display_per_request = min(
            config.NAVER_API_DISPLAY_COUNT, 100
        )  # API 최대 제한 100개

    def initialize_api(self) -> Tuple[Any, Any, Any, Any]:
        """
        기존 initialize_browser 함수와 동일한 인터페이스를 제공합니다.
        실제로는 API 클라이언트만 초기화합니다.
        """
        return self, None, None, None

    def close_api(self, *args):
        """
        기존 close_browser 함수와 동일한 인터페이스를 제공합니다.
        실제로는 아무 작업도 하지 않습니다.
        """
        logging.info(f"API 호출 횟수: {self.call_count}/{self.daily_limit}")
        pass

    def get_search_page(self, _, search_query: str, page_num: int) -> bool:
        """
        기존 get_search_page 함수와 유사한 인터페이스로,
        API 호출의 유효성을 확인합니다.

        Args:
            _: 사용하지 않음 (기존 page 객체와 호환성 유지)
            search_query: 검색어
            page_num: 페이지 번호

        Returns:
            bool: API 호출이 가능한지 여부
        """
        if self.call_count >= self.daily_limit:
            logging.warning("API 일일 호출 한도에 도달했습니다.")
            return False

        if not search_query:
            logging.warning("검색어가 비어있습니다.")
            return False

        if page_num < 1 or page_num > 100:
            logging.warning(f"페이지 번호가 유효하지 않습니다: {page_num}")
            return False

        # 현재 검색어와 페이지 설정
        self.current_query = search_query
        self.current_page = page_num
        return True

    def scrape_search_results(self, _) -> List[Dict[str, str]]:
        """
        기존 scrape_search_results 함수와 유사한 인터페이스로,
        현재 검색어와 페이지에 대한 API 결과를 반환합니다.

        Args:
            _: 사용하지 않음 (기존 page 객체와 호환성 유지)

        Returns:
            List[Dict[str, str]]: 검색 결과 리스트
        """
        if not hasattr(self, "current_query") or not hasattr(self, "current_page"):
            logging.warning("현재 검색어와 페이지가 설정되지 않았습니다.")
            return []

        # 결과를 저장할 리스트
        results = []

        # config에 설정된 display 값 (최대 요청할 결과 개수)
        total_display = config.NAVER_API_DISPLAY_COUNT

        # 한 번의 API 요청으로 가져올 수 있는 최대 개수는 100
        # 따라서 100개 이상 요청하려면 여러 번 호출해야 함
        api_calls_needed = math.ceil(total_display / 100)

        try:
            # 여러 번의 API 호출로 결과 수집
            for call_index in range(api_calls_needed):
                # 현재 페이지에 기반한 시작 인덱스 계산
                # 첫 번째 호출은 기본 페이지, 이후 호출은 추가 결과
                base_start = ((self.current_page - 1) * total_display) + 1
                current_start = base_start + (call_index * 100)

                # 현재 호출에서 요청할 결과 개수 (남은 결과가 100보다 적으면 남은 것만 요청)
                remaining = total_display - (call_index * 100)
                current_display = min(100, remaining)

                # 더 요청할 결과가 없으면 중단
                if current_display <= 0:
                    break

                # API 호출 전 start 값 제한 확인 (최대 1000)
                if current_start > 1000:
                    logging.warning(
                        f"API 'start' 파라미터 최대값(1000) 초과: query='{self.current_query}', start={current_start}. 해당 검색어의 추가 결과 수집을 중단합니다."
                    )
                    break  # 현재 키워드에 대한 추가 API 호출 중단

                # API 호출
                params = {
                    "query": self.current_query,
                    "start": current_start,
                    "display": current_display,
                }

                # 정렬 방식이 config에 있으면 추가
                if hasattr(config, "NAVER_API_SORT") and config.NAVER_API_SORT:
                    params["sort"] = config.NAVER_API_SORT

                logging.info(
                    f"API 호출: query={self.current_query}, start={current_start}, display={current_display}"
                )

                response = requests.get(
                    self.base_url, headers=self.headers, params=params
                )
                self.call_count += 1

                # 응답 확인
                if response.status_code != 200:
                    logging.warning(
                        f"API 호출 실패: {response.status_code}, {response.text}"
                    )
                    # 첫 번째 호출 실패 시 바로 빈 결과 반환
                    if call_index == 0:
                        return []
                    continue  # 다음 호출 시도 (이미 일부 결과가 있을 수 있으므로)

                # 결과 파싱
                data = response.json()
                items = data.get("items", [])

                # 첫 번째 호출(call_index == 0)에서 결과가 없으면 바로 빈 리스트 반환
                if call_index == 0 and not items:
                    logging.info(
                        f"검색어 '{self.current_query}' 페이지 {self.current_page}에 대한 결과가 없습니다."
                    )
                    return []

                for item in items:
                    # 기존 형식과 일치하도록 변환
                    title = self._clean_html(item.get("title", ""))
                    url = item.get("link", "")

                    if title and url:
                        results.append({"title": title, "url": url})

                logging.info(
                    f"검색 결과 {len(items)}개 추출 완료 ({call_index+1}/{api_calls_needed} 호출)"
                )

                # 더 이상 결과가 없거나, 현재 요청한 개수보다 적게 오면 중단
                if len(items) < current_display:
                    break

                # API 호출 간 딜레이 (안정성을 위해)
                time.sleep(config.NAVER_API_CALL_DELAY)

            return results

        except Exception as e:
            logging.error(f"API 검색 결과 호출 중 오류: {str(e)}")
            return []

    def _clean_html(self, text: str) -> str:
        """HTML 태그를 제거합니다."""
        return text.replace("<b>", "").replace("</b>", "")


# 기존 함수와 동일한 인터페이스 제공
def initialize_browser():
    """네이버 API 클라이언트를 초기화합니다."""
    api_client = NaverSearchAPI()
    return api_client.initialize_api()


def close_browser(api_client, *args):
    """API 클라이언트를 종료합니다."""
    api_client.close_api(*args)


def get_search_page(api_client, search_query: str, page_num: int) -> bool:
    """
    특정 페이지의 네이버 검색 결과로 이동합니다.

    Args:
        api_client: API 클라이언트 객체
        search_query: 검색어
        page_num: 페이지 번호

    Returns:
        bool: 페이지 이동 성공 여부
    """
    return api_client.get_search_page(None, search_query, page_num)


def scrape_search_results(api_client) -> List[Dict[str, str]]:
    """
    현재 페이지에서 검색 결과를 가져옵니다.

    Args:
        api_client: API 클라이언트 객체

    Returns:
        List[Dict[str, str]]: 검색 결과 목록
    """
    return api_client.scrape_search_results(None)
