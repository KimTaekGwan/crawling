"""
Module for storing scraped data in SQLite database.
"""

import os
import logging
from typing import List, Dict
import src.config as config
from src.db_storage import initialize_db, save_to_db, ensure_db_dir

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 기본 데이터베이스 파일명
DEFAULT_DB_FILENAME = config.DEFAULT_DB_FILENAME


def save_page_data(search_query: str, page_num: int, data: List[Dict[str, str]]):
    """
    Save data from a specific page to the SQLite database.

    Args:
        search_query: The search query used
        page_num: The page number where the data was scraped from
        data: The scraped data to save
    """
    # 데이터가 비어있으면 저장하지 않음
    if not data:
        logger.warning(f"No data to save for query '{search_query}', page {page_num}")
        return

    # 데이터베이스 초기화 확인
    ensure_db_dir()

    try:
        # DB 형식에 맞게 데이터 준비
        db_data = []
        valid_data = False  # 유효한 데이터가 있는지 확인하는 플래그

        for item in data:
            # URL 필드가 없는 경우 대체 필드 확인
            url = item.get("URL", item.get("url", "")).strip()

            # URL이 없으면 건너뜀
            if not url:
                continue

            # 유효한 데이터가 하나라도 있으면 플래그 설정
            valid_data = True

            # 필드 이름 변환 및 통일
            db_item = {}

            # URL 필드 설정
            db_item["url"] = url

            # Name 또는 title 필드 처리
            if "Name" in item:
                db_item["name"] = item["Name"]
            elif "name" in item:
                db_item["name"] = item["name"]
            elif "title" in item:
                db_item["name"] = item["title"]

            # 기타 필드 복사 (소문자로 변환)
            for key, value in item.items():
                if key.lower() not in ["url", "name"] and key not in [
                    "URL",
                    "Name",
                    "title",
                ]:
                    db_item[key.lower()] = value

            # 검색어 추가
            db_item["keyword"] = search_query

            db_data.append(db_item)

        # 유효한 데이터가 없으면 경고 로그 출력
        if not valid_data:
            logger.warning(
                f"No valid data to save from '{search_query}' page {page_num}"
            )
            return

        # 데이터가 있으면 저장
        if db_data:
            # SQLite DB에 저장
            count = save_to_db(db_data, DEFAULT_DB_FILENAME)
            logger.info(f"Saved {count} items from '{search_query}' page {page_num}")
        else:
            logger.warning(
                f"No valid data to save from '{search_query}' page {page_num}"
            )

    except Exception as e:
        logger.error(f"Error saving data from '{search_query}' page {page_num}: {e}")
