"""
Module for storing crawled data in SQLite database.
"""

import os
import sqlite3
from typing import Dict, List, Set, Optional, Tuple
import src.config as config
import logging


def ensure_db_dir():
    """Ensure the data directory exists."""
    os.makedirs(config.DATA_DIR, exist_ok=True)


def get_db_connection(db_filename: str) -> sqlite3.Connection:
    """
    Get a connection to the SQLite database.

    Args:
        db_filename: Name of the database file

    Returns:
        SQLite connection object
    """
    ensure_db_dir()
    # Check if db_filename already contains a path separator
    if os.path.sep in db_filename:
        db_path = db_filename # Use the provided path directly
    else:
        # Assume it's just a filename and join with DATA_DIR
        db_path = os.path.join(config.DATA_DIR, db_filename)
    
    # Log the final path being used for connection
    logging.debug(f"Attempting to connect to database at: {os.path.abspath(db_path)}")

    conn = sqlite3.connect(db_path)

    # Enable foreign keys and other helpful settings
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row

    return conn


def initialize_db(db_filename: str, schema: Optional[List[str]] = None) -> None:
    """
    Initialize the database with necessary tables.

    Args:
        db_filename: Name of the database file
        schema: Optional custom schema statements
    """
    if not schema:
        # Default schema for storing crawled website data
        schema = [
            """
            CREATE TABLE IF NOT EXISTS websites (
                url TEXT PRIMARY KEY,
                title TEXT,
                description TEXT,
                keyword TEXT,
                category TEXT,
                content TEXT,
                crawled_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                company TEXT,
                phone_number TEXT,
                email TEXT,
                address TEXT,
                talk_link TEXT,
                name TEXT,
                talk_message_status INTEGER DEFAULT 0,
                talk_message_date TIMESTAMP
            )
            """
        ]

    conn = get_db_connection(db_filename)
    try:
        for statement in schema:
            conn.execute(statement)
        conn.commit()
        print(f"Database {db_filename} initialized successfully")

        # 스키마 마이그레이션 실행
        migrate_db_schema(conn)
    except sqlite3.Error as e:
        print(f"Database initialization error: {e}")
    finally:
        conn.close()


def migrate_db_schema(conn: sqlite3.Connection) -> None:
    """
    데이터베이스 스키마를 마이그레이션합니다.
    필요한 컬럼이 없는 경우 추가합니다.

    Args:
        conn: 데이터베이스 연결 객체
    """
    try:
        cursor = conn.cursor()

        # 현재 websites 테이블의 컬럼 목록 조회
        cursor.execute("PRAGMA table_info(websites)")
        columns = [row["name"] for row in cursor.fetchall()]

        # 필요한 컬럼이 없으면 추가
        migrations = []

        if "talk_message_status" not in columns:
            migrations.append(
                "ALTER TABLE websites ADD COLUMN talk_message_status INTEGER DEFAULT 0"
            )
            print("Adding talk_message_status column to the websites table...")

        if "talk_message_date" not in columns:
            migrations.append(
                "ALTER TABLE websites ADD COLUMN talk_message_date TIMESTAMP"
            )
            print("Adding talk_message_date column to the websites table...")

        if "email_status" not in columns:
            migrations.append(
                "ALTER TABLE websites ADD COLUMN email_status INTEGER DEFAULT 0"
            )
            print("Adding email_status column to the websites table...")

        if "email_date" not in columns:
            migrations.append("ALTER TABLE websites ADD COLUMN email_date TIMESTAMP")
            print("Adding email_date column to the websites table...")

        # 마이그레이션 실행
        for migration in migrations:
            cursor.execute(migration)

        if migrations:
            conn.commit()
            print("Database schema migration completed successfully")
    except sqlite3.Error as e:
        print(f"Database migration error: {e}")
        conn.rollback()


def normalize_field_name(field_name: str) -> str:
    """
    필드 이름을 정규화합니다. CSV와 SQLite 간의 일관성을 유지하기 위함입니다.

    Args:
        field_name: 원본 필드 이름

    Returns:
        정규화된 필드 이름
    """
    # 대소문자 구분 없이 필드 이름 매핑
    field_mapping = {
        "url": "url",
        "name": "name",
        "keyword": "keyword",
        "title": "title",
        "description": "description",
        "category": "category",
        "content": "content",
        "company": "company",
        "phonenumber": "phone_number",
        "email": "email",
        "address": "address",
        "talklink": "talk_link",
    }

    normalized = field_name.lower().replace(" ", "_")
    return field_mapping.get(normalized, normalized)


def save_to_db(data: List[Dict[str, str]], db_filename: str) -> int:
    """
    Save data to the SQLite database, replacing existing records with the same URL.

    Args:
        data: List of dictionaries containing the data to save
        db_filename: Name of the database file

    Returns:
        Number of records saved
    """
    if not data:
        logging.warning(f"No data to save to {db_filename}")
        return 0

    conn = get_db_connection(db_filename)
    try:
        cursor = conn.cursor()
        saved_count = 0

        for item in data:
            # 필드 정규화
            normalized_item = {}
            for key, value in item.items():
                normalized_key = normalize_field_name(key)
                normalized_item[normalized_key] = value

            # URL 필드가 없으면 건너뜀
            url = normalized_item.get("url", "")
            if not url:
                logging.warning(f"항목에 URL이 없어 저장을 건너뜁니다: {item}")
                continue

            # 정규화된 필드 이름으로 쿼리 작성
            fields = list(normalized_item.keys())
            placeholders = ", ".join(["?" for _ in fields])
            field_names = ", ".join([f'"{f}"' for f in fields])

            query = f"INSERT OR REPLACE INTO websites ({field_names}) VALUES ({placeholders})"
            values = [normalized_item.get(field, "") for field in fields]

            try:
                cursor.execute(query, values)
                saved_count += 1
                logging.debug(f"데이터 저장 성공: {url}")
            except sqlite3.Error as e:
                logging.error(f"항목 저장 중 오류: {url} - {e}")

        conn.commit()
        logging.info(f"데이터베이스 {db_filename}에 {saved_count}개 레코드 저장 완료")
        return saved_count

    except sqlite3.Error as e:
        logging.error(f"데이터베이스 저장 오류: {e}")
        conn.rollback()
        return 0
    finally:
        conn.close()


def get_processed_urls(db_filename: str) -> Set[str]:
    """
    Get the set of URLs that have already been processed.

    Args:
        db_filename: Name of the database file

    Returns:
        Set of URLs that have been processed
    """
    processed_urls = set()
    ensure_db_dir()
    db_path = os.path.join(config.DATA_DIR, db_filename)

    if not os.path.exists(db_path):
        print(f"Database file {db_filename} does not exist. Starting fresh.")
        initialize_db(db_filename)
        return processed_urls

    conn = get_db_connection(db_filename)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT url FROM websites")
        rows = cursor.fetchall()

        for row in rows:
            if row["url"]:
                processed_urls.add(row["url"])

        print(f"Found {len(processed_urls)} previously processed URLs in the database")
        return processed_urls

    except sqlite3.Error as e:
        print(f"Error retrieving processed URLs: {e}")
        return processed_urls
    finally:
        conn.close()


def read_urls_from_db(db_filename: str) -> List[Dict[str, str]]:
    """
    Read all URLs and their data from the database.

    Args:
        db_filename: Name of the database file

    Returns:
        List of dictionaries containing URL data
    """
    urls = []
    ensure_db_dir()
    db_path = os.path.join(config.DATA_DIR, db_filename)

    if not os.path.exists(db_path):
        print(f"Database file {db_filename} does not exist.")
        return urls

    conn = get_db_connection(db_filename)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM websites")
        rows = cursor.fetchall()

        for row in rows:
            url_data = dict(row)
            urls.append(url_data)

        print(f"Retrieved {len(urls)} URLs from the database")
        return urls

    except sqlite3.Error as e:
        print(f"Error reading URLs from database: {e}")
        return urls
    finally:
        conn.close()


def filter_urls_by_keywords(
    items: List[Dict[str, str]],
    include_keywords: List[str] = None,
    exclude_keywords: List[str] = None,
    case_sensitive: bool = False,
) -> Tuple[List[Dict[str, str]], int, int, int]:
    """
    키워드 기반으로 URL 항목들을 필터링합니다.

    Args:
        items: 필터링할 항목 리스트 (딕셔너리 형태)
        include_keywords: 포함해야 할 키워드 리스트
        exclude_keywords: 제외해야 할 키워드 리스트
        case_sensitive: 대소문자 구분 여부

    Returns:
        필터링된 항목 리스트, 포함된 항목 수, 제외된 항목 수, 총 항목 수의 튜플
    """
    if not items:
        return [], 0, 0, 0

    # 키워드 리스트가 없으면 모든 항목 반환
    if not include_keywords and not exclude_keywords:
        return items, len(items), 0, len(items)

    filtered_items = []
    included_count = 0
    excluded_count = 0
    total_count = len(items)

    # 필터링 함수 정의
    def contains_keywords(text: str, keywords: List[str]) -> bool:
        if not text or not keywords:
            return False

        if not case_sensitive:
            text = text.lower()
            keywords = [k.lower() for k in keywords if k]

        for keyword in keywords:
            if keyword and keyword in text:
                return True
        return False

    # 각 항목 검사
    for item in items:
        # 검색 대상 텍스트 준비 (모든 필드 값을 공백으로 연결)
        search_text = " ".join(str(v) for v in item.values() if v)

        # 제외 키워드 검사
        if exclude_keywords and contains_keywords(search_text, exclude_keywords):
            excluded_count += 1
            continue

        # 포함 키워드 검사
        if include_keywords:
            if contains_keywords(search_text, include_keywords):
                included_count += 1
                filtered_items.append(item)
        else:
            # 포함 키워드가 없으면 제외 키워드만 적용
            included_count += 1
            filtered_items.append(item)

    return filtered_items, included_count, excluded_count, total_count
