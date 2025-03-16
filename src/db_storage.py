"""
Module for storing crawled data in SQLite database.
"""

import os
import sqlite3
from typing import Dict, List, Set, Optional
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
    db_path = os.path.join(config.DATA_DIR, db_filename)
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
                name TEXT
            )
            """
        ]

    conn = get_db_connection(db_filename)
    try:
        for statement in schema:
            conn.execute(statement)
        conn.commit()
        print(f"Database {db_filename} initialized successfully")
    except sqlite3.Error as e:
        print(f"Database initialization error: {e}")
    finally:
        conn.close()


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
