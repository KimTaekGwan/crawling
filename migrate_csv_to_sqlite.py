"""
CSV 파일에서 SQLite 데이터베이스로 데이터를 마이그레이션하는 스크립트.
"""

import os
import csv
import argparse
from typing import List, Dict
import src.config as config
from src.db_storage import initialize_db, save_to_db, get_db_connection


def read_csv_data(csv_filename: str) -> List[Dict[str, str]]:
    """
    CSV 파일의 데이터를 읽어옵니다.

    Args:
        csv_filename: 읽을 CSV 파일명

    Returns:
        CSV 데이터가 담긴 딕셔너리 리스트
    """
    data = []
    filepath = os.path.join(config.DATA_DIR, csv_filename)

    if not os.path.exists(filepath):
        print(f"파일이 존재하지 않습니다: {filepath}")
        return data

    print(f"CSV 파일을 읽는 중: {filepath}")
    try:
        with open(filepath, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
        print(f"{len(data)}개의 레코드를 읽었습니다.")
    except Exception as e:
        print(f"CSV 파일 읽기 오류: {e}")

    return data


def migrate_csv_to_sqlite(csv_filename: str, db_filename: str):
    """
    CSV 파일에서 SQLite 데이터베이스로 데이터를 마이그레이션합니다.

    Args:
        csv_filename: 마이그레이션할 CSV 파일명
        db_filename: 대상 SQLite 파일명
    """
    print(f"CSV({csv_filename})에서 SQLite({db_filename})로 마이그레이션을 시작합니다.")

    # 데이터베이스 초기화
    initialize_db(db_filename)

    # CSV 파일 읽기
    data = read_csv_data(csv_filename)
    if not data:
        print("마이그레이션할 데이터가 없습니다.")
        return

    # 데이터베이스에 저장
    saved_count = save_to_db(data, db_filename)
    print(
        f"마이그레이션 완료! {saved_count}개의 레코드가 SQLite 데이터베이스에 저장되었습니다."
    )

    # 마이그레이션된 데이터 확인
    conn = get_db_connection(db_filename)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM websites")
        count = cursor.fetchone()["count"]
        print(f"현재 데이터베이스에는 총 {count}개의 레코드가 있습니다.")
    except Exception as e:
        print(f"데이터베이스 확인 중 오류: {e}")
    finally:
        conn.close()


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description="CSV 파일에서 SQLite 데이터베이스로 데이터를 마이그레이션합니다."
    )
    parser.add_argument(
        "--csv",
        default="details_" + config.ALL_DATA_FILE_NAME,
        help="마이그레이션할 CSV 파일명 (기본값: details_all_data.csv)",
    )
    parser.add_argument(
        "--db",
        default="crawler_data.db",
        help="대상 SQLite 데이터베이스 파일명 (기본값: crawler_data.db)",
    )
    args = parser.parse_args()

    migrate_csv_to_sqlite(args.csv, args.db)


if __name__ == "__main__":
    main()
