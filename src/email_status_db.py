#!/usr/bin/env python3
"""
이메일 상태 데이터베이스 관리 모듈

이 모듈은 이메일 주소의 상태를 데이터베이스에 업데이트하는 기능을 제공합니다.
"""

import os
import sqlite3
import logging
from typing import List, Dict, Tuple, Optional, Set

# 프로젝트 모듈 임포트
import src.config as config

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 데이터베이스 파일 경로
DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "emails.db")


class EmailDatabase:
    """이메일 상태 데이터베이스 관리 클래스"""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        """
        이메일 데이터베이스 초기화

        Args:
            db_path: 데이터베이스 파일 경로
        """
        self.db_path = db_path
        self.conn = None
        self.cursor = None

    def connect(self) -> bool:
        """
        데이터베이스 연결

        Returns:
            연결 성공 여부
        """
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            self._create_tables_if_not_exists()
            logger.info(f"데이터베이스에 연결되었습니다: {self.db_path}")
            return True
        except Exception as e:
            logger.error(f"데이터베이스 연결 오류: {e}")
            return False

    def close(self) -> None:
        """데이터베이스 연결 종료"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
            logger.info("데이터베이스 연결이 종료되었습니다.")

    def _create_tables_if_not_exists(self) -> None:
        """필요한 테이블이 없으면 생성"""
        try:
            # 이메일 상태 테이블 생성
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS email_status (
                    email TEXT PRIMARY KEY,
                    status INTEGER NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # 상태 변경 이력 테이블 생성
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS status_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email TEXT NOT NULL,
                    old_status INTEGER,
                    new_status INTEGER NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (email) REFERENCES email_status (email)
                )
            """
            )

            self.conn.commit()
        except Exception as e:
            logger.error(f"테이블 생성 오류: {e}")
            raise

    def get_email_status(self, email: str) -> Optional[int]:
        """
        이메일의 현재 상태를 조회합니다.

        Args:
            email: 상태를 조회할 이메일 주소

        Returns:
            이메일 상태 코드 또는 None (이메일이 없는 경우)
        """
        try:
            self.cursor.execute(
                "SELECT status FROM email_status WHERE email = ?", (email,)
            )
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"이메일 '{email}' 상태 조회 중 오류: {e}")
            return None

    def update_email_status(self, email: str, status: int) -> bool:
        """
        이메일 상태를 업데이트합니다.

        Args:
            email: 업데이트할 이메일 주소
            status: 새 상태 코드

        Returns:
            업데이트 성공 여부
        """
        try:
            # 현재 상태 조회
            current_status = self.get_email_status(email)

            if current_status is None:
                # 이메일이 없으면 새로 추가
                self.cursor.execute(
                    "INSERT INTO email_status (email, status) VALUES (?, ?)",
                    (email, status),
                )
            else:
                # 이메일이 있으면 상태 업데이트
                self.cursor.execute(
                    "UPDATE email_status SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE email = ?",
                    (status, email),
                )

            # 상태 변경 이력 추가
            self.cursor.execute(
                "INSERT INTO status_history (email, old_status, new_status) VALUES (?, ?, ?)",
                (email, current_status, status),
            )

            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"이메일 '{email}' 상태 업데이트 중 오류: {e}")
            if self.conn:
                self.conn.rollback()
            return False

    def update_multiple_email_status(
        self, emails: Set[str], status: int
    ) -> Dict[str, bool]:
        """
        여러 이메일의 상태를 동시에 업데이트합니다.

        Args:
            emails: 업데이트할 이메일 주소 집합
            status: 새 상태 코드

        Returns:
            업데이트 결과 딕셔너리 (이메일: 성공 여부)
        """
        results = {}

        for email in emails:
            results[email] = self.update_email_status(email, status)

        return results

    def get_emails_by_status(self, status: int) -> List[str]:
        """
        특정 상태의 이메일 목록을 조회합니다.

        Args:
            status: 조회할 상태 코드

        Returns:
            해당 상태의 이메일 주소 목록
        """
        try:
            self.cursor.execute(
                "SELECT email FROM email_status WHERE status = ?", (status,)
            )
            return [row[0] for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"상태 코드 {status}인 이메일 조회 중 오류: {e}")
            return []

    def get_status_statistics(self) -> Dict[int, int]:
        """
        상태별 이메일 개수 통계를 반환합니다.

        Returns:
            상태 코드별 이메일 개수 딕셔너리
        """
        try:
            self.cursor.execute(
                "SELECT status, COUNT(*) FROM email_status GROUP BY status"
            )
            results = self.cursor.fetchall()
            return {status: count for status, count in results}
        except Exception as e:
            logger.error(f"상태 통계 조회 중 오류: {e}")
            return {}


# 모듈 직접 실행 시 간단한 테스트
if __name__ == "__main__":
    db = EmailDatabase()

    if db.connect():
        # 테스트 이메일 상태 업데이트
        test_email = "test@example.com"
        result = db.update_email_status(test_email, config.EMAIL_STATUS["NOT_SENT"])

        print(f"테스트 이메일 상태 업데이트 결과: {result}")

        # 현재 상태 출력
        status = db.get_email_status(test_email)
        print(
            f"현재 상태: {status} ({[k for k, v in config.EMAIL_STATUS.items() if v == status][0]})"
        )

        # 상태별 통계
        stats = db.get_status_statistics()
        print("\n상태별 이메일 개수:")
        for status_code, count in stats.items():
            status_name = [
                k for k, v in config.EMAIL_STATUS.items() if v == status_code
            ]
            status_name = status_name[0] if status_name else "알 수 없음"
            print(f"- {status_name} ({status_code}): {count}개")

        db.close()
