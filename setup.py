#!/usr/bin/env python
"""
Setup script for Naver search result scraper.
"""
import os
import subprocess
import sys


def install_dependencies():
    """Install Python dependencies using uv."""
    print("Installing Python dependencies using uv...")

    # uv가 설치되어 있는지 확인
    try:
        subprocess.run(["uv", "--version"], check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("uv가 설치되어 있지 않습니다. 설치합니다...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "uv"])
        except subprocess.CalledProcessError:
            print("uv 설치에 실패했습니다. pip를 사용하여 설치를 진행합니다.")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-e", "."])
            return

    # uv를 사용하여 의존성 설치
    subprocess.check_call(["uv", "pip", "install", "-e", "."])
    print("의존성 설치가 완료되었습니다.")


def install_playwright_browsers():
    """Install Playwright browsers."""
    print("Playwright 브라우저를 설치합니다...")
    subprocess.check_call(["playwright", "install", "chromium"])


def create_data_dir():
    """Create data directory if it doesn't exist."""
    os.makedirs("data", exist_ok=True)
    print("data 디렉토리를 생성했습니다.")


def create_entrypoints():
    """Create executable entry points for easier command-line usage."""
    print("실행 스크립트를 생성합니다...")

    # Unix/Linux/macOS용 스크립트
    if os.name != "nt":  # Windows가 아닌 경우
        with open("naver-scraper", "w") as f:
            f.write("#!/bin/sh\n")
            f.write('python -m src.main "$@"\n')

        with open("naver-details", "w") as f:
            f.write("#!/bin/sh\n")
            f.write('python -m src.detail_crawler "$@"\n')

        # 실행 권한 부여
        os.chmod("naver-scraper", 0o755)
        os.chmod("naver-details", 0o755)

    # Windows용 배치 파일
    else:
        with open("naver-scraper.bat", "w") as f:
            f.write("@echo off\n")
            f.write("python -m src.main %*\n")

        with open("naver-details.bat", "w") as f:
            f.write("@echo off\n")
            f.write("python -m src.detail_crawler %*\n")

    print("실행 스크립트 생성이 완료되었습니다.")


def main():
    """Run all setup steps."""
    print("네이버 검색 결과 크롤러 설정을 시작합니다...")

    install_dependencies()
    install_playwright_browsers()
    create_data_dir()
    create_entrypoints()

    print("\n설정이 완료되었습니다!")
    print("다음 명령으로 프로그램을 실행할 수 있습니다:")

    if os.name != "nt":  # Windows가 아닌 경우
        print("  ./naver-scraper - 네이버 검색 크롤링")
        print("  ./naver-details - 상세 정보 크롤링")
    else:
        print("  naver-scraper.bat - 네이버 검색 크롤링")
        print("  naver-details.bat - 상세 정보 크롤링")


if __name__ == "__main__":
    main()
