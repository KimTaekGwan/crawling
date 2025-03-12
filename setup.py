#!/usr/bin/env python
"""
Setup script for Naver search result scraper.
"""
import os
import subprocess


def install_requirements():
    """Install Python dependencies from requirements.txt."""
    print("Installing Python dependencies...")
    subprocess.check_call(["pip", "install", "-r", "requirements.txt"])


def install_playwright_browsers():
    """Install Playwright browsers."""
    print("Installing Playwright browsers...")
    subprocess.check_call(["playwright", "install", "chromium"])


def create_data_dir():
    """Create data directory if it doesn't exist."""
    os.makedirs("data", exist_ok=True)
    print("Created data directory.")


def main():
    """Run all setup steps."""
    print("Setting up Naver search result scraper...")

    install_requirements()
    install_playwright_browsers()
    create_data_dir()

    print("\nSetup complete! You can now run the scraper with 'python scrape_naver.py'")


if __name__ == "__main__":
    main()
