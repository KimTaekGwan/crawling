"""
Module for scraping Naver search results using Playwright.
"""

from playwright.sync_api import sync_playwright
from typing import List, Dict, Optional
import src.config as config


def initialize_browser():
    """Initialize and return a Playwright browser instance."""
    playwright = sync_playwright().start()
    # browser = playwright.chromium.launch(headless=False)
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    return playwright, browser, context, page


def close_browser(playwright, browser, context):
    """Close the browser and Playwright instances."""
    context.close()
    browser.close()
    playwright.stop()


def get_search_page(page, search_query: str, page_num: int) -> bool:
    """
    Navigate to a specific page of Naver search results.

    Args:
        page: Playwright page object
        search_query: The search query string
        page_num: The page number to navigate to

    Returns:
        bool: True if navigation was successful
    """
    start_index = (page_num - 1) * 15 + 1
    url = f"{config.BASE_URL}?query={search_query}&page={page_num}&start={start_index}&where=web"

    try:
        page.goto(url)
        # Wait for search results to load
        page.wait_for_selector(config.RESULTS_SELECTOR, timeout=10000)
        return True
    except Exception as e:
        print(f"Error navigating to page {page_num}: {e}")
        return False


def scrape_search_results(page) -> List[Dict[str, str]]:
    """
    Scrape salon data from the current page.

    Args:
        page: Playwright page object

    Returns:
        List[Dict[str, str]]: List of dictionaries containing name and URL
    """
    results = []

    try:
        # Get all list items
        items = page.query_selector_all(config.RESULTS_SELECTOR)

        for item in items:
            # Get the title link element
            link_element = item.query_selector(config.TITLE_LINK_SELECTOR)

            if link_element:
                # Extract URL
                url = link_element.get_attribute("href")

                # Extract name (text content without markup)
                name = link_element.inner_text()

                if url and name:
                    results.append({"Name": name, "URL": url})

    except Exception as e:
        print(f"Error scraping search results: {e}")

    return results
