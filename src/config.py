"""
Configuration settings for the Naver search result scraper.
"""

# Search parameters
# 여러 타입의 키워드 리스트 정의
SEARCH_KEYWORD_TYPES = {
    "지역": ["부산", "서울", "대구", "인천"],  # 지역 리스트 (선택 사항)
    "키워드": ["미용실", "헤어샵", "살롱"],  # 키워드 리스트 (선택 사항)
    # "나라": ["한국"],  # 나라 리스트 (필수 사항)
}

# 키워드 타입 순서 (마지막 타입은 필수, 나머지는 선택)
SEARCH_TYPE_ORDER = ["지역", "키워드"]
# SEARCH_TYPE_ORDER = ["지역", "키워드", "나라"]

# 검색어 접미사 (@ 또는 다른 특수문자)
SEARCH_SUFFIX = "@"

# 검색어 조합 방식 (타입별 구분자)
SEARCH_JOINER = ""

START_PAGE = 2
END_PAGE = 10

# URLs and selectors
BASE_URL = "https://search.naver.com/search.naver"
RESULTS_SELECTOR = "#main_pack > section > div > ul > li"
TITLE_LINK_SELECTOR = "div.total_wrap > div.total_group > div.total_tit > a"

# Output settings
DATA_DIR = "./data/"
OUTPUT_FILE_NAME_TEMPLATE = "{}.csv"  # 검색어별 파일명
ALL_DATA_FILE_NAME = "all.csv"

# CSV headers
CSV_HEADERS = ["Name", "URL"]
ALL_CSV_HEADERS = ["Keyword", "Name", "URL"]  # all.csv에는 검색어 칼럼 추가
