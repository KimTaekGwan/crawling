"""
Configuration settings for the Naver search result scraper.
"""

from dotenv import load_dotenv
import os

load_dotenv()


# 템플릿 파일에서 읽어오는 함수
def read_template_file(file_path, default_content="", required=True):
    """
    템플릿 파일을 읽어옵니다.

    Args:
        file_path: 템플릿 파일 경로
        default_content: 파일을 읽을 수 없을 때의 기본 내용
        required: 파일이 반드시 필요한지 여부 (True인 경우 파일이 없으면 예외 발생)

    Returns:
        템플릿 내용

    Raises:
        FileNotFoundError: 파일이 없고 required=True인 경우
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError as e:
        if required:
            print(f"오류: 템플릿 파일 {file_path}를 찾을 수 없습니다.")
            raise
        print(
            f"경고: 템플릿 파일 {file_path}를 찾을 수 없습니다. 기본 내용을 사용합니다."
        )
        return default_content
    except Exception as e:
        if required:
            print(f"오류: 템플릿 파일 {file_path} 읽기 실패: {e}")
            raise
        print(f"경고: 템플릿 파일 {file_path} 읽기 실패: {e}. 기본 내용을 사용합니다.")
        return default_content


# 부산 키워드
부산 = [
    "부산",
    "부산강서",
    "금정",
    "부산금정",
    "부산남",
    "부산동",
    "동래",
    "부산동래",
    "부산진",
    "부산북",
    "사상",
    "부산사상",
    "사하",
    "부산사하",
    "부산서",
    "수영",
    "부산수영",
    "연제",
    "부산연제",
    "영도",
    "부산영도",
    "부산중",
    "해운대",
    "부산해운대",
]


# 모두 제작
모두제작 = [
    "네이버모두제작",
    "네이버모두제작업체",
    "네이버모두제작사",
    "네이버모두대행",
    "네이버모두마케팅",
    "네이버모두홈페이지제작",
    "네이버모두홈페이지이전",
    "모두홈페이지제작",
    "모두홈페이지제작업체",
    "모두홈페이지마케팅",
    "모두홈페이지이전",
    "모두홈페이지유지보수",
    "모두홈페이지리뉴얼",
    "모두홈페이지대행",
    "모두사이트제작",
    "모두사이트이전",
    "모두사이트마케팅",
    "모두사이트유지보수",
    "네이버모두서비스종료",
    "네이버모두이전업체",
    "네이버모두홈페이지리뉴얼",
    "네이버모두홈페이지대행",
    "모두홈페이지운영대행",
    "모두홈페이지제작사",
    "모두홈페이지리뉴얼업체",
]


# 키워드
키워드 = [
    "커뮤니케이션",
    "IT",
    "마케팅",
    "디자인",
    "웹",
    "앱",
    "소프트웨어",
    "프로그래밍",
    "웹사이트",
]

# 업종
업종 = [
    "네일",
    "농장",
    "목장",
    "미용",
    "스파",
    "배달음식",
    # "법무",
    # "법률",
    "병원",
    # "부동산중개",
    "빵집",
    "제과점",
    "숙박",
    "캠핑장",
    "슈퍼",
    "마트",
    "스포츠",
    "헬스",
    # "약국",
    "운송",
    "렌터카",
    "음식점",
    "카페",
    "이사",
    "인테리어",
    "청소",
    "포토스튜디오",
    "학원",
    "헤어",
    "메이크업",
    "화랑",
    "출판",
    "공방",
    "레저",
]

# 광역자치단체
광역자치단체 = [
    "서울",
    "부산",
    "인천",
    "대구",
    "광주",
    "대전",
    "울산",
    "경기",
    "충북",
    "충남",
    "전북",
    "경북",
    "경남",
]

# 기초자치단체
기초자치단체 = [
    "수원",
    "성남",
    "안양",
    "고양",
    "안산",
    "용인",
    "부천",
    "청주",
    "천안",
    "전주",
    "포항",
    "창원",
]

# 구
구 = [
    "강남",
    "강동",
    "강북",
    "강서",
    "관악",
    "광진",
    "구로",
    "금천",
    "노원",
    "도봉",
    "동대문",
    "동작",
    "마포",
    "서대문",
    "서초",
    "성동",
    "성북",
    "송파",
    "양천",
    "영등포",
    "용산",
    "은평",
    "종로",
    "중구",
    "중랑",  # (서울 구들의 약칭)
    "금정",
    "남구",
    "동구",
    "동래",
    "부산진",
    "북구",
    "사상",
    "사하",
    "서구",
    "수영",
    "연제",
    "영도",
    "해운대",  # (부산 구들의 약칭 중복 제거 후)
    "미추홀",
    "연수",
    "남동",
    "부평",
    "계양",  # (인천 구들)
    "달서",
    "수성",  # (대구 구들; "남", "동", "북", "서", "중" 중복 발생)
    "광산",  # (광주)
    "대덕",
    "유성",  # (대전)
    # (울산은 "남", "동", "북", "중" 이미 상기에 포함)
    # 경기도 산하 시들의 구
    "권선",
    "영통",
    "장안",
    "팔달",  # 수원
    "분당",
    "수정",
    "중원",  # 성남
    "동안",
    "만안",  # 안양
    "덕양",
    "일산동",
    "일산서",  # 고양
    "단원",
    "상록",  # 안산
    "기흥",
    "수지",
    "처인",  # 용인
    "원미",
    "소사",
    "오정",  # 부천
    # 충청북도, 청주시
    "상당",
    "흥덕",
    "청원",
    "서원",
    # 충청남도, 천안시
    "동남",
    "서북",
    # 전북, 전주시
    "덕진",
    "완산",
    # 경북, 포항시
    "남구",
    "북구",
    "중구",
    "서구",
    "영천",
    "영주",
    # 경남, 창원시
    "의창",
    "성산",
    "마산합포",
    "마산회원",
    "진해",
]


# Search parameters
# 여러 타입의 키워드 리스트 정의
SEARCH_KEYWORD_TYPES = {
    "지역": 부산,
    # "지역": 광역자치단체 + 기초자치단체,
    # "지역": 광역자치단체 + 기초자치단체 + 구,
    # "지역": ["부산", "서울", "대구", "인천"],  # 지역 리스트 (선택 사항)
    "키워드": 업종,
    # "키워드": 키워드 + 업종,
    # "키워드": ["미용실", "헤어샵", "살롱"],  # 키워드 리스트 (선택 사항)
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

# 데이터 저장 설정
DATA_DIR = "./data/"
DEFAULT_DB_FILENAME = "crawler_data.db"

# 데이터베이스 컬럼 (참고용)
DB_COLUMNS = [
    "url",  # URL 주소
    "name",  # 업체명
    "keyword",  # 검색 키워드
    "company",  # 기업명
    "phone_number",  # 전화번호
    "email",  # 이메일
    "address",  # 주소
    "talk_link",  # 톡톡 링크
    "description",  # 설명
    "content",  # 내용
    "category",  # 카테고리
    "crawled_date",  # 크롤링 날짜
]

# Detail Crawler 필터링 설정
# 포함해야 하는 기본 키워드 리스트 (빈 리스트는 필터링하지 않음을 의미)
DETAIL_INCLUDE_KEYWORDS = []
# DETAIL_INCLUDE_KEYWORDS = 부산

# 제외해야 하는 기본 키워드 리스트 (빈 리스트는 필터링하지 않음을 의미)
DETAIL_EXCLUDE_KEYWORDS = 모두제작
# DETAIL_EXCLUDE_KEYWORDS = 모두제작

# 키워드 대소문자 구분 여부
DETAIL_CASE_SENSITIVE = False

# 톡톡 메시지 자동화 설정
TALKTALK_MESSAGE = """
소상공인진흥공단에서 스마트상점 사업을 통하여 무료로 홈페이지 제작이 가능하여 연락드립니다.

네이버 모두홈페이지 서비스가 올해 6월에 종료된다는 사실 알고 계신가요?

저희 (주)위븐 에서는 소상공인진흥공단과 함께 무료로 홈페이지 제작 지원사업을 진행하고 있으며, 

'소상공인'에 한하여 네이버 모두 홈페이지 이용자분들의 홈페이지 이관도 무료로 도와드리고 있습니다. 

소상공인은 개인사업자, 법인사업자 구분 없이
- 음식점, 카페, 미용실 등은 직원 5명 미만이면 소상공인입니다.
- 제조업, 건설업은 직원 10명 미만이면 소상공인입니다.
- 연매출도 업종별로 10억 ~ 120억 이하면 소상공인으로 인정됩니다. 

소상공인확인서 발급 방법은 간단하며 아래 링크에서 확인하실 수 있습니다.
https://blog.naver.com/prim57r/223792479698

모두 홈페이지 이관 외에도 
할인 행사 등을 위한 프로모션 랜딩페이지 제작 목적으로도 지원 가능합니다.
(헤어샵, 헬스짐, 식당 등 다양한 업종)

지원 기간이 이번 주 금요일(3/21)까지로 
서둘러 신청바랍니다.

저희 서비스에 대해 더 자세히 알아보시려면 아래 스마트스토어 페이지를 방문해 주세요.
https://smartstore.zaemit.com/

관심 있으시다면 바로 연락 부탁드립니다.
모두홈페이지 종료 전에 원활하게 이관하실 수 있도록 도와드리겠습니다.

감사합니다.
"""

# 톡톡 메시지 전송 병렬 처리 수
TALKTALK_PARALLEL_COUNT = 4

# 톡톡 메시지 상태 코드
TALKTALK_STATUS = {
    "NOT_SENT": 0,  # 메시지 전송 안됨
    "SENT": 1,  # 메시지 전송 완료
    "ERROR": 2,  # 에러 발생
    "NO_TALK_LINK": 3,  # 톡톡 링크 없음
    "ALREADY_SENT": 4,  # 이미 전송됨
}

# 톡톡 메시지 입력 및 전송 셀렉터
TALKTALK_INPUT_SELECTOR = "#content > section > footer > div:nth-child(2) > div.chat_write_area > div > div.chat_input_area > textarea"
TALKTALK_SUBMIT_SELECTOR = "#content > section > footer > div:nth-child(2) > div.chat_write_area > div > div.submit_btn_wrap > button"

# 톡톡 페이지 로딩 대기 시간 (초)
TALKTALK_PAGE_LOAD_TIMEOUT = 20
TALKTALK_BETWEEN_MSG_DELAY = 1

# 브라우저 세션 관련 설정
NAVER_LOGIN_URL = "https://nid.naver.com/nidlogin.login"
BROWSER_SESSION_TIMEOUT = 3600  # 브라우저 세션 타임아웃 (초)

# ========== 이메일 전송 관련 설정 ==========

# 템플릿 파일 경로
TEMPLATES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates"
)
EMAIL_HTML_TEMPLATE_PATH = os.path.join(TEMPLATES_DIR, "email_template.html")
EMAIL_TEXT_TEMPLATE_PATH = os.path.join(TEMPLATES_DIR, "email_template.txt")

# SMTP 서버 설정
EMAIL_SMTP_SERVER = "smtp.naver.com"
EMAIL_SMTP_PORT = 587  # TLS 사용 시 587, SSL 사용 시 465
EMAIL_SENDER = os.getenv("EMAIL_SENDER", "발신자ID@naver.com")  # 네이버 이메일 아이디
EMAIL_PASSWORD = os.getenv(
    "EMAIL_PASSWORD", "비밀번호"
)  # 네이버 이메일 비밀번호 또는 앱 비밀번호

# 이메일 제목
EMAIL_SUBJECT = "(광고)네이버 모두홈페이지 종료...🤢 무료 홈페이지 제작 및 이관 지원 안내💚 (3/21 마감)"

# 이메일 병렬 처리 수
EMAIL_PARALLEL_COUNT = 4

# 이메일 전송 간 딜레이 (초)
EMAIL_BETWEEN_DELAY = 1

# 이메일 상태 코드
EMAIL_STATUS = {
    "NOT_SENT": 0,  # 이메일 전송 안됨
    "SENT": 1,  # 이메일 전송 완료
    "ERROR": 2,  # 에러 발생
    "NO_EMAIL": 3,  # 이메일 주소 없음
    "ALREADY_SENT": 4,  # 이미 전송됨
}

# 기본 HTML 및 텍스트 템플릿 내용
DEFAULT_HTML_CONTENT = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>네이버 모두홈페이지 종료 안내 및 무료 홈페이지 제작 지원</title>
    <style>
        body { font-family: '맑은 고딕', sans-serif; line-height: 1.6; }
    </style>
</head>
<body>
    <h2>네이버 모두홈페이지 서비스 종료 안내</h2>
    <p>안녕하세요, 네이버 모두홈페이지 서비스가 올해 6월에 종료됩니다.</p>
    <p>저희 (주)위븐에서는 소상공인진흥공단과 함께 무료로 홈페이지 제작 지원사업을 진행하고 있습니다.</p>
</body>
</html>
"""

DEFAULT_TEXT_CONTENT = """
네이버 모두홈페이지 서비스 종료 안내

안녕하세요, 네이버 모두홈페이지 서비스가 올해 6월에 종료됩니다.
저희 (주)위븐에서는 소상공인진흥공단과 함께 무료로 홈페이지 제작 지원사업을 진행하고 있습니다.
"""

# 이메일 내용 로드 시 예외 처리
try:
    # 이메일 내용 (HTML 형식) - 템플릿 파일에서 읽어옴
    EMAIL_HTML_CONTENT = read_template_file(
        EMAIL_HTML_TEMPLATE_PATH,
        DEFAULT_HTML_CONTENT,
        required=False,  # 환경에 따라 required를 동적으로 조정 (개발 환경에서는 False로 설정)
    )

    # 일반 텍스트 버전 (HTML을 지원하지 않는 이메일 클라이언트용) - 템플릿 파일에서 읽어옴
    EMAIL_TEXT_CONTENT = read_template_file(
        EMAIL_TEXT_TEMPLATE_PATH,
        DEFAULT_TEXT_CONTENT,
        required=False,  # 환경에 따라 required를 동적으로 조정 (개발 환경에서는 False로 설정)
    )

    # 템플릿 파일 존재 여부 정보 설정
    HTML_TEMPLATE_EXISTS = os.path.exists(EMAIL_HTML_TEMPLATE_PATH)
    TEXT_TEMPLATE_EXISTS = os.path.exists(EMAIL_TEXT_TEMPLATE_PATH)

except Exception as e:
    print(f"이메일 템플릿 로드 중 오류 발생: {e}")
    print("기본 템플릿 내용을 사용합니다.")
    EMAIL_HTML_CONTENT = DEFAULT_HTML_CONTENT
    EMAIL_TEXT_CONTENT = DEFAULT_TEXT_CONTENT
    HTML_TEMPLATE_EXISTS = False
    TEXT_TEMPLATE_EXISTS = False
