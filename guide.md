# 네이버 크롤링 프로젝트 사용 가이드 (Windows 기준)

## 목차
1. [프로젝트 소개](#프로젝트-소개)
2. [환경 설정](#환경-설정)
3. [네이버 검색 크롤링 (main.py)](#네이버-검색-크롤링-mainpy)
4. [상세 정보 크롤링 (detail_crawler.py)](#상세-정보-크롤링-detail_crawlerpy)
5. [데이터베이스 활용](#데이터베이스-활용)
6. [문제 해결](#문제-해결)
7. [자주 묻는 질문](#자주-묻는-질문)

## 프로젝트 소개

이 프로젝트는 네이버 검색 결과를 자동으로 수집하고, 수집된 URL에서 상세 정보(회사명, 전화번호, 이메일, 주소 등)를 추출하는 크롤링 도구입니다.

### 주요 기능
- 다양한 키워드 조합으로 네이버 검색 결과 수집
- 수집된 URL에서 회사 정보 자동 추출
- 모두(modoo.at) 사이트 정보 자동 추출 및 정리
- SQLite 데이터베이스에 결과 저장
- 중복 URL 제거 및 데이터 정리

## 환경 설정

### 1. Python 설치
1. [Python 공식 사이트](https://www.python.org/downloads/)에서 최신 버전(3.8 이상 권장) 다운로드
2. 설치 파일 실행
3. **중요**: "Add Python to PATH" 옵션 반드시 체크
   ![Python 설치 화면](https://www.python.org/static/community_logos/python-logo.png)
4. "Install Now" 클릭하여 설치 진행

### 2. 필요한 라이브러리 설치
1. 시작 메뉴 → 검색창에 "cmd" 입력 → "명령 프롬프트" 마우스 오른쪽 클릭 → "관리자 권한으로 실행"
2. 다음 명령어 입력:
   ```
   pip install playwright sqlite3 
   playwright install
   ```
   이 명령어는 웹 브라우저 자동화 도구인 Playwright와 데이터베이스 라이브러리를 설치합니다.

### 3. 프로젝트 다운로드
1. 이 프로젝트 폴더를 컴퓨터의 원하는 위치에 저장합니다.
2. 폴더 구조가 다음과 같은지 확인:
   ```
   프로젝트_폴더/
   ├── src/
   │   ├── main.py         (네이버 검색 크롤링)
   │   ├── detail_crawler.py (상세 정보 크롤링)
   │   ├── config.py       (설정 파일)
   │   ├── scraper.py      (브라우저 제어)
   │   ├── db_storage.py   (데이터베이스 관리)
   │   └── storage.py      (데이터 저장)
   └── guide.md            (이 가이드)
   ```

## 네이버 검색 크롤링 (main.py)

### 기본 사용법
1. 시작 메뉴 → 검색창에 "cmd" 입력 → "명령 프롬프트" 실행
2. 명령 프롬프트(cmd)에서 프로젝트 폴더로 이동:
   ```
   cd C:\사용자\내이름\다운로드\프로젝트_폴더
   ```
3. 다음 명령어로 실행:
   ```
   python -m src.main
   ```
4. 크롤링 정보가 표시되면 "y"를 입력하여 진행합니다.

### 작동 방식 상세 설명
1. **키워드 조합 생성**: `config.py`에 정의된 키워드로 모든 가능한 조합을 생성합니다.
   - 예: `지역` 키워드 + `업종` 키워드 + `필수어` 키워드
   - 기본 설정: "서울 식당 홈페이지", "부산 카페 사이트" 등 조합 생성

2. **검색 실행**: 생성된 키워드로 네이버 검색을 실행합니다.
   - Playwright를 사용하여 실제 브라우저 동작 자동화
   - `START_PAGE`부터 `END_PAGE`까지 각 페이지 크롤링

3. **데이터 추출**: 검색 결과 페이지에서 URL, 제목, 설명 등을 추출합니다.
   - 웹페이지에서 원하는 정보를 선택하여 추출
   - 검색 키워드 정보도 함께 저장

4. **데이터 저장**: 추출된 데이터는 SQLite 데이터베이스(`crawler_data.db`)에 저장됩니다.
   - URL을 기본 키(Primary Key)로 사용
   - 중복 URL은 최신 정보로 업데이트

5. **중복 방지**: 이미 크롤링한 키워드는 건너뛰는 기능이 있습니다.
   - 데이터베이스에 저장된 검색어 확인 후 처리

### 코드 분석: main.py의 주요 기능

```python
# 키워드 조합 생성 함수
def generate_keyword_combinations():
    """모든 가능한 키워드 조합을 생성합니다."""
    # config.py에 정의된 키워드 타입과 값을 읽어옴
    # 필수 타입(마지막 타입)과 선택적 타입으로 구분하여 조합 생성
    # 예: [(타입1, 값1), (타입2, 값2), ...] 형태의 조합 생성
    
# 키워드 조합으로 검색어 만들기
def combine_keywords(keyword_combo):
    """키워드 조합을 하나의 검색어로 만듭니다."""
    # 중복 키워드 제거 및 config.SEARCH_JOINER로 결합
    # config.SEARCH_SUFFIX 추가 (예: "@")
    
# 페이지 스크래핑 함수
def scrape_page(page, search_query, page_num):
    """검색 결과 페이지에서 데이터를 추출합니다."""
    # 지정된 페이지로 이동하여 검색 결과 스크래핑
    # 결과를 데이터베이스에 저장
```

### 고급 설정 (config.py)
`src/config.py` 파일을 수정하여 아래 설정을 변경할 수 있습니다:

```python
# 검색할 키워드 타입과 값들
SEARCH_KEYWORD_TYPES = {
    "지역": ["서울", "경기", "인천", "부산"],     # 지역 리스트
    "업종": ["식당", "카페", "미용실", "학원"],  # 업종 리스트
    "필수어": ["홈페이지", "사이트"]             # 마지막 타입은 필수로 포함됨
}

# 검색어 조합 방식 설정
SEARCH_TYPE_ORDER = ["지역", "업종", "필수어"]  # 타입 순서
SEARCH_JOINER = " "  # 키워드 결합 방식 (공백)
SEARCH_SUFFIX = ""   # 검색어 뒤에 추가할 텍스트

# 검색 페이지 범위
START_PAGE = 1  # 시작 페이지
END_PAGE = 3    # 종료 페이지

# 병렬 처리 수 
SEARCH_PARALLEL_COUNT = 4  # 동시에 처리할 검색어 수
```

### 주요 옵션 설정
- **강제 실행**: 이미 크롤링한 키워드도 다시 처리
  ```python
  # src/main.py에서 직접 수정
  set_force_run(True)
  ```

- **기존 키워드 건너뛰기 비활성화**: 모든 키워드 재크롤링
  ```python
  # src/main.py에서 직접 수정
  set_skip_existing(False)
  ```

- **병렬 처리 수 변경**: 동시에 처리할 검색어 수 조정
  ```python
  # src/main.py에서 직접 수정
  set_parallel_count(8)  # 8개로 변경
  ```

## 상세 정보 크롤링 (detail_crawler.py)

### 기본 사용법
1. 먼저 `main.py`로 URL을 수집한 후에 실행해야 합니다.
2. 명령 프롬프트(cmd)에서 다음 명령어 실행:
   ```
   python -m src.detail_crawler
   ```

### 작동 방식 상세 설명
1. **URL 정규화 및 중복 제거**:
   - 데이터베이스에서 URL을 읽어와 정규화 (쿼리 파라미터 제거)
   - 중복 URL 제거
   - 모두(modoo.at) 도메인이 아닌 URL 제외

2. **URL별 상세 정보 추출**:
   - Playwright를 사용하여 각 URL에 접속
   - 웹페이지 HTML에서 정규식 패턴으로 정보 추출
   - 푸터 영역에서 회사명, 전화번호, 이메일, 주소 등 추출
   - 페이지 내 네이버 톡톡 링크 추출

3. **병렬 처리**:
   - 여러 URL을 동시에 처리하여 크롤링 속도 향상
   - 기본값은 4개 URL 동시 처리 (설정 변경 가능)

4. **데이터 저장**:
   - 추출된 정보는 SQLite 데이터베이스에 업데이트
   - 기존 레코드는 덮어쓰기 (URL을 기준으로)

### 코드 분석: detail_crawler.py의 주요 기능

```python
# URL 정규화 및 중복 제거
def clean_database_urls(db_filename):
    """데이터베이스의 URL을 정규화하고 중복을 제거합니다."""
    # URL 정규화: 쿼리 파라미터와 프래그먼트 제거
    # 중복 URL 제거: 정규화된 URL 기준으로 중복 체크
    # 모두(modoo.at) 도메인이 아닌 URL 제거
    
# 웹페이지에서 푸터 정보 추출
def extract_footer_info(page):
    """웹페이지의 푸터에서 기업 정보를 추출합니다."""
    # 정규식을 사용하여 전화번호, 이메일, 주소, 기업명 추출
    # 특히 모두(modoo.at) 사이트 푸터 구조에 최적화

# 톡톡 링크 추출
def extract_talk_link(page):
    """웹페이지에서 네이버 톡톡 링크를 추출합니다."""
    # 스크립트 태그에서 정규식으로 톡톡 링크 추출

# 페이지별 상세 정보 추출
def crawl_detail_page(url):
    """특정 URL에서 상세 정보를 크롤링합니다."""
    # URL 접속 및 페이지 로딩
    # 푸터 정보 추출
    # 톡톡 링크 추출
    # 결과 반환
```

### 명령줄 옵션
다양한 옵션을 사용하여 크롤링 동작을 조정할 수 있습니다:

```
python -m src.detail_crawler [옵션]
```

사용 가능한 옵션:
- `--db 파일명.db`: 사용할 데이터베이스 파일 지정 (기본값: crawler_data.db)
- `--interval 숫자`: 중간 저장 간격 설정 (기본값: 10)
- `--new`: 이미 크롤링한 URL도 다시 처리
- `--parallel 숫자`: 병렬 처리 수 설정 (기본값: 4)
- `--include 키워드1 키워드2...`: 특정 키워드가 포함된 URL만 처리
- `--exclude 키워드1 키워드2...`: 특정 키워드가 포함된 URL 제외
- `--case-sensitive`: 키워드 대소문자 구분
- `--skip-url-cleaning`: URL 정규화 및 중복 제거 과정 건너뛰기

### 사용 예시

```
# 처음부터 다시 크롤링하고 병렬 처리 수를 8로 설정
python -m src.detail_crawler --new --parallel 8

# "카페"가 포함된 URL만 크롤링
python -m src.detail_crawler --include 카페

# "학원"이 포함된 URL 제외하고 크롤링
python -m src.detail_crawler --exclude 학원

# URL 정규화 과정 건너뛰기 (이미 정리된 경우)
python -m src.detail_crawler --skip-url-cleaning
```

## 데이터베이스 구조 및 활용

### 데이터베이스 구조
프로젝트는 SQLite 데이터베이스를 사용하여 크롤링 데이터를 저장합니다.

**websites 테이블 구조**:
```sql
CREATE TABLE IF NOT EXISTS websites (
    url TEXT PRIMARY KEY,      -- 웹사이트 URL (기본 키)
    title TEXT,                -- 웹사이트 제목
    description TEXT,          -- 설명
    keyword TEXT,              -- 검색에 사용된 키워드
    category TEXT,             -- 카테고리
    content TEXT,              -- 내용
    crawled_date TIMESTAMP,    -- 크롤링 날짜
    company TEXT,              -- 기업명
    phone_number TEXT,         -- 전화번호
    email TEXT,                -- 이메일 주소
    address TEXT,              -- 주소
    talk_link TEXT,            -- 네이버 톡톡 링크
    name TEXT                  -- 사이트 이름
)
```

### DB Browser for SQLite 설치
1. [DB Browser for SQLite](https://sqlitebrowser.org/dl/) 사이트 방문
2. Windows 설치 프로그램 다운로드 (Standard installer)
   ![DB Browser for SQLite 다운로드](https://sqlitebrowser.org/images/sqlitebrowser.svg)
3. 다운로드한 설치 파일 실행하여 설치

### 데이터베이스 열기
1. DB Browser for SQLite 실행
2. "데이터베이스 열기" 클릭
3. 프로젝트 폴더에 생성된 `crawler_data.db` 파일 선택
4. 왼쪽 패널에서 "테이블" → "websites" 선택하면 데이터 확인 가능

### 유용한 SQL 쿼리

데이터 추출을 위해 "SQL 실행" 탭을 클릭하고 다음 쿼리를 입력:

#### 1. 기본 데이터 조회
```sql
-- 모든 데이터 조회
SELECT * FROM websites;

-- 상세 정보가 있는 사이트만 조회
SELECT * FROM websites 
WHERE company != '' OR phone_number != '' OR email != '';
```

#### 2. 특정 정보가 있는 사이트 조회
```sql
-- 전화번호가 있는 사이트
SELECT name, url, company, phone_number, email, address 
FROM websites 
WHERE phone_number != '';

-- 이메일이 있는 사이트
SELECT name, url, company, phone_number, email, address 
FROM websites 
WHERE email != '';

-- 전화번호와, 이메일 모두 있는 사이트
SELECT name, url, company, phone_number, email, address 
FROM websites 
WHERE phone_number != '' AND email != '';
```

#### 3. 특정 키워드로 검색
```sql
-- 특정 키워드로 검색한 결과만 조회
SELECT * FROM websites WHERE keyword LIKE '%카페%';

-- 특정 지역 사이트 조회
SELECT * FROM websites WHERE name LIKE '%서울%' OR address LIKE '%서울%';

-- 특정 업종 조회 (제목이나 키워드에 포함된)
SELECT * FROM websites WHERE name LIKE '%미용실%' OR keyword LIKE '%미용실%';
```

#### 4. 데이터 분석 쿼리
```sql
-- 키워드별 사이트 수 집계
SELECT keyword, COUNT(*) as site_count 
FROM websites 
GROUP BY keyword 
ORDER BY site_count DESC;

-- 전화번호 있는 사이트 비율
SELECT 
    (SELECT COUNT(*) FROM websites WHERE phone_number != '') * 100.0 / 
    (SELECT COUNT(*) FROM websites) AS percent_with_phone;
```

#### 5. 데이터 내보내기
1. 쿼리 실행 후 결과 테이블에서 마우스 오른쪽 클릭
2. "내보내기" 선택
3. CSV 형식 선택 (MS Excel에서 열 수 있음)
4. 저장 위치 지정 후 "확인" 클릭

### 데이터 백업 방법
1. DB Browser for SQLite에서 "데이터베이스" → "데이터베이스를 파일로 저장" 선택
2. 백업 파일명 지정 (예: `crawler_data_backup_날짜.db`)
3. 저장 위치 선택 후 "저장" 클릭

## 문제 해결

### 크롤링이 시작되지 않는 경우
1. Python이 올바르게 설치되었는지 확인:
   ```
   python --version
   ```
   3.8 이상 버전이 표시되어야 합니다.

2. 필요한 라이브러리가 설치되었는지 확인:
   ```
   pip list
   ```
   목록에 playwright가 있어야 합니다.

3. 프로젝트 폴더 구조가 올바른지 확인

4. 관리자 권한으로 명령 프롬프트 실행 후 시도

### 크롤링 중 오류가 발생하는 경우
1. 인터넷 연결 상태 확인
2. 방화벽이나 보안 프로그램이 차단하지 않는지 확인
3. 크롤링 속도 조절 (병렬 처리 수 줄이기)
   ```
   python -m src.detail_crawler --parallel 2
   ```
4. 로그 확인: 오류 메시지를 확인하여 문제 파악

### 네이버에서 차단되는 경우
1. 병렬 처리 수를 줄여서 실행 (1~2개로 설정)
2. 시간 간격을 두고 다시 시도
3. IP 주소가 변경되면 다시 시도
4. VPN 사용 고려 (단, 이용약관 확인 필요)

### 데이터베이스 관련 문제
1. 데이터베이스 파일 확인
   ```
   dir data\crawler_data.db
   ```
   파일이 존재하는지 확인

2. 데이터베이스 파일이 손상된 경우:
   - 백업 파일이 있다면 복원
   - 없다면 손상된 DB 파일 삭제 후 처음부터 다시 시작

3. 데이터베이스 용량 문제:
   ```
   dir data\crawler_data.db /s
   ```
   용량이 큰 경우 필요 없는 데이터 정리 고려

## 자주 묻는 질문

### Q: 키워드 조합은 어떻게 변경하나요?
A: `src/config.py` 파일에서 `SEARCH_KEYWORD_TYPES` 딕셔너리를 수정하세요. 각 타입에 원하는 키워드를 추가하거나 제거할 수 있습니다.

```python
SEARCH_KEYWORD_TYPES = {
    "지역": ["서울", "부산", "대구"],  # 원하는 지역 추가
    "업종": ["식당", "카페", "미용실", "학원", "피트니스"],  # 업종 추가
    "필수어": ["홈페이지", "사이트"]
}
```

### Q: 크롤링 속도를 높이려면 어떻게 하나요?
A: 병렬 처리 수를 증가시키세요. 단, 너무 높게 설정하면 네이버에서 차단될 수 있습니다.
```
python -m src.detail_crawler --parallel 8
```

### Q: 이미 수집한 URL만 다시 크롤링하려면 어떻게 하나요?
A: detail_crawler.py에서 다음 옵션을 사용하세요:
```
python -m src.detail_crawler --new
```

### Q: 특정 키워드가 포함된 URL만 크롤링하려면 어떻게 하나요?
A: --include 옵션을 사용하세요:
```
python -m src.detail_crawler --include 카페 서울
```

### Q: 크롤링 결과를 Excel로 어떻게 내보내나요?
A: DB Browser for SQLite에서 쿼리 실행 후 결과를 CSV로 내보내고 Excel로 열 수 있습니다.
1. 원하는 SQL 쿼리 실행
2. 결과 테이블에서 마우스 오른쪽 클릭 → "내보내기" 선택
3. CSV 선택 후 저장 위치 지정 후 "확인" 클릭
4. Excel에서 CSV 파일 열기