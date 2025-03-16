# 네이버 크롤링 도구

네이버 검색 결과와 해당 웹페이지의 상세 정보를 크롤링하는 도구입니다. CSV 대신 SQLite 데이터베이스를 사용하여 데이터를 효율적으로 관리합니다.

## 주요 기능

- 네이버 검색 결과 크롤링
- 검색 결과 URL의 상세 정보 크롤링
- SQLite 데이터베이스를 통한 데이터 관리
- 병렬 처리를 통한 빠른 크롤링
- 중복 URL 및 이미 처리된 데이터 자동 필터링

## 설치 방법

### 요구 사항

- Python 3.8 이상
- uv (파이썬 패키지 관리 도구)

### 설치 과정

1. 저장소 클론

```bash
git clone <repository-url>
cd <repository-directory>
```

2. uv 설치 (설치되어 있지 않은 경우)

```bash
# 공식 설치 방법
curl -LsSf https://astral.sh/uv/install.sh | sh

# 또는 pip 사용
pip install uv
```

3. 설치 스크립트 실행 (의존성 설치 및 초기 설정)

```bash
python setup.py
```

이 스크립트는 다음 작업을 수행합니다:

- uv를 사용하여 필요한 패키지 설치
- Playwright 브라우저 설치
- data 디렉토리 생성
- 실행 스크립트 생성

## 사용 방법

### 1. 네이버 검색 결과 크롤링

```bash
# Linux/Mac
./naver-scraper [options]

# Windows
naver-scraper.bat [options]
```

옵션:

- `--force`: 이미 처리된 검색어도 강제로 다시 크롤링합니다.
- `--no-skip-existing`: 이미 크롤링된 키워드도 다시 크롤링합니다.
- `--parallel <num>`: 병렬 처리 수를 설정합니다 (기본값: 4).
- `--log-level <level>`: 로그 레벨을 설정합니다 (DEBUG, INFO, WARNING, ERROR, CRITICAL).

### 2. 상세 정보 크롤링

```bash
# Linux/Mac
./naver-details [options]

# Windows
naver-details.bat [options]
```

옵션:

- `--new`: 처음부터 다시 크롤링합니다 (기존 상세 정보 무시).
- `--interval <num>`: 중간 저장 간격을 설정합니다 (기본값: 10개 URL마다).
- `--db <filename>`: 데이터베이스 파일명을 지정합니다 (기본값: crawler_data.db).
- `--parallel <num>`: 병렬 처리 수를 설정합니다 (기본값: 4).
- `--log-level <level>`: 로그 레벨을 설정합니다.

## uv로 설치 및 실행하기

프로젝트를 개발 모드로 설치하여 어디서든 실행할 수 있도록 하려면:

```bash
# 개발 모드로 설치
uv pip install -e .

# 이제 다음 명령으로 실행 가능
naver-scraper
naver-details
```

또는 가상 환경을 생성하여 설치할 수도 있습니다:

```bash
# 가상 환경 생성 및 활성화
uv venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows

# 설치
uv pip install -e .
```

## 프로젝트 구조

```
naver-crawler/
├── src/                   # 소스코드
│   ├── config.py          # 설정 파일
│   ├── main.py            # 메인 로직 (검색 크롤링)
│   ├── detail_crawler.py  # 상세 정보 크롤링 로직
│   ├── scraper.py         # 스크래핑 유틸리티
│   ├── db_storage.py      # 데이터베이스 연결 및 저장 로직
│   └── storage.py         # 데이터 저장 인터페이스
├── data/                  # 데이터 디렉토리
│   └── crawler_data.db    # SQLite 데이터베이스 파일
├── naver-scraper          # 검색 크롤링 실행 스크립트 (Unix)
├── naver-details          # 상세 정보 크롤링 실행 스크립트 (Unix)
├── naver-scraper.bat      # 검색 크롤링 실행 스크립트 (Windows)
├── naver-details.bat      # 상세 정보 크롤링 실행 스크립트 (Windows)
├── pyproject.toml         # 프로젝트 메타데이터 및 의존성
├── setup.py               # 설치 스크립트
└── README.md              # 프로젝트 설명
```

## 라이선스

이 프로젝트는 MIT 라이선스에 따라 배포됩니다.
