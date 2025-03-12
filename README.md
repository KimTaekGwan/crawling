# 네이버 크롤링 API

네이버 검색 결과 및 상세 정보를 크롤링하고 데이터를 관리하는 API 서버입니다.

## 주요 기능

- 네이버 검색 결과 크롤링
- 검색 결과 URL의 상세 정보 크롤링
- 백그라운드에서 크롤링 작업 실행
- RESTful API를 통한 크롤링 작업 관리
- 크롤링 결과 조회 및 필터링
- SQLite 데이터베이스를 통한 데이터 관리

## 설치 방법

### 요구 사항

- Python 3.8 이상
- pip
- Playwright

### 설치 과정

1. 저장소 클론

```bash
git clone <repository-url>
cd <repository-directory>
```

2. 가상 환경 생성 및 활성화

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/MacOS
source venv/bin/activate
```

3. 필요한 패키지 설치

```bash
pip install -r requirements.txt
```

4. Playwright 설치

```bash
playwright install
```

## 사용 방법

### API 서버 실행

```bash
python run_api.py
```

기본적으로 서버는 `http://localhost:8000`에서 실행됩니다.

다음 옵션을 사용할 수 있습니다:

- `--host`: 바인딩할 호스트 (기본값: 0.0.0.0)
- `--port`: 사용할 포트 (기본값: 8000)
- `--reload`: 코드 변경 시 자동 리로드
- `--debug`: 디버그 모드 활성화

### API 사용 방법

#### API 문서

서버 실행 후 다음 URL에서 API 문서를 확인할 수 있습니다:

- `http://localhost:8000/docs` - Swagger UI
- `http://localhost:8000/redoc` - ReDoc

#### 주요 엔드포인트

##### 크롤링 작업

1. 검색 크롤링 시작

```
POST /api/v1/crawl/search
```

2. 상세 정보 크롤링 시작

```
POST /api/v1/crawl/detail
```

3. 작업 목록 조회

```
GET /api/v1/crawl/tasks
```

4. 작업 상태 조회

```
GET /api/v1/crawl/tasks/{task_id}
```

##### 결과 조회

1. 검색 결과 조회

```
GET /api/v1/results/search
```

2. 상세 결과 조회

```
GET /api/v1/results/detail
```

3. 크롤링 통계 조회

```
GET /api/v1/results/statistics
```

## 프로젝트 구조

```
crawling/
├── api/                   # API 관련 모듈
│   ├── background.py      # 백그라운드 작업 처리
│   ├── main.py            # FastAPI 메인 애플리케이션
│   └── routes/            # API 라우트
│       ├── crawl.py       # 크롤링 작업 관련 라우트
│       └── results.py     # 결과 조회 관련 라우트
├── crawlers/              # 크롤러 모듈
│   ├── detail_crawler.py  # 상세 정보 크롤러
│   └── search_crawler.py  # 검색 결과 크롤러
├── db/                    # 데이터베이스 관련 모듈
│   ├── database.py        # 데이터베이스 연결 설정
│   └── models.py          # SQLAlchemy 모델
├── src/                   # 소스코드
│   ├── config.py          # 설정 파일
│   ├── main.py            # 메인 로직
│   └── scraper.py         # 스크래핑 유틸리티
├── requirements.txt       # 의존성 패키지 목록
├── run_api.py             # API 서버 실행 스크립트
└── README.md              # 프로젝트 설명
```

## 라이선스

이 프로젝트는 MIT 라이선스에 따라 배포됩니다. 자세한 내용은 LICENSE 파일을 참조하세요.
