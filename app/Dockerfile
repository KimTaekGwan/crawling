FROM python:3.10

USER root

EXPOSE 8581

WORKDIR /code
# 환경 변수 설정

RUN apt-get update && \
    apt-get install -y wget unzip && \
    apt-get clean

# 폰트 다운로드 및 압축 해제
RUN wget http://cdn.naver.com/naver/NanumFont/fontfiles/NanumFont_TTF_ALL.zip && \
    unzip NanumFont_TTF_ALL.zip -d NanumFont

# 폰트를 /usr/share/fonts로 이동
RUN mv NanumFont /usr/share/fonts/

# 폰트 캐시 초기화
RUN fc-cache -f -v

RUN  apt-get update \
  && apt-get install -y wget unzip \
  && rm -rf /var/lib/apt/lists/*

RUN apt-get update \ 
  && apt-get install -y libglib2.0-0 \
  libnss3 \
  libgconf-2-4 \
  libfontconfig1 \
  libxcb1
RUN apt-get update && apt-get install -y \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libgtk-4-1 \
    libu2f-udev \
    libvulkan1 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# install chrome directly
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN dpkg -i google-chrome-stable_current_amd64.deb

# Install dependencies
RUN apt-get install -f


# 필요한 Python 패키지 설치
RUN pip install selenium fastapi uvicorn Pillow openpyxl pandas beautifulsoup4 tqdm gspread gspread-dataframe

# 환경 변수 설정
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
RUN apt-get update -y && apt-get install -y xvfb


# 컨테이너 내에서 작업할 디렉토리 생성
WORKDIR /code

# capture.py와 같은 스크립트를 복사하여 컨테이너 내부에 추가
# 필요한 패키지 및 라이브러리 설치

CMD ["bash", "-c", "Xvfb :99 -ac & export DISPLAY=:99"]
