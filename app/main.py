# Import FastAPI
from fastapi import FastAPI, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import (
    NoSuchElementException,
    ElementNotInteractableException,
)
import time, openpyxl, os, re, copy
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
import gspread
from gspread_dataframe import set_with_dataframe
from requests import get

# create the app
import smtplib  # SMTP 라이브러리
from string import Template  # 문자열 템플릿 모듈
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

json_file_path = "/code/app/mailing-421207-c0e8e568b22a.json"
gc = gspread.service_account(json_file_path)
spreadsheet_url = "https://docs.google.com/spreadsheets/d/135wSfAC7Rp9ct3bbvxC090AVqy4La4c5sTDl1B0l26A/edit#gid=0"
doc = gc.open_by_url(spreadsheet_url)
# worksheet_crawling = doc.worksheet('크롤링병원')
worksheet_design = doc.worksheet("디자인폼")
worksheet_title = doc.worksheet("이메일제목")

# bxpy itkh xbia jmwh
app = FastAPI()

# 모든 도메인에서의 접근을 허용할 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 모든 도메인에서 접근 가능
    allow_credentials=True,
    allow_methods=["*"],  # 모든 메소드 허용
    allow_headers=["*"],  # 모든 헤더 허용
)

SETTING = {
    "SERVER": "smtp.daum.net",  # SMTP 서버 주소
    "PORT": 465,
    "USER": "zgai@daum.net",
    "PASSWORD": "weven00#!!",
}


@app.get("/corp_introduce_crawler_test")
def corp_introduce_crawler_test(url: str):
    url = url
    print(url, "<==========")
    driver = create_chrome_driver()
    driver.get(url)

    iframes = driver.find_elements(By.CSS_SELECTOR, "iframe")
    print(len(iframes), "<===개수")
    for iframe in iframes:
        print(iframe, "<=============")
        if iframe.get_attribute("name") == "mainFrame":
            driver.switch_to.frame(iframe)
            time.sleep(2)
            inner_iframe = driver.find_elements(By.TAG_NAME, "iframe")
            html_content = driver.page_source
            htmlObj = BeautifulSoup(html_content, "html.parser")
            with open("output.html", "w", encoding="utf-8") as file:
                file.write(str(htmlObj))
                file.write("<!--여기가 주석이다.-->")
                print("첫번쨰 프린트")
            for j in inner_iframe:
                print(len(inner_iframe), "<======inner 개수")
                try:
                    driver.switch_to.frame(j)
                    time.sleep(2)
                    print("sucess switch child frame")
                    html_content = driver.page_source
                    htmlObj = BeautifulSoup(html_content, "html.parser")
                    print(htmlObj, "<========htmlObj")
                    if htmlObj is not None:
                        with open("output.html", "a", encoding="utf-8") as file:
                            file.write(str(htmlObj))
                            file.write("<!--여기가 주석이다.-->")
                    # driver.switch_to.parent_frame()
                except:
                    print("<========except")
                    driver.switch_to.frame(j)
                    html_content = driver.page_source
                    htmlObj = BeautifulSoup(html_content, "html.parser")
                    print(htmlObj, "<========htmlObj")
                    pass

    driver.quit()
    file.close()
    return "sucess"


@app.get("/mailing")
def send_email():
    # 단일 전송 test
    # Gmail 계정 설정

    # server = smtplib.SMTP('smtp.gmail.com', 465)
    # server.starttls()
    # server.login(gmail_user, gmail_password)
    # server.login(gmail_user, gmail_password)
    # text = msg.as_string()
    # server.sendmail(gmail_user, to_email, text)
    # server.quit()
    # test
    # msg = MIMEMultipart()
    # msg['From'] = 'zgai@zgai.ai'
    # msg['To'] = 'yug6789@naver.com'
    # msg['Subject'] = title
    # msg.attach(MIMEText(html_body, 'html'))
    # server = smtplib.SMTP_SSL(SETTING['SERVER'], SETTING['PORT'])
    # server.login(SETTING['USER'], SETTING['PASSWORD'])
    # server.sendmail(SETTING['USER'], 'yug6789@naver.com', msg.as_string())
    # server.quit()
    # return JSONResponse(status_code=200, content={"message": "Email sent successfully to " + 'yug6789@naver.com'})
    # google logic
    # gmail_user = 'rmjinsan@gmail.com' # 보내는 사람 구글 이메일
    # gmail_password = 'bxpy itkh xbia jmwh'  # 앱 비밀번호

    # kakao_daum_user = 'zgai@kakao.com'
    # kakao_daum_password = 'weven00#!!'
    # msg = MIMEMultipart()
    # msg['From'] = gmail_user
    # msg['From'] = SETTING.get('USER')
    # msg['To'] = to_email
    # msg['Subject'] = subject
    # kakao logic
    # 이메일 구성
    email = worksheet_crawling.col_values(2)
    names = worksheet_crawling.col_values(1)
    title = worksheet_title.col_values(1)
    title = title[1]
    body = worksheet_design.col_values(1)
    html_body = "\n".join(body)
    print(html_body, "<================body")
    print(
        title,
        "<====title\n",
    )

    for name, to_email in zip(names[1:], email[1:]):
        msg = MIMEMultipart()
        msg["From"] = "zgai@zgai.ai"
        msg["To"] = to_email
        msg["Subject"] = title
        msg.attach(MIMEText(html_body, "html"))

        # 이메일 서버를 통해 이메일 전송
        try:
            server = smtplib.SMTP_SSL(SETTING["SERVER"], SETTING["PORT"])
            server.login(SETTING["USER"], SETTING["PASSWORD"])
            server.sendmail(SETTING["USER"], to_email, msg.as_string())
            server.quit()
            return JSONResponse(
                status_code=200,
                content={"message": "Email sent successfully to " + to_email},
            )
        except Exception as e:
            return JSONResponse(
                status_code=500,
                content={"message": "Failed to send email", "error": str(e)},
            )


@app.get("/crawler")
def datacrwaling(source: str, option_num: int):
    driver = create_chrome_driver()
    # option은 마케팅팀의 요청에 따라 추가 예정  0 : naver, 1 : cafe24
    option = [
        "https://www.naver.com",
        "https://d.cafe24.com/category?display=PTWD&product_type_code=PTWD&stype=name&miprice=0&maprice=99%2C999%2C999&searchPrice=&searchDate=all&s=&order=HIT_DESC",
    ]

    # 5개의 목록중에 없는 데이터는 null로 입력
    data = {"Name": [], "Email": [], "Phone": [], "URL": []}
    if option_num == 0:
        driver.get(option[option_num])
        brandHrefs = None
        link_titHrefs = None
        lnk_headHrefs = None
        siteHref = None
        powerHrefs = None
        trendHrefs = None
        try:
            input_element = WebDriverWait(driver, 5).until(
                EC.visibility_of_element_located(
                    (By.CSS_SELECTOR, "input.search_input")
                )
            )
            input_element.click()
            input_element.send_keys(source)
            button = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "btn_search"))
            )
            button.click()
            print("button click control sucess.")
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            links = WebDriverWait(driver, 3).until(
                EC.visibility_of_all_elements_located((By.CLASS_NAME, "link_tit"))
            )
            link_titHrefs = [link.get_attribute("href") for link in links]

            links = WebDriverWait(driver, 3).until(
                EC.visibility_of_all_elements_located((By.CLASS_NAME, "lnk_head"))
            )
            lnk_headHrefs = [link.get_attribute("href") for link in links]

            if link_titHrefs != None and lnk_headHrefs != None:
                siteHref = link_titHrefs + lnk_headHrefs
            elif link_titHrefs != None:
                siteHref = link_titHrefs
            elif lnk_headHrefs != None:
                siteHref = lnk_headHrefs

            links = WebDriverWait(driver, 3).until(
                EC.visibility_of_all_elements_located((By.CLASS_NAME, "lnk_thumb"))
            )
            powerHrefs = [link.get_attribute("href") for link in links]

            links = WebDriverWait(driver, 10).until(
                EC.visibility_of_all_elements_located((By.CLASS_NAME, "lnk_thumb"))
            )

            powerHrefs = [link.get_attribute("href") for link in links]

            # links = WebDriverWait(driver, 3).until(
            #     EC.visibility_of_all_elements_located((By.CLASS_NAME, "title_link"))
            # )
            # trendHrefs = [link.get_attribute('href') for link in links]

            # links = driver.find_elements(By.CSS_SELECTOR, "[class*='fds-comps']")
            # brandHrefs = [link.get_attribute('href') for link in links]
            # brandHrefs = list(set(brandHrefs))

            links = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//a[contains(@href, 'ad.search.naver.com')]")
                )
            )
            if not isinstance(links, list):
                links = [links]
            print(links, "links")
            more_click = [link.get_attribute("href") for link in links]
            print(more_click, "more_click")

            if len(more_click) == 1:
                i = 0
                print(f"Multiple links found: {len(more_click)}")
                # Example: Click all found links or handle them as needed
                collected_urls = []
                link_tit_Hrefs = []
                time.sleep(1)
                for link in more_click:
                    driver.get(link)
                    print(f"get start {link} {i} \n")
                    time.sleep(1)
                    link_tit_links = WebDriverWait(driver, 3).until(
                        EC.visibility_of_all_elements_located(
                            (By.CLASS_NAME, "tit_wrap")
                        )
                    )
                    link_tit_Hrefs = [
                        link.get_attribute("href") for link in link_tit_links
                    ]

                    pagination_links = WebDriverWait(driver, 10).until(
                        EC.visibility_of_all_elements_located(
                            (By.CSS_SELECTOR, ".paginate a")
                        )
                    )
                    collected_urls = [
                        link.get_attribute("href") for link in pagination_links
                    ]

                    for link in collected_urls:
                        driver.get(link)
                        WebDriverWait(driver, 10).until(
                            lambda driver: driver.execute_script(
                                "return document.readyState"
                            )
                            == "complete"
                        )
                        link_tit_links = WebDriverWait(driver, 3).until(
                            EC.visibility_of_all_elements_located(
                                (By.CLASS_NAME, "tit_wrap")
                            )
                        )

                        linktitHrefs = [
                            link.get_attribute("href") for link in link_tit_links
                        ]
                        powerHrefs.extend(linktitHrefs)

                        time.sleep(1)

                    i += 1
                    powerHrefs.extend(link_tit_Hrefs)
                print(powerHrefs, "\npowerHrefs\n")
            elif len(more_click) > 1:

                print(f"Multiple links found: {len(more_click)}")
                # Example: Click all found links or handle them as needed
                collected_urls = []
                link_tit_Hrefs = []
                print(more_click)
                time.sleep(1)
                for link in more_click:

                    driver.get(link)
                    WebDriverWait(driver, 10).until(
                        lambda driver: driver.execute_script(
                            "return document.readyState"
                        )
                        == "complete"
                    )
                    time.sleep(1)
                    print(f"get start {link} {i} \n")
                    link_tit_links = WebDriverWait(driver, 3).until(
                        EC.visibility_of_all_elements_located(
                            (By.CLASS_NAME, "tit_wrap")
                        )
                    )
                    link_tit_Hrefs = [
                        link.get_attribute("href") for link in link_tit_links
                    ]

                    pagination_links = WebDriverWait(driver, 10).until(
                        EC.visibility_of_all_elements_located(
                            (By.CSS_SELECTOR, ".paginate a")
                        )
                    )
                    time.sleep(1)
                    for link in pagination_links:
                        collected_urls.append(link.get_attribute("href"))

                    for link in collected_urls:
                        driver.get(link)
                        print(f"start detail url {link}")
                        WebDriverWait(driver, 10).until(
                            lambda driver: driver.execute_script(
                                "return document.readyState"
                            )
                            == "complete"
                        )
                        print("complete")
                        link_tit_links = WebDriverWait(driver, 3).until(
                            EC.visibility_of_all_elements_located(
                                (By.CLASS_NAME, "tit_wrap")
                            )
                        )
                        linktitHrefs = [
                            link.get_attribute("href") for link in link_tit_links
                        ]
                        powerHrefs.extend(linktitHrefs)
                        time.sleep(1)

            else:
                print("No links found.")

            powerHrefs.extend(link_tit_Hrefs)
            print(powerHrefs, "\npowerHrefs\n")
        except Exception as e:
            print(f"An error occurred: {str(e)}")
        finally:
            driver.quit()

            # naver 기준 / "link_tit" href 수집, "사이트 일반 홍보"
        try:
            print("\n====================siteHref 시작===========================")
            for href in siteHref:
                if href != None:
                    driver = create_chrome_driver()
                    driver.get(href)
                    time.sleep(1)
                    # 페이지가 완전히 로드되기를 기다립니다.
                    WebDriverWait(driver, 10).until(
                        lambda driver: driver.execute_script(
                            "return document.readyState"
                        )
                        == "complete"
                    )

                    footer_elements = driver.find_elements(
                        By.CSS_SELECTOR, "[id*='footer'], .footer, footer"
                    )
                    if footer_elements:
                        footerList = []
                        for element in footer_elements:
                            footerList.append(element.text)
                        if footerList:
                            footer_crawl = site_text_crawling(footerList, href)
                            append_data(data, footer_crawl)

                    time.sleep(1)
                    iframes = driver.find_elements(By.CSS_SELECTOR, "iframe")
                    for iframe in iframes:
                        name = []
                        text = []
                        corpList = []
                        corp_crawl = {}
                        if iframe.get_attribute("name") == "mainFrame":

                            driver.switch_to.frame("mainFrame")
                            time.sleep(1)
                            # 사업정보 불러오기
                            overlays = ".info_wrp"
                            corpData = driver.find_elements(By.CSS_SELECTOR, overlays)
                            for corp in corpData:
                                corpList.append(corp.text)
                            if corpList:
                                corp_crawl = corp_text_crawling(corpList, href)
                                append_data(data, corp_crawl)

                            # 페이지 소스를 가져옵니다.
                            overlays = ".nick"
                            nick = driver.find_elements(By.CSS_SELECTOR, overlays)

                            for nickname in nick:
                                name.append(nickname.text)
                            name = list(set(name))
                            name = [item for item in name if item.strip()]
                            name = name[0]

                            overlays = ".itemfont.col"
                            stringData = driver.find_elements(By.CSS_SELECTOR, overlays)
                            for strData in stringData:
                                text.append(strData.text)
                            if text:
                                blog_crawl = blog_text_crawling(text, href, name)
                                append_data(data, blog_crawl)

                            time.sleep(1)
                            driver.quit()  # 웹드라이버를 종료합니다.

                        else:
                            pass

                    time.sleep(1)
                    driver.quit()
        except Exception as e:
            print(f"An error occurred: {str(e)}")
        finally:
            driver.quit()
        #     #naver 기준 / "link_tit" href 수집, "브랜드 파워블로그 등등 수집"
        try:
            print(
                len(powerHrefs),
                "\n====================powerHrefs 시작===========================",
            )
            for href in powerHrefs:
                if href is not None:
                    driver = None
                    try:
                        driver = create_chrome_driver()
                        driver.get(href)
                        WebDriverWait(driver, 10).until(
                            lambda driver: driver.execute_script(
                                "return document.readyState"
                            )
                            == "complete"
                        )
                        print("powerHref complete")
                        footer_elements = driver.find_elements(
                            By.CSS_SELECTOR, "[id*='footer'], .footer, footer"
                        )
                        footerList = [
                            element.text for element in footer_elements if element.text
                        ]
                        if footerList:
                            footer_crawl = site_text_crawling(footerList, href)
                            append_data(data, footer_crawl)
                        print("cycle success")
                    except Exception as e:
                        print(f"Error processing {href}: {e}")
                    finally:
                        if driver:
                            driver.quit()
                    time.sleep(1)  # 서버 부하를 줄이기 위해 각 요청 사이에 딜레이 추가
                    iframes = driver.find_elements(By.CSS_SELECTOR, "iframe")
                    for iframe in iframes:
                        name = []
                        text = []
                        corpList = []
                        corp_crawl = {}
                        if iframe.get_attribute("name") == "mainFrame":

                            driver.switch_to.frame("mainFrame")
                            time.sleep(1)
                            # 사업정보 불러오기
                            overlays = ".info_wrp"
                            corpData = driver.find_elements(By.CSS_SELECTOR, overlays)
                            for corp in corpData:
                                corpList.append(corp.text)
                            if corpList:
                                corp_crawl = corp_text_crawling(corpList, href)
                                append_data(data, corp_crawl)

                            # 페이지 소스를 가져옵니다.
                            overlays = ".nick"
                            nick = driver.find_elements(By.CSS_SELECTOR, overlays)

                            for nickname in nick:
                                name.append(nickname.text)
                            name = list(set(name))
                            name = [item for item in name if item.strip()]
                            name = name[0]

                            overlays = ".itemfont.col"
                            stringData = driver.find_elements(By.CSS_SELECTOR, overlays)
                            for strData in stringData:
                                text.append(strData.text)
                            if text:
                                blog_crawl = blog_text_crawling(text, href, name)
                                append_data(data, blog_crawl)

                            time.sleep(1)
                            driver.quit()  # 웹드라이버를 종료합니다.

                        else:
                            pass
            # print(data,"<=========================data")

        except Exception as e:
            print(f"An error occurred: {str(e)}")
        finally:
            driver.quit()

        # try:
        #     print("\n====================trendHrefs 시작===========================")
        #     for href in trendHrefs:
        #         if href != None:
        #             driver = create_chrome_driver()
        #             driver.get(href)
        #             time.sleep(1)
        #             # 페이지가 완전히 로드되기를 기다립니다.
        #             WebDriverWait(driver, 10).until(
        #                 lambda driver: driver.execute_script('return document.readyState') == 'complete'
        #             )
        #             footer_elements = driver.find_elements(By.CSS_SELECTOR, "[id*='footer'], .footer, footer")
        #             if footer_elements:
        #                 footerList = []
        #                 for element in footer_elements:
        #                     footerList.append(element.text)
        #                 if footerList:
        #                     footer_crawl = site_text_crawling(footerList, href)
        #                     append_data(data, footer_crawl)

        #             time.sleep(1)
        #             iframes = driver.find_elements(By.CSS_SELECTOR,"iframe")
        #             for iframe in iframes:
        #                 name = []
        #                 text = []
        #                 corpList = []
        #                 corp_crawl = {}
        #                 if iframe.get_attribute('name') == 'mainFrame':

        #                     driver.switch_to.frame('mainFrame')
        #                     time.sleep(1)
        #                     # 사업정보 불러오기
        #                     overlays = ".info_wrp"
        #                     corpData = driver.find_elements(By.CSS_SELECTOR, overlays)
        #                     for corp in corpData:
        #                         corpList.append(corp.text)
        #                     if corpList:
        #                         corp_crawl = corp_text_crawling(corpList, href)
        #                         append_data(data, corp_crawl)

        #                     # 페이지 소스를 가져옵니다.
        #                     overlays = ".nick"
        #                     nick = driver.find_elements(By.CSS_SELECTOR, overlays)

        #                     for nickname in nick:
        #                         name.append(nickname.text)
        #                     name = list(set(name))
        #                     name = [item for item in name if item.strip()]
        #                     name = name[0]

        #                     overlays = ".itemfont.col"
        #                     stringData = driver.find_elements(By.CSS_SELECTOR, overlays)
        #                     for strData in stringData:
        #                         text.append(strData.text)
        #                     if text:
        #                         blog_crawl = blog_text_crawling(text, href, name)
        #                         append_data(data, blog_crawl)

        #                     time.sleep(1)
        #                     driver.quit()  # 웹드라이버를 종료합니다.

        #                 else:
        #                     pass

        # except Exception as e:
        #     print(f"An error occurred: {str(e)}")
        # finally:
        #     driver.quit()

        # naver 기준 / "link_tit" href 수집, "광고, 홍보, 사이트 url 수집"
        print("\n====================BrandHref 시작===========================")
        try:
            for href in brandHrefs:
                if href != None:
                    driver = create_chrome_driver()
                    driver.get(href)
                    time.sleep(1)
                    # 페이지가 완전히 로드되기를 기다립니다.
                    WebDriverWait(driver, 10).until(
                        lambda driver: driver.execute_script(
                            "return document.readyState"
                        )
                        == "complete"
                    )
                    footer_elements = driver.find_elements(
                        By.CSS_SELECTOR, "[id*='footer'], .footer, footer"
                    )
                    if footer_elements:
                        footerList = []
                        for element in footer_elements:
                            footerList.append(element.text)
                        if footerList:
                            footer_crawl = site_text_crawling(footerList, href)
                            append_data(data, footer_crawl)

                    time.sleep(1)
                    iframes = driver.find_elements(By.CSS_SELECTOR, "iframe")
                    for iframe in iframes:
                        name = []
                        text = []
                        corpList = []
                        corp_crawl = {}
                        if iframe.get_attribute("name") == "mainFrame":

                            driver.switch_to.frame("mainFrame")
                            time.sleep(1)
                            # 사업정보 불러오기
                            overlays = ".info_wrp"
                            corpData = driver.find_elements(By.CSS_SELECTOR, overlays)
                            for corp in corpData:
                                corpList.append(corp.text)
                            if corpList:
                                corp_crawl = corp_text_crawling(corpList, href)
                                append_data(data, corp_crawl)

                            # 페이지 소스를 가져옵니다.
                            overlays = ".nick"
                            nick = driver.find_elements(By.CSS_SELECTOR, overlays)

                            for nickname in nick:
                                name.append(nickname.text)
                            name = list(set(name))
                            name = [item for item in name if item.strip()]
                            name = name[0]

                            overlays = ".itemfont.col"
                            stringData = driver.find_elements(By.CSS_SELECTOR, overlays)
                            for strData in stringData:
                                text.append(strData.text)
                            if text:
                                blog_crawl = blog_text_crawling(text, href, name)
                                append_data(data, blog_crawl)

                            time.sleep(1)
                            driver.quit()  # 웹드라이버를 종료합니다.

                        else:
                            pass
        except Exception as e:
            print(f"An error occurred: {str(e)}")
        finally:
            driver.quit()

        time.sleep(1)

        driver.quit()

    if option_num == 1:
        try:
            driver.set_window_size(1920, 1080)
            print("cafe24 crawling")
            driver.get(option[option_num])

            # search_button = WebDriverWait(driver, 5).until(
            #         EC.visibility_of_element_located((By.CSS_SELECTOR, 'button.btn-srch-ipt'))
            #     )
            # search_button.click()
            # print("search_button click sucess")
            # WebDriverWait(driver, 10).until(
            #     lambda d: d.execute_script('return document.readyState') == 'complete'
            # )
            # print("page loading complete")
            # detail_search = WebDriverWait(driver, 5).until(
            # EC.visibility_of_element_located((By.CSS_SELECTOR, "span.in-txt[data-value='카테고리 전체']"))
            # )
            # detail_search.click()
            # print("detail_search button click sucess.")
            # driver.execute_script("document.querySelector('.dim').style.pointerEvents = 'none';")
            # try:
            #     source = source
            #     print(source,"<=====source")
            #     category_click = WebDriverWait(driver, 10).until(
            #         EC.element_to_be_clickable((By.XPATH, f"//span[contains(text(), '{source}')]/preceding-sibling::i[@class='chkbox']"))
            #         # EC.element_to_be_clickable((By.XPATH, f"//span[contains(text(), '{source}')]/ancestor::label"))
            #     )
            #     # WebDriverWait(driver, 20).until(
            #     #     EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), '패션의류')]/preceding-sibling::input[@type='checkbox']"))
            #     # ).click()
            #     time.sleep(3)
            #     category_click.click()
            #     time.sleep(3)
            #     print("click sucess")
            # except TimeoutException:
            #     driver.save_screenshot("debug_screenshot.png")
            #     print(f"Element with text {source} not found. Screenshot taken for debugging.")
            # # span 요소에서 상위의 label 요소로 이동한 다음 input 요소를 찾습니다.
            # checkbox = category_click.find_element(By.XPATH, "./ancestor::label/input[@type='checkbox']")
            # # 체크박스가 아직 체크되지 않았다면 체크합니다.
            # if not category_click.is_selected():
            #     category_click.click()
            # print("Checkbox is now checked.")
            # search = WebDriverWait(driver, 5).until(
            # EC.visibility_of_element_located((By.CSS_SELECTOR, "span.txt-sm"))
            # )
            # search.click()
            print("detail search sucess")

            before_h = driver.execute_script("return document.body.scrollHeight")

            # 무한 스크롤
            i = 0
            while True:
                driver.set_window_size(1920, 1080)
                # 맨 아래로 스크롤을 내린다.
                time.sleep(8)
                # driver.save_screenshot("debug_screenshot"+str(i)+".png")
                # driver.find_elements(By.CSS_SELECTOR, "div.ui-card-list").send_keys(Keys.END)

                # 스크롤 사이 페이지 로딩 시간
                driver.execute_script(
                    "window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth'});"
                )

                time.sleep(8)

                # 스크롤 후 높이

                after_h = driver.execute_script("return document.body.scrollHeight")

                driver.execute_script(f"window.scrollTo(0, {after_h} - 1000);")
                driver.execute_script(f"window.scrollTo(0, {after_h});")
                time.sleep(3)
                driver.execute_script(
                    "window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth'});"
                )
                driver.execute_script(
                    "window.scrollTo({ top: document.body.scrollHeight + 500, behavior: 'smooth'});"
                )
                time.sleep(8)

                after_h = driver.execute_script("return document.body.scrollHeight")

                # driver.save_screenshot("debug_screenshot_downscroll"+str(i)+".png")
                i += 1
                if after_h == before_h:
                    break
                before_h = after_h

            print("scroll finished")

            WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.CLASS_NAME, "items-wrap"))
            )

            # items-wrap 클래스를 가진 모든 section 태그 찾기
            sections = driver.find_elements(By.CLASS_NAME, "items-wrap")

            # 각 section에서 첫 번째 a 태그의 href 속성 추출
            links = []
            for section in sections:
                # section 내에서 첫 번째 'a' 태그를 찾고 그의 'href' 속성을 가져옵니다.
                link = section.find_element(By.CSS_SELECTOR, "a.link").get_attribute(
                    "href"
                )
                links.append(link)
            # 추출된 모든 링크 출력
            print(links, "\n========>", len(links))
            driver.quit()
            name = None
            phone = None
            email = None
            for link in links:
                try:
                    driver = create_chrome_driver()
                    driver.get(link)
                    # 모든 info-content 찾기
                    info_contents = driver.find_elements(
                        By.CSS_SELECTOR, "div.info-content"
                    )
                    # 두 번째 info-content 접근
                    if len(info_contents) > 1:
                        second_info = info_contents[1]

                        try:
                            name = second_info.find_element(
                                By.CSS_SELECTOR, "p:nth-of-type(1) em"
                            ).text
                        except NoSuchElementException:
                            name = None
                        data["Name"].append(name)

                        try:
                            phone = second_info.find_element(
                                By.CSS_SELECTOR, "p:nth-of-type(2) em"
                            ).text
                        except NoSuchElementException:
                            phone = None
                        data["Phone"].append(phone)

                        try:
                            email = second_info.find_element(
                                By.CSS_SELECTOR, "p:nth-of-type(3) em"
                            ).text
                        except NoSuchElementException:
                            email = None
                        data["Email"].append(email)

                        data["URL"].append(link)

                except Exception as e:
                    print("An error occurred:", e)

                finally:
                    if driver:
                        # 작업 완료 후 드라이버 종료
                        driver.quit()

        except Exception as e:
            print("An error occurred:", e)

        finally:
            # 작업 완료 후 드라이버 종료
            driver.quit()

    if option_num == 0:
        doc_source = "크롤링병원"
    elif option_num == 1:
        doc_source = "카페24크롤링"
    print(data, "<======data")
    print(
        len(data["Name"]),
        "name\n",
        len(data["Email"]),
        "email\n",
        len(data["Phone"]),
        "phone",
    )
    assert all(
        len(lst) == len(data["Name"]) for lst in data.values()
    ), "Data length mismatch in lists"
    df = pd.DataFrame(data)
    None_df = df[(df["Name"].isnull()) & (df["Email"].isnull())].index
    # 공백 제거, 대소문자 통일
    # df['Name'] = df['Name'].str.strip().str.lower()
    df["Name"] = df["Name"].astype(str).str.strip().str.lower()

    # 'Name' 열의 값 빈도 세기
    name_counts = df["Name"].value_counts()

    df = df.drop(None_df, axis=0)
    # df = df.drop_duplicates(['Email'])
    json_file_path = "/code/app/mailing-421207-c0e8e568b22a.json"
    gc = gspread.service_account(json_file_path)
    spreadsheet_url = "https://docs.google.com/spreadsheets/d/135wSfAC7Rp9ct3bbvxC090AVqy4La4c5sTDl1B0l26A/edit#gid=0"
    doc = gc.open_by_url(spreadsheet_url)

    worksheet = doc.worksheet(doc_source)

    # 데이터를 삽입할 행 계산
    existing_rows = worksheet.row_count
    next_row = existing_rows + 1

    # 스프레드시트 크기 확인 및 행 추가
    if next_row > worksheet.row_count:
        # 실제 데이터가 존재하는 마지막 행을 확인하고 필요한 만큼 행을 추가
        last_row_with_data = len(worksheet.get_all_values())
        worksheet.add_rows(next_row - last_row_with_data)

    set_with_dataframe(worksheet, df, row=next_row, include_column_header=False)
    # for i, row in enumerate(name_counts.iteritems(), start=2):  # start=2는 스프레드시트의 2번째 행부터 시작
    #     worksheet.update_cell(i, 6, row.Name)  # 'F'열은 6번째 열
    #     worksheet.update_cell(i, 7, row.Count)

    print(df)
    # python으로 excel 출력하는 부분 작업
    file_name = "Hospital" + source + ".xlsx"
    df.to_excel(file_name, index=False, engine="openpyxl")

    file_path = os.path.abspath(file_name)
    print("Do Finish Process")
    if os.path.exists(file_path):
        return FileResponse(
            path=file_path,
            filename=file_name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    return {"error": "File not found."}


def site_text_crawling(string, href):
    corpName_pattern = (
        r"(?:(?:COPYRIGHT|©)\s*[\s\(c\)]*\s*©?\s*)([\w\s&.-]+?)(?:\s*All rights|\.|©)"
    )
    email_pattern = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
    phone_pattern = r"\(?\d{3}\)?[-.\s]?\d{4}[-.\s]?\d{4}"
    patterns = [
        r"회사명\s*:\s*([\w\s&.-]+)",  # 회사명: 다음에 오는 문자열
        r"상호명\s*:\s*([\w\s&.-]+)",  # 상호명: 다음에 오는 문자열
        r"브랜드명\s*:\s*([\w\s&.-]+)",  # 브랜드명: 다음에 오는 문자열
        r"대표자\s*:\s*([\w\s&.-]+)",  # 대표자: 다음에 오는 문자열
        r"주식회사\s*([\w\s&.-]+)",  # 주식회사 다음에 오는 문자열
        r"(?:COPYRIGHT|©)\s*[\s\(c\)]*\s*©?\s*([\w\s&.-]+?)(?:\s*All rights|\.|©)",  # 저작권 정보 다음에 오는 문자열
    ]

    # 결과를 저장할 리스트
    company_names = set()

    href = href
    corpData = None
    emailData = None
    phoneData = None

    for text in string:

        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                # 공백 제거 및 결과 저장
                company_names.add(match.strip())

        if not corpData:
            corps = re.findall(corpName_pattern, text)
            if corps:
                corpData = corps[0]

        if not emailData:
            emails = re.findall(email_pattern, text)
            if emails:
                emailData = emails[0]

        if not phoneData:
            phones = re.findall(phone_pattern, text)
            if phones:
                phoneData = phones[0]

    return {"name": corpData, "email": emailData, "phone": phoneData, "URL": href}


def blog_text_crawling(string, href, nick):
    email_pattern = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
    phone_pattern = r"\(?\d{3}\)?[-.\s]?\d{4}[-.\s]?\d{4}"

    emailData = None
    phoneData = None
    href = href
    for text in string:
        if not emailData:  # 이메일 데이터가 아직 설정되지 않았다면
            emails = re.findall(email_pattern, text)
            if emails:
                emailData = emails[0]  # 첫 번째 이메일만 저장

        if not phoneData:  # 전화번호 데이터가 아직 설정되지 않았다면
            phones = re.findall(phone_pattern, text)
            if phones:
                phoneData = phones[0]  # 첫 번째 전화번호만 저장

    print(nick, "nick")

    return {"name": nick, "email": emailData, "phone": phoneData, "URL": href}


def corp_text_crawling(text_list, href):
    # 패턴 정의
    print(text_list, "<====corp text_list")
    email_pattern = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
    phone_pattern = r"\b\d{2,3}[-.\s]?\d{3,4}[-.\s]?\d{4}\b"
    name_pattern = r"상호\s*:\s*([^\n]+)"

    # 결과 변수 초기화
    email = None
    phone = None
    name = None
    href = href

    # 텍스트 리스트 검색
    for item in text_list:
        if "메일" in item:
            email_match = re.search(email_pattern, item)
            if email_match:
                email = email_match.group()

        if "전화" in item:
            phone_match = re.search(phone_pattern, item)
            if phone_match:
                phone = phone_match.group()

        if "상호" in item:
            name_match = re.search(name_pattern, item)
            if name_match:
                name = name_match.group(1)  # 첫 번째 캡처 그룹을 사용

    return {"name": name, "email": email, "phone": phone, "URL": href}


def list_set(Data):
    return list(set(Data))


def append_data(data, result):
    # 각 키에 대해 결과에서 값을 가져오거나 None을 할당
    data["Name"].append(result.get("name", None))
    data["Email"].append(result.get("email", None))
    data["Phone"].append(result.get("phone", None))
    data["URL"].append(result.get("URL", None))
    return "sucess"


def create_chrome_driver():
    options = webdriver.ChromeOptions()
    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36"
    options.add_argument("user-agent=" + user_agent)
    options.add_argument("--lang=ko-KR")
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_experimental_option("excludeSwitches", ["disable-popup-blocking"])

    # 드라이버 인스턴스 생성
    driver = webdriver.Chrome(options=options)
    return driver
