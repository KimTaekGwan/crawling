from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
import os, copy
import smtplib               # SMTP 라이브러리
from string import Template  # 문자열 템플릿 모듈
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from email.mime.image     import MIMEImage

#bxpy itkh xbia jmwh
app = FastAPI()

# 모든 도메인에서의 접근을 허용할 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 모든 도메인에서 접근 가능
    allow_credentials=True,
    allow_methods=["*"],  # 모든 메소드 허용
    allow_headers=["*"],  # 모든 헤더 허용
)


@app.get('/mailing')
def send_email(subject, body, to_email):
    # Gmail 계정 설정
    gmail_user = 'rmjinsan@gmail.com' # 보내는 사람 구글 이메일
    gmail_password = 'bxpy itkh xbia jmwh'  # 앱 비밀번호

    # 이메일 구성
    msg = MIMEMultipart()
    msg['From'] = gmail_user
    msg['To'] = to_email
    msg['Subject'] = subject

    # 이메일 본문 추가
    msg.attach(MIMEText(body, 'plain'))

    # 이메일 서버를 통해 이메일 전송
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(gmail_user, gmail_password)
    text = msg.as_string()
    server.sendmail(gmail_user, to_email, text)
    server.quit()

# class EmailHTMLImageContent:
#     """e메일에 담길 이미지가 포함된 컨텐츠"""
#     def __init__(self, str_subject, str_image_file_name, str_cid_name, template, template_params):
#         """이미지파일(str_image_file_name), 컨텐츠ID(str_cid_name)사용된 string template과 딕셔너리형 template_params받아 MIME 메시지를 만든다"""
#         assert isinstance(template, Template)
#         assert isinstance(template_params, dict)
#         self.msg = MIMEMultipart()
        
#         # e메일 제목을 설정한다
#         self.msg['Subject'] = str_subject # e메일 제목을 설정한다
        
#         # e메일 본문을 설정한다
#         str_msg  = template.safe_substitute(**template_params) # ${변수} 치환하며 문자열 만든다
#         mime_msg = MIMEText(str_msg, 'html')                   # MIME HTML 문자열을 만든다
#         self.msg.attach(mime_msg)
        
#         # e메일 본문에 이미지를 임베딩한다
#         assert template.template.find("cid:" + str_cid_name) >= 0, 'template must have cid for embedded image.'
#         assert os.path.isfile(str_image_file_name), 'image file does not exist.'        
#         with open(str_image_file_name, 'rb') as img_file:
#             mime_img = MIMEImage(img_file.read())
#             mime_img.add_header('Content-ID', '<' + str_cid_name + '>')
#         self.msg.attach(mime_img)
        
#     def get_message(self, str_from_email_addr, str_to_eamil_addrs):
#         """발신자, 수신자리스트를 이용하여 보낼메시지를 만든다 """
#         mm = copy.deepcopy(self.msg)
#         mm['From'] = str_from_email_addr          # 발신자 
#         mm['To']   = ",".join(str_to_eamil_addrs) # 수신자리스트 
#         return mm

# class EmailSender:
#     """e메일 발송자"""
#     def __init__(self, str_host, num_port=25):
#         """호스트와 포트번호로 SMTP로 연결한다 """
#         self.str_host = str_host
#         self.num_port = num_port
#         self.ss = smtplib.SMTP(host=str_host, port=num_port)
#         # SMTP인증이 필요하면 아래 주석을 해제하세요.
#         #self.ss.starttls() # TLS(Transport Layer Security) 시작
#         #self.ss.login('계정명', '비밀번호') # 메일서버에 연결한 계정과 비밀번호
    
#     def send_message(self, emailContent, str_from_email_addr, str_to_eamil_addrs):
#         """e메일을 발송한다 """
#         cc = emailContent.get_message(str_from_email_addr, str_to_eamil_addrs)
#         self.ss.send_message(cc, from_addr=str_from_email_addr, to_addrs=str_to_eamil_addrs)
#         del cc
        
# @app.get("/mailing")
# def send_email_with_image(str_subject : str):
#     print("mailing start")
#     str_subject = str_subject
#     template_params       = {'NAME':'test'}
#     str_image_file_name   = 'data:image/jpeg;base64,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'
#     str_cid_name          = 'my_image1'
#     template = Template("""<html>
#                                 <head></head>
#                                 <body>
#                                     Hi ${NAME}.<br>
#                                     <img src="cid:my_image1"><br>
#                                     This is a test message.
#                                 </body>
#                             </html>""")
    
#     emailHTMLImageContent = EmailHTMLImageContent(str_subject, str_image_file_name, str_cid_name, template, template_params)

#     str_from_email_addr = 'RMjinsan@gmail.com' # 발신자
#     str_to_eamil_addrs  = ['yug6789@naver.com', 'yug6789@daum.com'] # 수신자리스트

 
#     email_sender = EmailSender('RMjinsan@gmail.com',25)
    
#     try:
#         EmailSender.send_message(emailHTMLImageContent, str_from_email_addr, str_to_eamil_addrs)
#     except Exception as error:
#         raise HTTPException(status_code=500, detail=str(error))

#     return {"message": "Email sent successfully"}

