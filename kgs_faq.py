import json
import pprint
import re
import time
from bs4 import BeautifulSoup
import requests
import datetime
import pprint
import boto3
import os
from dotenv import load_dotenv
from psycopg2 import sql
import psycopg2
import schedule

# import chromedriver_autoinstaller

def createFolder(directory):
  try:
    if not os.path.exists(directory):
      os.makedirs(directory)
  except OSError:
    print('Error: Creating directory. ' + directory)
def GetSearchGasSafetyFAQ():
  results=[]
  pageIndex=1
  endFlag=False
  fileNameList=[]
  while True:

    cookies = {
        'JSESSIONID': '82951E27E7102D916CF4E239480FC6E8.tomcat2',
    }
    
    headers = {
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
      'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
      'Content-Type': 'application/x-www-form-urlencoded',
      'Origin': 'https://www.kgs.or.kr',
      'Referer': 'https://www.kgs.or.kr/kgs/aaaa/board.do',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    }

    data = {
      'pageIndex': pageIndex,
      'etc1': "",
      'searchType': '1',
      'searchText': '',
    }
    
    baseUrl = 'https://www.kgs.or.kr/kgs/aaaa/board.do'
    
    while True:
      try:
        response = requests.post(
            baseUrl, 
            cookies=cookies, 
            headers=headers, 
            data=data,
            verify=False,
            allow_redirects=True,
            timeout=30
        )
        break
      except Exception as e:
        print(f"에러발생: {str(e)}")
        time.sleep(1)
    soup=BeautifulSoup(response.text, 'html.parser')
    # with open('source/kgs_faq.html', 'w', encoding='utf-8') as outfile:
    #   outfile.write(response.text)
    items=soup.find_all('section', class_='faq-list')
    print(len(items))
    if len(items)==0:
      print('글없음1')
      break
    for item in items:
      title=item.find('li',class_='text').get_text().strip()
      if title=="내용이 없습니다":
        print('글없음2')
        endFlag=True
        break
      for br in item.find_all('br'):
          br.replace_with('\n')
      contents = item.find('div', class_='faq_con').get_text().strip()
      indexNo=item.find('li',class_='number').get_text().strip()
      regiDate=item.find("li",class_="date").get_text().strip()
      category=item.find('li',class_="sort").get_text().strip()
      result={"title":title,"contents":contents,'category':category,'indexNo':indexNo,'regiDate':regiDate}
      
      # with open('source/kgs_faq.json', 'r', encoding='utf-8') as file:
      #     kgs_faq_data = json.load(file)
      
      kgs_faq_data={
        "KGS-FAQ": [
            {
                "metadata": {
                    "Type": "Kgs-faq",
                    "Source": "https://www.kgs.or.kr/kgs/aaaa/board.do",
                    "Author": "한국가스안전공사",
                    "CreationDate": "", 
                    "ModDate": "", 
                    "Category": "",
                    "FileName": ""
                },
                "data": {
                    "id": "",
                    "title": "",
                    "content": {
                        "questions": [
                        ],
                        "answers": [
                        ],
                        "questionDate": "",
                        "answerDate": ""
                    }
                }
            }
        ]
      }
      # pprint.pprint(kgs_faq_data)
      # 데이타 형태변환
      regiDate = datetime.datetime.strptime(regiDate, "%Y.%m.%d").strftime("%Y-%m-%d 00:00:00")
      kgs_faq_data['KGS-FAQ'][0]['metadata']['CreationDate'] = regiDate
      kgs_faq_data['KGS-FAQ'][0]['metadata']['ModDate'] = regiDate
      categoryList={
        '고압가스':'GAS001',
        '액화석유가스':'GAS002',
        '도시가스':'GAS003',
        '수소':'GAS004',
        '기타':'GAS005'
      }
      categoryCode = categoryList.get(category, "GAS005")  # Default to "000" if category not found
      kgs_faq_data['KGS-FAQ'][0]['metadata']['Category'] = categoryCode
      
      kgs_faq_data['KGS-FAQ'][0]['data']['title'] = title
      kgs_faq_data['KGS-FAQ'][0]['data']['content']['questionDate'] = regiDate
      kgs_faq_data['KGS-FAQ'][0]['data']['content']['answerDate'] = regiDate
      kgs_faq_data['KGS-FAQ'][0]['data']['id'] = indexNo
      
      def sanitize_filename(filename):
        # 파일명으로 사용할 수 없는 특수문자 리스트
        invalid_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
        
        # 공백은 언더스코어로 변경
        filename = filename.replace(' ', '_')
        
        # 특수문자 제거
        for char in invalid_chars:
            filename = filename.replace(char, '')
        
        # 추가적인 제어문자나 특수문자 제거
        filename = ''.join(char for char in filename if char.isprintable())
        
        # 파일명 길이 제한 (선택사항, S3는 1024바이트까지 지원)
        if len(filename.encode('utf-8')) > 225:  # 여유있게 225바이트로 제한
            filename = filename[:75]  # UTF-8에서 한글은 3바이트이므로 대략적으로 계산
        
        return filename


      sanitized_title = sanitize_filename(title)
      sanitized_category = sanitize_filename(category)
      sanitized_indexNo = sanitize_filename(indexNo)
      
      # Extract question and answer using regex
      question_match = re.search(r'\(질의내용\)(.*?)\(답변내용\)', contents, re.DOTALL)
      answer_match = re.search(r'\(답변내용\)(.*)', contents, re.DOTALL)
      
      if question_match and answer_match:
        question_text = question_match.group(1).strip()
        answer_text = answer_match.group(1).strip()
        
        # Split the text into lists
        questions = [line.strip() for line in question_text.split('\n') if line.strip()]
        answers = [line.strip() for line in answer_text.split('\n') if line.strip()]
      else:
          # Use title as the question and contents as the answer
        questions = [title]
        answers = [line.strip() for line in contents.split('\n') if line.strip()]
      kgs_faq_data['KGS-FAQ'][0]['data']['content']['questions'] = questions
      kgs_faq_data['KGS-FAQ'][0]['data']['content']['answers'] = answers

      file_name = f'tmp/{sanitized_title}_{sanitized_category}_{sanitized_indexNo}.json'
      fileNameList.append(file_name)
      results.append(kgs_faq_data)
      kgs_faq_data['KGS-FAQ'][0]['metadata']['FileName'] = file_name.replace("tmp/", "").replace(".json","")
      with open(file_name, 'w', encoding='utf-8') as outfile:
          json.dump(kgs_faq_data, outfile, ensure_ascii=False, indent=2)
      with open("results.json", "w",encoding='utf-8-sig') as file:
        json.dump(results, file, ensure_ascii=False, indent=2)
      
    if endFlag:
      print("더없음2")
      break
    print("pageIndex:",pageIndex)
    pageIndex+=1
    time.sleep(1)
  return fileNameList,results

# GetSearchGasSafetyFAQ()
def UploadImageToS3(file_path):
  # AWS 계정의 액세스 키와 시크릿 키를 설정합니다.

  aws_access_key_id=os.getenv('aws_access_key_id')
  aws_secret_access_key=os.getenv('aws_secret_access_key')
  region_name='ap-northeast-2'
  bucket_name='htc-ai-datalake'
  # S3 클라이언트를 생성합니다.
  s3_client = boto3.client(
      's3',
      aws_access_key_id=aws_access_key_id,
      aws_secret_access_key=aws_secret_access_key,
      region_name=region_name
  )
      
  s3_path = file_path.replace('tmp/', '')
  # Change the path to the desired directory
  timeNowMonth=datetime.datetime.now().strftime("%Y%m")
  desired_path = "collection/kgs-faq/{}/".format(timeNowMonth)
  try:
      response = s3_client.upload_file(
          Filename=file_path,
          Bucket=bucket_name,
          Key=desired_path + s3_path
      )
      print("파일 업로드 성공!")
  except Exception as e:
      print("파일 업로드 실패:", str(e))
      return None
      
def PrintS3FileNames():
    aws_access_key_id = os.getenv('aws_access_key_id')
    aws_secret_access_key = os.getenv('aws_secret_access_key')
    region_name='ap-northeast-2'
    bucket_name = 'htc-ai-datalake'
    prefix = "collection/kgs-faq"
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                print(obj['Key'])
        else:
            print("버킷에 파일이 없습니다.")
    except Exception as e:
        print("파일 목록을 가져오는 데 실패했습니다:", str(e))  
def DeleteS3FileNames():
    aws_access_key_id = os.getenv('aws_access_key_id')
    aws_secret_access_key = os.getenv('aws_secret_access_key')
    region_name = os.getenv('region_name')
    bucket_name = 'htc-ai-datalake'
    prefix = "collection/kgs-faq/"
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    try:
        # List all objects in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' in response:
            # Create a list of objects to delete
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            
            # Delete all objects
            s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={
                    'Objects': objects_to_delete,
                    'Quiet': False
                }
            )
            print(f"{len(objects_to_delete)}개의 파일이 삭제되었습니다.")
        else:
            print("버킷에 파일이 없습니다.")
    except Exception as e:
        print("작업 실패:", str(e))
def insert_dummy_data(inputDatas):
  # 데이터베이스 연결 정보
    initial_db_params = {
        'dbname': 'htc-aikr-prod',
        'user': 'postgres',
        'password': 'ddiMaster1!',
        'host': '127.0.0.1',
        'port': '5432'
    }
    try:
        # 데이터베이스에 연결
        connection = psycopg2.connect(**initial_db_params)
        cursor = connection.cursor()
        
        # 더미 데이터 삽입
        insert_query = """
            INSERT INTO "COLLECTION_DATA" ("NAME", "FILE_NAME", "FILE_PATH", "METHOD", "COLLECTION_ID")
            VALUES (%s, %s, %s, %s, %s)
        """
        # dummy_data = [
        #     ('name1', 'file1', '/path/to/file1', 'AUTO', 78),
        # ]
        
        datas=[]
        timeNowMonth=datetime.datetime.now().strftime("%Y%m")
        timeNow=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for inputData in inputDatas:
          name=inputData['KGS-FAQ'][0]['data']['title']+".json"
          file_name=inputData['KGS-FAQ'][0]['metadata']['FileName']
          file_path="collection\\kgs-faq\\{}".format(timeNowMonth,file_name)
          method='AUTO'
          collection_id='53'
          datas.append((name,file_name,file_path,method,collection_id))
        
        
        for data in datas:
            cursor.execute(insert_query, data)
        
        # 변경사항 커밋
        connection.commit()
        print("Dummy data inserted successfully.")
    
    except Exception as error:
        print(f"Error: {error}")
    
    finally:
        # 연결 닫기
        if connection:
            cursor.close()
            connection.close()

def job():
  # ================실행부분
  createFolder('tmp')
  # ==============데이터 가져오기
  load_dotenv()
  # 데이타 수집
  fileNameList,results=GetSearchGasSafetyFAQ()
  with open('source/fileNameList.json', 'w', encoding='utf-8') as outfile:
      json.dump(fileNameList, outfile, ensure_ascii=False, indent=2)
  
  # ===========S3 업로드
  hostingUrlList=[]
  for index,fileName in enumerate(fileNameList):
    print("{}/{}번째 업로드 중...".format(index+1,len(fileNameList)))
    try:
      hostingUrl=UploadImageToS3(fileName)
      hostingUrlList.append(hostingUrl)
    except Exception as e:
      print("업로드 실패:",fileName,"/ 에러:",e)
  #===========DB업로드
  insert_dummy_data(results)

schedule.every().monday.at("09:00").do(job)
print("처음엔 무조건 시작")
job()
while True:
    timeNow = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    print("현재시간은:", timeNow)
    schedule.run_pending()
    time.sleep(10)

#=====점검하기
# load_dotenv()
# PrintS3FileNames()
# DeleteS3FileNames()


