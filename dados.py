from datetime import date, timedelta, datetime
from dotenv import load_dotenv
import os
import json

load_dotenv()

url = os.environ.get("URL_SGA")

#====================================================================================================================================================#

#                   LISTAS                    #

list_month = list()
list_year = list()
list_prev = list()

#====================================================================================================================================================#

#                   CONFIGURAÇÃO DATA/MÊS                   #

current_date = datetime.now() 
	

start_date = current_date - timedelta(days=60)

today = date.today()
today_f = today.strftime("%d/%m/%Y")
today_week = today.weekday() 
  
yesterday = today + timedelta(-1)
yesterday_f = yesterday.strftime("%d/%m/%Y")
yesterday_week = yesterday.weekday()

twoweeks = today + timedelta(-15)
twoweeks_f = twoweeks.strftime("%d/%m/%Y")
twoweeks_week = twoweeks.weekday()


nov = yesterday_f
nov_end = today_f
nov_weeks = twoweeks_f

month = datetime.now().month
year = datetime.now().year

month = month - 1

if month in range (1, 10):
  
  month = '0' + str(month)
  
elif month in range (11, 12):
  
  month = month

bill = f"{month}/{year}"

#====================================================================================================================================================================#

#                   CONFIGURAÇÃO ANO                    #

mes = 6

for i in range(mes, -1, -1):
  
  previous_date = start_date - timedelta(days=30 * i)
  
  month_prev = previous_date.month
  year_prev = previous_date.year
  
  if month_prev == 0:
        
    month_prev = 12
        
  elif month_prev == -1:
        
    month_prev = 11
        
  list_month.append(month_prev)
  list_year.append(year_prev)  
 
mes_ = mes + 1
 
bill_prev = [(list_month[i], list_year[i]) for i in range(mes_)]

formatted_months = []
for mes, ano in bill_prev:
    formatted_months.append(str(f"{mes:02d}/{ano}"))
    
# for month_prev_six in formatted_months:
#     print(month_prev_six)

#====================================================================================================================================================================#

#                    DADOS SERVIDOR                     #

db = os.getenv('DBNAME')
user = os.getenv('DBUSER')
password = os.getenv('DBPASS')
host = os.getenv('DBHOST')

#====================================================================================================================================================================#

# HEADERS .ENV

env_alianze = json.loads(os.environ.get("HEADERS_ALIANZE"))

env_ancore = json.loads(os.environ.get("HEADERS_ANCORE"))

env_autobras = json.loads(os.environ.get("HEADERS_AUTOBRAS"))

env_brtruck = json.loads(os.environ.get("HEADERS_BRTRUCK"))

env_coopera = json.loads(os.environ.get("HEADERS_COOPERA"))

env_elevaton = json.loads(os.environ.get("HEADERS_ELEVATON"))

env_exodo = json.loads(os.environ.get("HEADERS_EXODO"))

env_fortis = json.loads(os.environ.get("HEADERS_FORTIS"))

env_grupo_canaa = json.loads(os.environ.get("HEADERS_GRUPO_CANAA"))

env_gta = json.loads(os.environ.get("HEADERS_GTA"))

env_localize = json.loads(os.environ.get("HEADERS_LOCALIZE"))

env_master = json.loads(os.environ.get("HEADERS_MASTER"))

env_movidas = json.loads(os.environ.get("HEADERS_MOVIDAS"))

env_pc = json.loads(os.environ.get("HEADERS_PC"))

env_raio = json.loads(os.environ.get("HEADERS_RAIO"))

env_rogers = json.loads(os.environ.get("HEADERS_ROGERS"))

env_servcar = json.loads(os.environ.get("HEADERS_SERVCAR"))

env_speed = json.loads(os.environ.get("HEADERS_SPEED"))

env_tech_protege = json.loads(os.environ.get("HEADERS_TECH"))

env_torre = json.loads(os.environ.get("HEADERS_TORRE"))

env_unik = json.loads(os.environ.get("HEADERS_UNIK"))

env_valle = json.loads(os.environ.get("HEADERS_VALLE"))

#====================================================================================================================================================================#

#                   TOKENS ASSOCIAÇÕES                    #

#TOKEN ANCORE

headers_ancore = env_ancore

#TOKEN RAIO

headers_raio = env_raio

#TOKEN VALLE

headers_valle = env_valle

#TOKEN SPEED

headers_speed = env_speed

#TOKEN ELEVATON

headers_elevaton = env_elevaton

#TOKEN COOPERA

headers_coopera = env_coopera

#TOKEN PROTEGE CAR

headers_pc = env_pc

#TOKEN AUTOBRAS

headers_autobras = env_autobras

#TOKEN GTA

headers_gta = env_gta

#TOKEN EXODO

headers_exodo = env_exodo

#TOKEN BR TRUCK

headers_brtruck = env_brtruck

#TOKEN TORRE

headers_torre = env_torre

#TOKEN SERVCAR

headers_servcar = env_servcar

#TOKEN UNIK

headers_unik = env_unik

#TOKEN ALIANZE

headers_alianze = env_alianze

#TOKEN FORTIS

headers_fortis = env_fortis

#TOKEN GRUPO CANAA

headers_grupo_canaa = env_grupo_canaa

#TOKEN LOCALIZE

headers_localize = env_localize

#TOKEN MASTER

headers_master = env_master

#TOKEN MOVIDAS

headers_movidas = env_movidas

#TOKEN ROGERS

headers_rogers = env_rogers

#TOKEN TECH PROTEGE

headers_tech = env_tech_protege

#====================================================================================================================================================================#

