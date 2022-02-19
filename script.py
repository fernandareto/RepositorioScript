import pickle
import os
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import datetime 
import gspread
from dataclasses import dataclass
from datetime import datetime, timedelta
import gspread 
import pandas as pd
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import mysql.connector
import mysql
from mimetypes import MimeTypes
from pickle import TRUE
from re import T
from urllib import response
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from bs4 import BeautifulSoup
import io
import sys


####Codigo GOOGLE para obtener el servicio con el api de gmail###
def Create_Service(client_secret_file, api_name, api_version, *scopes):
    print(client_secret_file, api_name, api_version, scopes, sep='-')
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = [scope for scope in scopes[0]]
    print(SCOPES)

    cred = None

    pickle_file = f'token_{API_SERVICE_NAME}_{API_VERSION}.pickle'
    # print(pickle_file)

    if os.path.exists(pickle_file):
        with open(pickle_file, 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            cred = flow.run_local_server()

        with open(pickle_file, 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=cred)
        print(API_SERVICE_NAME, 'service created successfully')
        return service
    except Exception as e:
        print('Unable to connect.')
        print(e)
        return None

def convert_to_RFC_datetime(year=1900, month=1, day=1, hour=0, minute=0):
    dt = datetime.datetime(year, month, day, hour, minute, 0).isoformat() + 'Z'
    return dt

#Crea el servicio de gmail y el token de autenticaciòn para enviar correos desde la cuenta gmail 
CLIENT_SECRET_FILE = 'client_secret.json'
API_NAME = 'gmail'
API_VERSION = 'v1'
SCOPES = ['https://mail.google.com/']

service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

#autenticación para leer el archivo sheet de drive
gc = gspread.service_account(filename='proyecto-341313-f7fe5a0f69b4.json')
#abrir el archivo en drive donde se encuentran los datos
sh = gc.open("datos")

workshet = sh.get_worksheet(0)

#obtiene los valores del archivo en un dataframe
datos = workshet.get_all_values()

dataframe= pd.DataFrame(workshet.get_all_records())

#obtiene los datos necesarios del archivo sheet para realizar las busquedas
tabla= dataframe.iloc[:,[0,1,2,3,6]]

mails = (dataframe['contacto_proveedor'])
fechas = (dataframe['fecha_vencimiento'])
tercero = (dataframe['nombre_proveedor'])
identificador=(dataframe['identificador'])
serviciopro = (dataframe['servicio']) 

#conexión a la base de datos mysql que esta en la nube alojada en freesqldatabase y toma los id de los terceros que ya se notificaron en la primera ejecuciòn del script
conexion = mysql.connector.connect(host = 'sql10.freesqldatabase.com',port = 3306, user = 'sql10473104', password= 'WXqzNP2trR', database = 'sql10473104')
print(conexion)
cursorr = conexion.cursor()
cursorr.execute("SELECT identificador FROM proveedores_notificados")
identificadores=cursorr.fetchall()
listasincomas = str(identificadores)[1:-1]
print(listasincomas)


#Busca de los terceros que tienen su fecha de vencimiento de aoc en 10 días a partir del día que se corre el script
for index,date in enumerate(fechas):
    format = '%Y-%m-%d'
    daysexpiration = 10
    dateformat= datetime.strptime(date,"%d/%m/%Y").strftime(format)
    datetocompare = datetime.strptime(dateformat,format)
    identidadter = identificador[index]
    print (identidadter)
    
    if str(identidadter) not in listasincomas:
    
        if  datetocompare <= (datetime.today() + timedelta(daysexpiration)) and datetocompare > datetime.today():
            mailcontacto = mails[index]
            servicioter = serviciopro[index]   
            terceropro = tercero[index]  
            identidadter = identificador[index] 
            datesendmail = datetime.today().strftime("%Y-%m-%d")

            if mailcontacto !='':
        #envia el mensaje a los terceros que se encuentran dentro del plazo de 10 días para vencer su AOC        
                emailMsg = 'Su AoC Se encuentra proximo a vencer, por favor remita su AoC actualizado'
                mimeMessage = MIMEMultipart()
                mimeMessage['to'] = mailcontacto
                mimeMessage['subject'] = 'AoC Proximo a vencer '
                mimeMessage.attach(MIMEText(emailMsg, 'plain'))
                raw_string = base64.urlsafe_b64encode(mimeMessage.as_bytes()).decode()
                message = service.users().messages().send(userId='me', body={'raw': raw_string}).execute()
            #inserta los valores asociados a los correos que ha notificado que se va a vencer su AoC en la base de datos mysql en tabla proveedores_notificados
                cursorr = conexion.cursor()
                cursorr.execute("INSERT INTO proveedores_notificados (identificador, nombre, servicio, fecha_vencimiento_aoc, correo_contacto, fecha_notificacion) VALUES ('{}','{}','{}','{}','{}','{}')".format(identidadter,terceropro,servicioter,datetocompare,mailcontacto,datesendmail))
                conexion.commit()
            #Notifica cuando un correo no se encuentra el archivo sheet donde esta la información de los terceros
            else:
                emailMsg = 'No existe correo para contactar al proveedor' + terceropro
                mimeMessage = MIMEMultipart()
                mimeMessage['to'] = 'retomeli2022@gmail.com'
                mimeMessage['subject'] = 'No se logro notificar el vencimiento de AoC'
                mimeMessage.attach(MIMEText(emailMsg, 'plain'))
                raw_string = base64.urlsafe_b64encode(mimeMessage.as_bytes()).decode()
                message = service.users().messages().send(userId='me', body={'raw': raw_string}).execute()
                
    else:
        
        print('ya fue notificado el proveedor')    
        
#hace la conexión a la BD para obtener los proveedores de los cuales no se ha recibido correo
conexion = mysql.connector.connect(host = 'sql10.freesqldatabase.com',port = 3306, user = 'sql10473104', password= 'WXqzNP2trR', database = 'sql10473104')
cursorr = conexion.cursor()
cursorr.execute("SELECT identificador,nombre FROM proveedores_notificados WHERE Estado=0")
proveedores_notificados=cursorr.fetchall()

#recorre los id de los proveedores a los cuales se les notifico que su AoC esta pròximo a vencer
for proveedor in proveedores_notificados:
    idproveedor = proveedor[0]
    nombreproveedor = proveedor[1]
    
    #Busca los correos que se reciben con el asunto nombreproveedor+renovacion de AoC+idproveedor
    search_ids = service.users().messages().list(userId='me', q=(nombreproveedor+'-renovacion de AoC-'+str(idproveedor))).execute()
    number_results = search_ids['resultSizeEstimate']

    #Valida si encontro correos con el asunto 
    if number_results == 1:
        idsmail= search_ids['messages']
        
        if len(idsmail)==1:
            for msg_id in idsmail:
                print (msg_id['id']) #obtiene el ID del mensaje que encontro

            msg = service.users().messages().get(userId='me', id=msg_id['id'], format='full').execute()
            # divide el mensaje que encontro
            payload = msg['payload']
            headers = payload.get("headers")
            
            parts = payload.get("parts")[0]
            data = parts['body']['data']
            
           
            if headers:
            # obtiene información del correo electronico encontrado
                for header in headers:
                    name = header.get("name")
                    value = header.get("value")
                    if name.lower() == 'from':
                      
                        frommail=value
                        print(frommail)
                    if name.lower() == "to":
                       
                        tomail=value
                        print(tomail)
                    if name.lower() == "subject":
                       
                        subjectmail=value
                        print(subjectmail)
                    if name.lower() == "date":
                       
                        datemail=value
                        print(datemail)

                
                data = data.replace("-","+").replace("_","/")
                decoded_data = base64.b64decode(data)

                soup = BeautifulSoup(decoded_data , "lxml")
                body = soup.body()   
                print(str(body))    
                                  
            #agrega la información que se obtiene del correo a la BD y coloca en 1 el Estado de cada proveedor en la BD para identificar cuales ya dieron respuesta        
            cursorr.execute("UPDATE proveedores_notificados SET Estado='{}',Remitente='{}',Fecha_Respuesta_Proveedor='{}',Fecha_Actualizada_AOC='{}' WHERE identificador={}".format(1,frommail,datemail,str(body),idproveedor)) 
            conexion.commit()   
                  
    else: 
        
        print ('No hay mensajes recibidos por parte de los proveedores a los cuales se notifico que se va a vencer su AoC')
        

    
        


             
















                



        
      
        
      
        













