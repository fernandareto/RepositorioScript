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
from Google import Create_Service
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import mysql.connector
import mysql
from mimetypes import MimeTypes
from pickle import TRUE
from re import T
from urllib import response
from Google import Create_Service
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from bs4 import BeautifulSoup
import io
import sys


####Codigo GOOGLE
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




##INICIO CODIGO FUNCIONAL

#autentica para enviar correos desde la cuenta gmail
CLIENT_SECRET_FILE = 'client_secret.json'
API_NAME = 'gmail'
API_VERSION = 'v1'
SCOPES = ['https://mail.google.com/']

service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)
print(service)

#autentica para leer el archivo sheet
gc = gspread.service_account(filename='proyecto-341313-f7fe5a0f69b4.json')
#lee el archivo datos que esta en drive
sh = gc.open("datos")

workshet = sh.get_worksheet(0)

#obtiene los valores del archivo en un dataframe
datos = workshet.get_all_values()

dataframe= pd.DataFrame(workshet.get_all_records())

#obtiene los datos necesarios del archivo sheet
tabla= dataframe.iloc[:,[0,1,2,3,6]]
print(tabla)


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


#hace la busqueda de los terceros que tienen su fecha de vencimiento de aoc en 10 días a partir del día que se corre el script
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
            # print (identidadter,terceropro, servicioter,datetocompare,mailcontacto, datesendmail)
        #envia el mensaje a los terceros que se encuentran dentro del plazo de 10 días para vencer su AOC        
                emailMsg = 'Su AoC Se encuentra proximo a vencer, por favor remita su AoC actualizado'
                mimeMessage = MIMEMultipart()
                mimeMessage['to'] = mailcontacto
                mimeMessage['subject'] = 'AoC Proximo a vencer - Mercado Libre'
                mimeMessage.attach(MIMEText(emailMsg, 'plain'))
                raw_string = base64.urlsafe_b64encode(mimeMessage.as_bytes()).decode()
                message = service.users().messages().send(userId='me', body={'raw': raw_string}).execute()
                print ('enviado')
            #inserta los valores asociados a los correos que ha notificado que se va a vencer su aoc en la base de datos mysql en tabla proveedores_notificados
                cursorr = conexion.cursor()
                cursorr.execute("INSERT INTO proveedores_notificados (identificador, nombre, servicio, fecha_vencimiento_aoc, correo_contacto, fecha_notificacion) VALUES ('{}','{}','{}','{}','{}','{}')".format(identidadter,terceropro,servicioter,datetocompare,mailcontacto,datesendmail))
                conexion.commit()
                print("registro insertado con exito")

            else:
                emailMsg = 'No existe correo para contactar al proveedor'+ terceropro
                mimeMessage = MIMEMultipart()
                mimeMessage['to'] = 'retomeli2022@gmail.com'
                mimeMessage['subject'] = 'No se logro notificar el vencimiento de AoC'
                mimeMessage.attach(MIMEText(emailMsg, 'plain'))
                raw_string = base64.urlsafe_b64encode(mimeMessage.as_bytes()).decode()
                message = service.users().messages().send(userId='me', body={'raw': raw_string}).execute()
                print ('nitificado')
    else:
        
        print('ya fueron notificado')    
        


#seacrh mail recibido

conexion = mysql.connector.connect(host = 'sql10.freesqldatabase.com',port = 3306, user = 'sql10473104', password= 'WXqzNP2trR', database = 'sql10473104')
print(conexion)

cursorr = conexion.cursor()
cursorr.execute("SELECT identificador,nombre FROM proveedores_notificados WHERE Estado=0")
proveedores_notificados=cursorr.fetchall()


for proveedor in proveedores_notificados:
    #print (proveedor[0])
    #print (proveedor[1])
    idproveedor = proveedor[0]
    nombreproveedor = proveedor[1]
    #renovacion = 'RenovacionAOC'
    
    #busco el id o cantidad de los correos que encuentra con ese asunto
    search_ids = service.users().messages().list(userId='me', q=(nombreproveedor+'-renovacion de AoC-'+str(idproveedor))).execute()
    #print (nombreproveedor)
    number_results = search_ids['resultSizeEstimate']
      
    #print (search_ids['resultSizeEstimate'])

#numberresult es la cantidad de mensajes que encontro por ese asunto q
    if number_results == 1:
        idsmail= search_ids['messages']
        ##si encuentra 1 mensaje con ese asunto 1 haga eso:
        if len(idsmail)==1:
            for msg_id in idsmail:
                print (msg_id['id']) #tomo el ID del mensaje que encontro con el asunto q

            msg = service.users().messages().get(userId='me', id=msg_id['id'], format='full').execute()
            # parts can be the message body, or attachments
            payload = msg['payload']
            headers = payload.get("headers")
            #parts = payload.get("parts")
            parts = payload.get("parts")[0]
            data = parts['body']['data']
            
            #has_subject = False
            if headers:
            # this section prints email basic info & creates a folder for the email
                for header in headers:
                    name = header.get("name")
                    value = header.get("value")
                    if name.lower() == 'from':
                        # we print the From address
                        #print("From:", value)
                        frommail=value
                        print(frommail)
                    if name.lower() == "to":
                        # we print the To address
                        #print("To:", value)
                        tomail=value
                        print(tomail)
                    if name.lower() == "subject":
                        # make our boolean True, the email has "subject"
                        #print("Subject:", value)
                        subjectmail=value
                        print(subjectmail)
                    if name.lower() == "date":
                        #print("Date:", value)
                        datemail=value
                        print(datemail)

                
                data = data.replace("-","+").replace("_","/")
                decoded_data = base64.b64decode(data)

                soup = BeautifulSoup(decoded_data , "lxml")
                body = soup.body()   
                print(str(body))    
                                  
                    
            cursorr.execute("UPDATE proveedores_notificados SET Estado='{}',Remitente='{}',Fecha_Respuesta_Proveedor='{}',Fecha_Actualizada_AOC='{}' WHERE identificador={}".format(1,frommail,datemail,str(body),idproveedor)) 
            conexion.commit()   
            print('cambioestado')        
    else: 
        
        print ('No hay mensajes recibidos por parte de los proveedores a los cuales se notifico que se va a vencer su AoC')
        

    
        


             
















                



        
      
        
      
        













