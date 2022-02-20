from mimetypes import MimeTypes
from pickle import TRUE
from re import T
from urllib import response
from Google import Create_Service
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import mysql.connector
import mysql
from bs4 import BeautifulSoup
from googleapiclient.http import MediaIoBaseUpload
import io
from googleapiclient.http import MediaIoBaseDownload
import sys
import os


CLIENT_SECRET_FILE = 'client_secret.json'
API_NAME = 'gmail'
API_VERSION = 'v1'
SCOPES = ['https://mail.google.com/']

service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

CLIENT_SECRET_FILE = 'client_secret.json'
API_NAME = 'drive'
API_VERSION = 'v2'
SCOPES = ['https://www.googleapis.com/auth/drive']
service_drive = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

def create_folder_in_drives(service,folder_name,parent_folder = []):
    file_metadata = {'name':folder_name, 'parents':parent_folder, 'mimeType':'application/vnd.google-apps.folder'}
    file = service.file().create(body=file_metadata,fields='id').execute()
    return file

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
        print (idproveedor)
        print ('No hay mensajes recibidos por parte de los proveedores a los cuales se notifico que se va a vencer su AoC')
        

    
        


             









