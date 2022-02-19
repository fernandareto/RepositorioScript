import mysql.connector
import mysql


conexion = mysql.connector.connect(host = 'sql10.freesqldatabase.com',port = 3306, user = 'sql10473104', password= 'WXqzNP2trR', database = 'sql10473104')

print(conexion)

cursorr = conexion.cursor()
cursorr.execute("SELECT identificador FROM proveedores_notificados")
identificadores=cursorr.fetchall()
print(identificadores)
            


listasincomas = str(identificadores)[1:-1]
print(listasincomas)

dato=1
if str(dato) not in listasincomas:
    print('no esta')
else:
    print('si esta')








        







                                
            