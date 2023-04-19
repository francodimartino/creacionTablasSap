import psycopg2
import pandas as pd

def conectar_db(host, port, database, user, password):
  print("Creando conexión con base de datos")
  connection = psycopg2.connect(user=user,
                                password=password,
                                host=host,
                                port=port,
                                database=database)

  print("Conexion con base de datos creada")
  return connection

  #crear tabla en la base de datos, segun un csv, previo armado de un df

def limpiar_nombre_columna(nombre_columna):
    simbolos_a_evitar=["(",")","'","-",".","/", " "]
    for simbolo in simbolos_a_evitar:
        nombre_columna=nombre_columna.replace(simbolo,"")
    return nombre_columna

def crear_o_truncar_tabla(csv, connection, nombre_tabla, truncar=False):
    print(f"Creando tabla {nombre_tabla} en la base de datos")
    cur = connection.cursor()
    df = pd.read_csv(csv, sep='|', encoding='latin-1', low_memory=False)

    #chequear que no exista en la base una tabla con el mismo nombre
    cur.execute(f"SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '{nombre_tabla}');")
    existe_tabla = cur.fetchone()[0]
    
    if existe_tabla:
        print(f"La tabla {nombre_tabla} ya existe en la base de datos")
        
        if truncar:
            cur.execute(f"TRUNCATE TABLE {nombre_tabla};")
            print(f"La tabla {nombre_tabla} fue truncada")
    else:
        print(f"La tabla {nombre_tabla} no existe en la base de datos, la creo")


    # Crear la consulta SQL para crear la tabla
        inicio_consulta = f"CREATE TABLE {nombre_tabla} ("
        consulta=""

        for columna in df.columns:
            
            tipo_dato = df[columna].dtype
            
            
            if tipo_dato == 'int64':
                consulta += f'"{columna}" INTEGER, '
            elif tipo_dato == 'float64':
                consulta += f'"{columna}" FLOAT, '
            elif tipo_dato == 'datetime64[ns]':
                consulta += f'"{columna}" TIMESTAMP, '
            else:
                consulta += f'"{columna}" VARCHAR, '

        # Eliminar la última coma de la consulta SQL y cerrar la sentencia
        

        consulta = inicio_consulta+consulta[:-2] + ");"
        
        # Ejecutar la consulta SQL
        cur.execute(consulta)
    

    # Cerrar la conexión a la base de datos
    
    



def cargar_carpeta_en_tabla(conexion, carpeta, tabla, extension=".txt"):
    print(carpeta)
    import os
    tabla_diferentes=["ZF01_VIG.txt", "ZF01_ANT.txt"]
    for archivo in os.listdir(carpeta):
        if archivo.endswith(extension) and os.path.getsize(carpeta+archivo)>1000:
            print(f"iniciando Carga de {archivo}")
            df=pd.read_csv(carpeta+archivo, sep='|', encoding='latin-1', low_memory=False)
            if archivo in tabla_diferentes:
                #agregar la columna saldo_final, con ceros en la posicion 5 de columnas
                df.insert(19, "saldo_final", 0)
                #reemplazar NaN por ceros
                df=df.fillna(0)
            try:
                  
                cargar_df_en_tabla(conexion, df, tabla)
                conexion.commit()
            except Exception as e:
                #save in "archivos_con_error.txt" the name of the entire file and folder and the error
                with open("archivos_con_error.txt", "a") as f:
                    f.write(f"{carpeta+archivo}: {e}\n")
                print(f"Error al cargar el archivo {archivo}: {e}")
                conexion.rollback()
                continue
            print(f"Archivo {archivo} cargado correctamente")


def cargar_df_en_tabla(conexion, df, tabla):
    
    #crear un cursor para ejecutar la consulta
    cur = conexion.cursor()

    #crear la consulta para insertar los datos
    consulta = f"INSERT INTO {tabla} VALUES ("
    cant=0
    for columna in df.columns:
        consulta += "%s, "
        cant+=1

    consulta = consulta[:-2] + ");"
    
    valores=df.values.tolist()
    
    cur.executemany(consulta, valores)
    



#NO SE USAN EN ESTA VERSION
#crear funcion que lea un csv "archivos_cargados.csv" y devuelva una lista con los archivos cargados
def leer_archivos_cargados():
    archivos_cargados=[]
    with open("archivos_cargados.txt", "r") as archivo:
        for linea in archivo:
            archivos_cargados.append(linea[:-1])
    return archivos_cargados

#crear funcion que escriba en un csv "archivos_cargados.csv" los archivos cargados guardados en una lista pisando los anteriores
def escribir_archivos_cargados(archivos_cargados):
    with open("archivos_cargados.txt", "w") as archivo:
        for archivo_cargado in archivos_cargados:
            archivo.write(archivo_cargado+"\n")

    
    
