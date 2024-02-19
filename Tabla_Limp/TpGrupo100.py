# -*- coding: utf-8 -*-
"""
Created on Mon Feb 19 11:51:55 2024

@author: JOSELIN
"""

import pandas as pd
from inline_sql import sql, sql_val

carpeta = "~/Desktop/TP_LABODATOS/" # cambien a su carpet 

#Dataframes vacios en 3FN

pais = pd.DataFrame(columns = ["idPais", "Nombre"])
sede = pd.DataFrame(columns=["idSede","idPais","Descripcion","Estado"])
redes = pd.DataFrame(columns = ["idSede","URL"])
regiones = pd.DataFrame(columns = ['idPais', 'Region'])
secciones = pd.DataFrame(columns=["idSede","SeccionDescripcion"])
pbi = pd.DataFrame(columns = ["idPais", "PBI_2022"])

# Levantamos datos de sus fuentes originales, de los cuales detectamos problemas 
# en la estructura de la fuente que ocasiona problemas en el proceso de carga de datos

seccionesOriginal = pd.read_csv(carpeta+"TP1-Labo-de-Datos/Tabla_Limp/TablasOriginales/lista-secciones.csv")

# En la fuente 'API_NY.GDP.PCAP.CD_DS2_en_csv_v2_73.csv' encontramos que al comienzo
# hay 3 filas las cuales modifican la estructura deseada de los datos
pbisOriginal = pd.read_csv(carpeta+"TP1-Labo-de-Datos/Tabla_Limp/TablasOriginales/API_NY.GDP.PCAP.CD_DS2_en_csv_v2_73.csv", skiprows=[0,2])

# En la fuente 'lista-sedes-datos.csv' encontramos un dato cargado en una columna fuera del rango deseado
# Procedimos de la siguiente manera para solucionar el problema, pero perdiendo este dato fuera de rango
atributos = [*pd.read_csv(carpeta+"TP1-Labo-de-Datos/Tabla_Limp/TablasOriginales/lista-sedes-datos.csv", nrows=1)]
sedeDatosOriginal = pd.read_csv(carpeta+"TP1-Labo-de-Datos/Tabla_Limp/TablasOriginales/lista-sedes-datos.csv", usecols= [i for i in range(len(atributos))])


# Arreglado el problema de estructura y lectura de las fuentes, procedemos a
# realizar la limpieza de datos tal como describimos en el informe


# En primer lugar, en la fuente 'lista-sedes-datos.csv' encontramos muchos datos
# faltantes, asi como tambien datos pertenecientes al atributo 'redes_sociales'
# los cuales no son atomicos y no permiten saber con exactitud cual es su red_social.
# Procedemos a separarlos sin perder ningun dato

sedeDatosOriginal['redes_sociales'] = sedeDatosOriginal['redes_sociales'].str.split('  //  ')

sedeDatosOriginal = sedeDatosOriginal.explode('redes_sociales')

# sedeLimpio = sedeLimpio.drop_duplicates()  ACA BORRA UNA ENTRADA (embajada de qatar)

# Eliminamos las filas con red_social = ' ' creada sin intencion en el paso anterior
sedeDatosOriginal = sedeDatosOriginal[sedeDatosOriginal['redes_sociales']!='']

# Como el metodo explode() repite index para una misma fila, procedemos a reiniciar los mismos
sedeDatosOriginal = sedeDatosOriginal.reset_index(drop=True) 
### aca nombramos el problema de calidad que encontramos, no se cual es bien
### puede ser que hay muchas redes en una sola fila, lo q trae problemas de instancia?
### 


### de PBIORIGINAL podemos decir q no se cumple el atributo completitud, faltan datos en 2022
### posible arreglo? eliminarlos 


# Importamos los datos (ya limpios) 

# no entiendo bien como importar, esta bien asi?

pais = sql^"""
            SELECT *
            FROM pais
            UNION
            SELECT pbi."Country Code",
                    pbi."Country Name"
            FROM pbisOriginal AS pbi
            """

# Aplicamos UNION (sin ALL) para eliminar tuplas duplicadas al haber separado las redes sociales
sede = sql^"""
            SELECT *
            FROM sede
            UNION
            SELECT sdo.sede_id,
                    sdo.pais_iso_3,
                    sdo.sede_desc_castellano,
                    sdo.estado
            FROM sedeDatosOriginal AS sdo
            """

# Aplicamos UNION (sin ALL) para eliminar tuplas duplicadas ###podemos eliminarla antes en la carga del .csv
redes = sql^"""
            SELECT *
            FROM redes
            UNION 
            SELECT sdo.sede_id,
                    sdo.redes_sociales
            FROM sedeDatosOriginal AS sdo
            """

# Aplicamos UNION (sin ALL) para eliminar tuplas duplicadas ya que existen varias sedes para un pais.
regiones = sql^"""
                SELECT *
                FROM regiones,
                UNION 
                SELECT sdo.pais_iso_3,
                        sdo.region_geografica
                FROM sedeDatosOriginal AS sdo
                """

### en seccionesOriginal hay 3 duplicados
secciones = sql^"""
                SELECT *
                FROM secciones
                UNION ALL
                SELECT sco.sede_id, 
                        sco.sede_desc_castellano
                FROM seccionesOriginal AS sco
                """

pbi = sql^"""
            SELECT *
            FROM pbi
            UNION ALL
            SELECT pbo."Country Code", 
                    pbo."2022" 
            FROM pbisOriginal AS pbo
            """





# Ejercicios

 consigna    = """pais y pbi
               """
               
 
 consultaSQL = """
                SELECT DISTINCT p.PBI_2022, a.idPais 
                from pbi as p, pais as a
                where p.idPais=a.idPais 
                Union
                
               """

 imprimirEjercicio(consigna, [pais,pbi], consultaSQL, sql^consultaSQL)


consigna=""" sedes por pais"""
consultaSQL=""" 
                SELECT idPais ,COUNT(*) as Sedes
                from sede
                GROUP BY idPais
                ORDER BY idPais ASC
                

            """

imprimirEjercicio(consigna, [sede], consultaSQL, sql^consultaSQL)

 
consigna=""" secciones promocion"""

consultaAQL=""" SELECT idSede, COUNT(*) as 
                from secciones
                



            """
imprimirEjercicio(consigna, [secciones], consultaSQL, sql^consultaSQL)            
            
carpeta=""







# =============================================================================
# DEFINICION DE FUNCIÓN DE IMPRESIÓN EN PANTALLA
# =============================================================================
# Imprime en pantalla en un formato ordenado:
    # 1. Consigna
    # 2. Cada dataframe de la lista de dataframes de entrada
    # 3. Query
    # 4. Dataframe de salida
def imprimirEjercicio(consigna, listaDeDataframesDeEntrada, consultaSQL, dataframeResultadoDeConsultaSQL):
    
    print("# -----------------------------------------------------------------------------")
    print("# Consigna: ", consigna)
    print("# -----------------------------------------------------------------------------")
    print()
    for i in range(len(listaDeDataframesDeEntrada)):
        print("# Entrada 0",i,sep='')
        print("# -----------")
        print(listaDeDataframesDeEntrada[i])
        print()
    print("# SQL:")
    print("# ----")
    print(consultaSQL)
    print()
    print("# Salida:")
    print("# -------")
    print(dataframeResultadoDeConsultaSQL)
    print()
    print("# -----------------------------------------------------------------------------")
    print("# -----------------------------------------------------------------------------")
    print()
    print()

