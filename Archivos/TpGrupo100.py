# -*- coding: utf-8 -*-
"""
Trabajo Práctico 1

Materia : Laboratorio de datos - FCEyN - UBA
Autores  : Martinelli Lorenzo, Padilla Ramiro, Chapana Joselin

"""

import pandas as pd
from inline_sql import sql, sql_val

carpeta = "~/Dropbox/UBA/2024/LaboDeDatos/TP1/Archivos/TablasOriginales/" 

# Creamos los Dataframes vacios.
pais = pd.DataFrame(columns = ['idPais', 'Nombre', 'Pbi'])
sede = pd.DataFrame(columns=['idSede','idPais','Descripcion','estado'])
redes = pd.DataFrame(columns = ["idSede","URL"])
regiones = pd.DataFrame(columns = ['idPais', 'Region'])
secciones = pd.DataFrame(columns=['idSede','SeccionDescripcion'])

# Importamos los datos originales.
seccionesOriginal = pd.read_csv(carpeta+"lista-secciones.csv")

# Nos saltamos las tres primeras filas puesto que no aoortan datos relevantes.
pbisOriginal = pd.read_csv(carpeta+"API_NY.GDP.PCAP.CD_DS2_en_csv_v2_73.csv", skiprows=[0,2])

# Existe un atributo fuera de rango, lo solucionamos obviandolo.
atributos = [*pd.read_csv(carpeta+"lista-sedes-datos.csv", nrows=1)]
sedeDatosOriginal = pd.read_csv(carpeta+"lista-sedes-datos.csv", usecols= [i for i in range(len(atributos))])


# Limpieza de Datos 

# Pasamos a 1FN la siguiente tabla, puesto que en la columna redes_sociales posee valores que no son atómicos.
sedeDatosOriginal['redes_sociales'] = sedeDatosOriginal['redes_sociales'].str.split('  //  ')
sedeDatosOriginal = sedeDatosOriginal.explode('redes_sociales')

# Eliminamos las filas con red_social = ' ' creada sin intencion en el paso anterior
sedeDatosOriginal = sedeDatosOriginal[sedeDatosOriginal['redes_sociales']!='']

# Como el metodo explode() repite index para una misma fila, procedemos a reiniciar los mismos
sedeDatosOriginal = sedeDatosOriginal.reset_index(drop=True)

# Renombramos las columnas que nos interesan.
sedeDatosOriginal.rename(columns = {'sede_id': 'idSede',
                                    'pais_iso_3': 'idPais',
                                    'sede_desc_castellano': 'Descripcion', 
                                    'region_geografica': 'Region',
                                    'redes_sociales': 'URL'}, inplace = True)

pbisOriginal.rename(columns = {'Country Name': 'Nombre',
                               'Country Code': 'idPais',
                               '2022': 'Pbi'}, inplace = True)

seccionesOriginal.rename(columns = {'sede_id': 'idSede',
                                    'sede_desc_castellano': 'SeccionDescripcion'}, inplace = True)



# Seleccionamos las columnas pertinentes a nuestro DER.
sedeLimpia = sedeDatosOriginal[['idSede', 'idPais', 'Descripcion', 'estado']]
paisLimpia = pbisOriginal[['idPais', 'Nombre', 'Pbi']] 
redesLimpia = sedeDatosOriginal[['idSede', 'URL']]
regionesLimpia = sedeDatosOriginal[['idPais', 'Region']]
seccionesLimpia = seccionesOriginal[['idSede', 'SeccionDescripcion']]

# Aca deberiamos limpiar los nulls, paises que no son paises, etc. Basicamente terminar de limpiar.

# Convertimos los datos limpios en archivos csv, dejo un ejemplo. 
sede.to_csv('~/Dropbox/UBA/2024/LaboDeDatos/TP1/Archivos/TablasLimpias/sedeLimpia.csv', index = False)

# Importamos los datos (ya limpios) 

# Teniendo los csv limpios podriamps importarlos de la carpeta, o sino, laburar directamente con los 
# creados unas lineas mas arriba.

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
# 1)

#obtenemos pbi con nombre de cada pais
consultaSQL = """
                SELECT DISTINCT  a.idPais, a.Nombre as Pais,p.PBI_2022
                from pbi as p, pais as a
                where p.idPais=a.idPais 
                ORDER BY a.idPais asc
               """
tabla1= sql^ consultaSQL               

#cantidad de sedes por pais
consultaSQL=""" 
                SELECT  idPais ,COUNT(*) as Sedes
                FROM sede
                GROUP BY idPais
                ORDER BY Sedes DESC
                
            """

tabla2= sql^ consultaSQL


#unimos pais,sedes y pbi
consultaSQL="""
                SELECT DISTINCT Pais,Sedes,PBI_2022
                FROM tabla1 as t1
                INNER JOIN tabla2 as t2
                ON t1.idPais=t2.idPais
                ORDER BY Sedes desc

            """
tabla3= sql^ consultaSQL                

#Promedio secciones.

#Cantidad de secciones por sede
consultaSQL= """
                SELECT idSede,COUNT(*) as cantidadDescripcion
                FROM secciones as sc
                GROUP BY idSede
                
             """
cantSec=sql^ consultaSQL

#juntamos cantSet y sede

consultaSQL=""" 
               SELECT DISTINCT *
               FROM sede as s
               INNER JOIN cantSec as c
               ON s.idSede=c.idSede
           """
unionTablas=sql^ consultaSQL

#obtenemos el promedio de secciones por pais
consultaSQL="""
                SELECT  idPais ,AVG(cantidadDescripcion) as promedio
                FROM unionTablas
                GROUP BY idPais 
                ORDER BY idPais asc
               
            """
promedioSec=sql^ consultaSQL

#obtenemos nombre de paises y el promedio correspondiente

consultaSQL="""
               SELECT Nombre,Promedio
               FROM promedioSec as p
               INNER JOIN pais as pa
               ON p.idPais=pa.idPais

            """
tablaPromPais=sql^ consultaSQL

#Finalmente unimos los datos de la tabla 3 con la de promedioSec
consultaSQL=""" 
                SELECT Pais,Sedes,Promedio,PBI_2022
                FROM tablaPromPais as t
                INNER JOIN tabla3 as t3
                ON t3.Pais=t.Nombre
  
            """
            
respuesta1=sql^ consultaSQL






