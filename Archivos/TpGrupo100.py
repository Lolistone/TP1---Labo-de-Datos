# -*- coding: utf-8 -*-
"""
Trabajo Práctico 1

Materia: Laboratorio de datos - FCEyN - UBA
Autores  : Martinelli Lorenzo, Padilla Ramiro, Chapana Joselin

"""

import pandas as pd
from inline_sql import sql, sql_val

carpeta = "~/Dropbox/UBA/2024/LaboDeDatos/TP1/Tabla_Limp/TablasOriginales/" 

# Creamos los Dataframes vacios.
pais = pd.DataFrame(columns = ["idPais", "Nombre"])
sede = pd.DataFrame(columns=["idSede","idPais","Descripcion","Estado"])
redes = pd.DataFrame(columns = ["idSede","URL"])
regiones = pd.DataFrame(columns = ['idPais', 'Region'])
secciones = pd.DataFrame(columns=["idSede","SeccionDescripcion"])
pbi = pd.DataFrame(columns = ["idPais", "PBI_2022"])

# Importamos los datos originales.
seccionesOriginal = pd.read_csv(carpeta+"lista-secciones.csv")

# Nos saltamos las tres primeras filas puesto que no aoortan datos relevantes.
pbisOriginal = pd.read_csv(carpeta+"API_NY.GDP.PCAP.CD_DS2_en_csv_v2_73.csv", skiprows=[0,2])

# Existe un atributo fuera de rango, lo solucionamos obviandolo.
atributos = [*pd.read_csv(carpeta+"lista-sedes-datos.csv", nrows=1)]
sedeDatosOriginal = pd.read_csv(carpeta+"lista-sedes-datos.csv", usecols= [i for i in range(len(atributos))])


#%% Limpieza de Datos

# Normalizamos la siguiente tabla, la cual corresponde a 'lista-sedes-datos.csv', puesto que en la columna redes_sociales
# posee valores que no son atómicos.
sedeDatosOriginal['redes_sociales'] = sedeDatosOriginal['redes_sociales'].str.split('  //  ')
sedeDatosOriginal = sedeDatosOriginal.explode('redes_sociales')

# sedeLimpio = sedeLimpio.drop_duplicates()  ACA BORRA UNA ENTRADA (embajada de qatar)

# Eliminamos las filas con red_social = ' ' creada sin intencion en el paso anterior
sedeDatosOriginal = sedeDatosOriginal[sedeDatosOriginal['redes_sociales']!='']

# Esto por si filtramos las redes que solo son URLS, (eliminamos los @ y cualquier otro texto) 
# CONSERVAMOS los nulls?  si no, con na=False se eliminan tmbn, tambien podemos pasar de 'nan' a 'sinredsocial'
# sedeDatosOriginal = sedeDatosOriginal[sedeDatosOriginal['redes_sociales'].str.contains('.com', case=False, na=True)]
# AUNQUE LUEGO LO HICE EN LA CONSULTA SQL

# Como el metodo explode() repite index para una misma fila, procedemos a reiniciar los mismos
sedeDatosOriginal = sedeDatosOriginal.reset_index(drop=True)

### aca nombramos el problema de calidad que encontramos, no se cual es bien
### puede ser que hay muchas redes en una sola fila, lo q trae problemas de instancia?
### Y no se respeta el atributo de calidad: Constancia.


### de PBIORIGINAL podemos decir q no se cumple el atributo completitud, faltan datos en 2022
### posible arreglo? eliminarlos 
### Otro posible problema, es de relatividad, hay muchos "paises" que son regiones lo cual no es de interes


# Importamos los datos (ya limpios) 

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

# Ya borramos duplicados antes
redes = sql^"""
            SELECT *
            FROM redes
            UNION
            SELECT sdo.sede_id,
                    sdo.redes_sociales
            FROM sedeDatosOriginal AS sdo
            """

# Aplicamos UNION (sin ALL) para eliminar tuplas duplicadas ya que existen varias sedes distintas en un pais.
regiones = sql^"""
                SELECT *
                FROM regiones,
                UNION
                SELECT sdo.pais_iso_3,
                        sdo.region_geografica
                FROM sedeDatosOriginal AS sdo
                """

### en seccionesOriginal hay 3 duplicados, por eso mismo UNION (SIN ALL)
secciones = sql^"""
                SELECT *
                FROM secciones
                UNION
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
 
#%% # # # # # # # # # # 
# # # Ejercicio H # # # 
# # # # # # # # # # # #

# i)

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

# ii)

# El promedio no se ve afectado por valors NULL de PBI, creo por AVG() lo salva
# Se puede agregar con WHERE p.PBI_2022 IS NOT NULL, pero es lo mismo
paises_conAlmenos_1SedeyPBI = sql^ """
                                SELECT DISTINCT s.idPais,
                                                r.region,     
                                                p.PBI_2022
                                FROM sede AS s
                                LEFT OUTER JOIN regiones AS r
                                ON s.idPais = r.idPais
                                LEFT OUTER JOIN pbi AS p    
                                ON s.idPais = p.idPais                                                            
                            """

resultadoEJii = sql^"""
                        SELECT Region AS "Región geográfica",
                                COUNT(*) AS "Paises Con Sedes Argentinas",
                                AVG(PBI_2022) AS "Promedio PBI per Cápita 2022 (U$S)" 
                        FROM paises_conAlmenos_1Sede_ConPBI
                        GROUP BY Region
                        ORDER BY "Promedio PBI per Cápita 2022 (U$S)" DESC;
                    """

# iii)

redes_segunPais = sql^"""
                        SELECT r.*,
                                s.idPais
                        FROM redes AS r, sede AS s
                        WHERE r.idSede = s.idSede
                    """

# No encontre otra manera que hacerlo MANUAL
redes_Tipos = sql^"""
                        SELECT DISTINCT r.idPais,
                                CASE 
                                   WHEN LOWER(r.URL) LIKE '%facebook%' THEN 'Facebook'
                                   WHEN LOWER(r.URL) LIKE '%twitter%' THEN 'Twitter'
                                   WHEN LOWER(r.URL) LIKE '%linkedin%' THEN 'LinkedIn'
                                   WHEN LOWER(r.URL) LIKE '%instagram%' THEN 'Instagram'
                                   WHEN LOWER(r.URL) LIKE '%youtube%' THEN 'Youtube'
                                   WHEN LOWER(r.URL) LIKE '%flickr%' THEN 'Flickr'
                                   ELSE 'SinRedSocial'
                               END AS tipo_red
                        FROM redes_segunPais AS r
                    """

# Solo resta contar, decidimos que los paies categorizados 'SinRedSocial' tengan 0 tipos redes
resultadoEJiii = sql^"""
                            SELECT rt.idPais,
                                    COUNT(CASE 
                                              WHEN (rt.tipo_red !='SinRedSocial') THEN 1 
                                              ELSE NULL 
                                          END
                                         ) AS "Cantidad de tipos de red social"
                            FROM redes_Tipos AS rt
                            GROUP BY rt.idPais
                        """

