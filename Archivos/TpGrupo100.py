# -*- coding: utf-8 -*-
"""
Trabajo Práctico 1

Materia : Laboratorio de datos - FCEyN - UBA
Autores  : Martinelli Lorenzo, Padilla Ramiro, Chapana Joselin

"""

import pandas as pd
import numpy as np
from inline_sql import sql, sql_val


carpeta = "~/Dropbox/UBA/2024/LaboDeDatos/TP1/Archivos/TablasOriginales/" 

# Creamos los Dataframes vacios.
pais = pd.DataFrame(columns = ['idPais', 'Nombre', 'Pbi'])
sede = pd.DataFrame(columns=['idSede','idPais','Descripcion'])
redes = pd.DataFrame(columns = ["idSede","URL"])
regiones = pd.DataFrame(columns = ['idPais', 'Region'])
secciones = pd.DataFrame(columns=['idSede','SeccionDescripcion'])


#%% Limpieza de Datos 

# Importamos los datos originales.
seccionesOriginal = pd.read_csv(carpeta+"lista-secciones.csv")

# Nos saltamos las tres primeras filas puesto que no aoortan datos relevantes.
pbisOriginal = pd.read_csv(carpeta+"API_NY.GDP.PCAP.CD_DS2_en_csv_v2_73.csv", skiprows=[0,2])

# Existe un atributo fuera de rango, lo solucionamos obviandolo.
atributos = [*pd.read_csv(carpeta+"lista-sedes-datos.csv", nrows=1)]
sedeDatosOriginal = pd.read_csv(carpeta+"lista-sedes-datos.csv", usecols= [i for i in range(len(atributos))])

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
sedeLimpia = sedeDatosOriginal[['idSede', 'idPais', 'Descripcion']]
paisLimpia = pbisOriginal[['idPais', 'Nombre', 'Pbi']] 
redesLimpia = sedeDatosOriginal[['idSede', 'URL']]
regionesLimpia = sedeDatosOriginal[['idPais', 'Region']]
seccionesLimpia = seccionesOriginal[['idSede', 'SeccionDescripcion']]

# Pasamos a 1FN el dataframe Redes.
redesLimpia['URL'] = redesLimpia['URL'].str.split('  //  ')
redesLimpia = redesLimpia.explode('URL')

# Eliminamos las filas con red_social = ' ' creada sin intencion en el paso anterior
redesLimpia = redesLimpia[redesLimpia['URL']!='']

# Eliminamos inconsistencias de las Url, quedandonos con aquellos que poseian .com o arroba.
redesLimpia = redesLimpia[redesLimpia['URL'].str.contains('@|.com', case = False, na=False)]

# El metodo explode() repite index para una misma fila y por otro lado, contains borra los indices. Los reseteamos.
redesLimpia = redesLimpia.reset_index(drop=True)

# Eliminamos de paises todos aquellos que no lo son. La lista a continuacion fue escrita manualmente.
no_son_paises = ['AFE', 'AFW', 'ARB', 'CAF', 'CEB', 'EAR', 'EAS', 'TEA', 'EAP', 'EMU', 'ECS', 'TEC', 'ECA',
                 'EUU', 'FCS', 'HPC', 'HIC', 'IBD', 'IBT', 'IDB', 'IDX', 'IDA', 'LTE', 'LCN', 'LAC', 'TLA',
                 'LDC', 'LMY', 'LIC', 'LMC', 'MEA', 'TMN', 'MNA', 'MIC', 'NAC', 'INX', 'OED', 'OSS', 'PSS',
                 'PST', 'PRE', 'SST', 'SAS', 'TSA', 'SSF', 'TSS', 'SSA', 'UMC', 'WLD']

paisLimpia.drop(paisLimpia[paisLimpia['idPais'].apply(lambda x: x in no_son_paises)].index, inplace = True)


# Eliminamos de paises también aquellos paises sin Pbi, es decir, con valor Null.
paisLimpia.dropna(subset = ['Pbi'], inplace = True)

# Convertimos los datos limpios en archivos csv, dejo un ejemplo. Insertar la ruta donde desean tener las tablas limpias.
TablasLimpias = '~/Dropbox/UBA/2024/LaboDeDatos/TP1/Archivos/TablasLimpias/'

sedeLimpia.to_csv(TablasLimpias + 'sede.csv', index = False)
paisLimpia.to_csv(TablasLimpias + 'pais.csv', index = False)
redesLimpia.to_csv(TablasLimpias + 'redes.csv', index = False)
regionesLimpia.to_csv(TablasLimpias + 'regiones.csv', index = False)
seccionesLimpia.to_csv(TablasLimpias + 'secciones.csv', index = False)

# Importamos las carpetas limpias.

sede = pd.read_csv( TablasLimpias + 'sede.csv')
pais = pd.read_csv( TablasLimpias + 'pais.csv')
redes = pd.read_csv( TablasLimpias + 'redes.csv')
regiones = pd.read_csv( TablasLimpias + 'regiones.csv')
secciones = pd.read_csv( TablasLimpias + 'secciones.csv')


#%% Analisis de Datos - Ejercicio H

# i)            

# Cantidad de sedes por pais
consultaSQL = """ 
                SELECT  idPais , COUNT(*) as Sedes
                FROM sede
                GROUP BY idPais
                ORDER BY Sedes DESC            
              """

sedes_por_pais = sql^ consultaSQL


# Unimos pais, sedes y pbi. Estoy perdiendo ~5 paises, deduzco que el motivo es que
# al borrar los pbi que figuraban como Null estos desaparecieron.
consultaSQL = """
                SELECT DISTINCT p.idPais, Nombre, Sedes, Pbi
                FROM pais AS p
                INNER JOIN sedes_por_pais AS sp
                ON p.idPais = sp.idPais
                ORDER BY Sedes DESC
              """
              
sedes_pbi = sql^ consultaSQL                

#Cantidad de secciones por sede
consultaSQL = """
                SELECT idSede, COUNT(*) AS cantidad_secciones
                FROM secciones AS sc
                GROUP BY idSede
              """
              
cant_seccciones = sql^ consultaSQL

# Calculamos el promedio de secciones por país
consultaSQL=""" 
               SELECT DISTINCT idPais, AVG(cantidad_secciones) AS Promedio
               FROM cant_seccciones AS c
               INNER JOIN sede AS s 
               ON c.idSede = s.idSede
               GROUP BY idPais
               ORDER BY idPais ASC
           """
           
promedio_secciones =sql^ consultaSQL

# Juntamos los datos
consultaSQL = """ 
                SELECT Nombre, Sedes, p.Promedio, Pbi
                FROM promedio_secciones AS p
                INNER JOIN sedes_pbi AS s
                ON s.idPais = p.idPais
            """
            
result = sql^ consultaSQL

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

#GRAFICOS
#i)
#juntamos datos importantes de las tablas region y sede
consultaSQL=""" 
                SELECT r.Region
                FROM regiones as r
                INNER JOIN sede as s
                ON r.idPais=s.idPais
                ORDER BY Region ASC
               
            """
tRegiones= sql^ consultaSQL

#Genera el grafico de frecuencia

tRegiones["Region"].value_counts().plot.bar()


# Genera el grafico de frecuencias (mejorando la informacion mostrada)
fig, ax = plt.subplots()

rcParams['axes.spines.right'] = True       # Elimina linea derecha  del recuadro
rcParams['axes.spines.left']  = False       # Elimina linea derecha  del recuadro
rcParams['axes.spines.top']   = False        # Elimina linea superior del recuadro


ax = tRegiones["Region"].value_counts().plot.bar(color="purple")

# Agrega titulo, etiquetas a los ejes y limita el rango de valores de los ejes
ax.set_title("Cantidad de sedes por Region",fontsize=17)
ax.set_yticks([])                                  # Remueve los ticks del eje y
ax.bar_label(ax.containers[0], fontsize=8)         # Agrega la etiqueta a cada barra
#ax.tick_params(axis='x', labelrotation=0)         # Rota las etiquetas del eje x para que las muestre horizontales
plt.xlabel("Región")
plt.ylabel("Cantidad")
   
