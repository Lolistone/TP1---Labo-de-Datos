# -*- coding: utf-8 -*-
"""
Trabajo Práctico 1

Materia : Laboratorio de datos - FCEyN - UBA
Autores  : Martinelli Lorenzo, Padilla Ramiro, Chapana Joselin

"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from   matplotlib import ticker   # Para agregar separador de miles 
import six
from inline_sql import sql, sql_val
import seaborn as sns


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

# Eliminamos duplicados de región.
regionesLimpia = regionesLimpia.drop_duplicates()

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


# Unimos pais, sedes y pbi. Utilizo un Outer Join para recuperar aquellos paises sin sedes.
consultaSQL = """
                SELECT DISTINCT p.idPais, 
                                  Nombre, 
                                  CASE WHEN Sedes IS NULL THEN 0 ELSE Sedes END AS Sedes, 
                                  Pbi
                FROM pais AS p
                LEFT OUTER JOIN sedes_por_pais AS sp
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

# Juntamos los datos, utilice round para mas porlijidad al mostrar las tablas en el informe.
consultaSQL = """ 
                SELECT Nombre, 
                        Sedes,
                        CASE WHEN p.Promedio is NULL THEN 0 ELSE ROUND(p.Promedio, 2) END AS Promedio, 
                        ROUND(Pbi) AS Pbi
                FROM promedio_secciones AS p
                RIGHT OUTER JOIN sedes_pbi AS s
                ON s.idPais = p.idPais
                ORDER BY Sedes DESC, Nombre ASC
            """
            
result = sql^ consultaSQL

# ii)

# Descarto paises sin sedes
consultaSQL = """
                SELECT DISTINCT *
                FROM sedes_pbi 
                WHERE Sedes > 0
              """

paises_con_sede = sql^ consultaSQL


# Uno las regiones a la tabla anterio y calculo el promedio.
consultaSQL = """
                SELECT DISTINCT r.region, COUNT(*) AS Sedes, AVG(p.Pbi) Pbi_Promedio
                FROM paises_con_sede AS p
                INNER JOIN regiones AS r
                ON r.idPais = p.idPais
                GROUP BY region
                ORDER BY Pbi_Promedio DESC
              """

region_pais_pbi = sql^ consultaSQL

# iii)

# Cómo pide las vias de comunicacion de las sedes de cada pais, nos quedamos solo con aquellos paises que
# efectivamente tienen sedes.

# Recuperamos paises sin redes sociales, y de paso pegamos la URL al nombre del Pais
consultaSQL = """
                SELECT DISTINCT s.idSede, s.idPais, r.URL
                       FROM redes AS r 
                       RIGHT OUTER JOIN sede AS s
                       ON r.idSede = s.idSede
              """

pais_redes = sql^ consultaSQL


# Clasificamos por redes
consultaSQL = """
                SELECT DISTINCT r.idPais,
                                r.idSede,
                                CASE 
                                    WHEN LOWER(r.URL) LIKE '%facebook%' THEN 'Facebook'
                                    WHEN LOWER(r.URL) LIKE '%twitter%' THEN 'Twitter'
                                    WHEN LOWER(r.URL) LIKE '%linkedin%' THEN 'LinkedIn'
                                    WHEN LOWER(r.URL) LIKE '%instagram%' OR LOWER(r.URL) LIKE '@_%' THEN 'Instagram'
                                    WHEN LOWER(r.URL) LIKE '%youtube%' THEN 'Youtube'
                                    WHEN LOWER(r.URL) LIKE '%flickr%' THEN 'Flickr'
                                    ELSE 'SinRedSocial'
                               END AS tipo_red,
                               r.URL
                FROM pais_redes AS r
              """
              
redes_tipos = sql^ consultaSQL

# Solo resta contar, decidimos que los paies categorizados 'SinRedSocial' tengan 0 tipos redes.
consultaSQL = """
                SELECT idPais,
                       CASE WHEN (tipo_red != 'SinRedSocial') THEN 1 ELSE 0 END AS cantidad
                FROM redes_tipos
                GROUP BY idPais, tipo_red
              """

cantidad_redes = sql^ consultaSQL

# Antes agrupe pais - tipo de red, y si esta era válida agregaba un 1, sino un 0. Resta entonces sumar.

# Aclaración: Faltan 5 paises puesto que al tomar la decisión de descartar aquellos paises sin PBI, 
# también desapareción su nombre. Una posible solución es no realizar este Join y manejarnos con el Id.

consultaSQL = """
                SELECT p.Nombre,
                       SUM(r.cantidad) AS "Cantidad de tipos de red social"
                FROM cantidad_redes AS r
                INNER JOIN pais AS p
                ON p.idPais = r.idPais
                GROUP BY p.Nombre
              """

redes_por_pais = sql^ consultaSQL


# iv)

# Uno sede al df redes_tipos que ya tenia antes con el nombre del pais. No considero a aquellos paises sin redes.
consultaSQL = """
                SELECT DISTINCT p.Nombre AS Pais,
                                r.idSede AS Sede,
                                r.tipo_red AS "Red Social",
                                r.URL
                FROM redes_tipos AS r
                INNER JOIN pais AS p
                ON p.idPais = r.idPais
                WHERE r.tipo_red != 'SinRedSocial'
                ORDER BY Pais, Sede, "Red Social", URL
              """

pais_sede_red = sql^ consultaSQL


#%% Gráficos

# i - Cantidad de Sedes por Region

consultaSQL=""" 
                SELECT DISTINCT r.Region, COUNT(*) AS Sedes
                FROM regiones as r
                INNER JOIN sede as s
                ON r.idPais=s.idPais
                GROUP BY REGION
                ORDER BY Sedes ASC
            
            """
cantidad_sedes_region= sql^ consultaSQL

fig, ax = plt.subplots()

plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['axes.spines.right']  = False           
plt.rcParams['axes.spines.left']   = True            
plt.rcParams['axes.spines.top']    = False
plt.rcParams['axes.spines.bottom'] = True  

    
ax.bar(data = cantidad_sedes_region, 
       x = 'Region', 
       height='Sedes',
       width = 0.9,
       color = 'slateblue')

ax.set_title('Cantidad de Sedes por Región',fontsize=18,fontweight='bold',pad=50)
ax.set_xlabel('Región', fontsize = 12)                       
ax.set_ylabel('Sedes', fontsize= 12)
plt.xticks(rotation=90)
ax.bar_label(ax.containers[0], fontsize=8)
ax.set_ylim(0, 50)                         

ax.yaxis.set_major_formatter(ticker.StrMethodFormatter("{x:,.0f}"));


# ii - Para cada pais donde existe al menos una sede agregamos la informacion sobre su region.
# Descartamos los paises que su PBI es Null

consultaSQL = """
                SELECT r.*, p.Pbi
                FROM regiones AS r, pais AS p
                WHERE r.idPais = p.idPais
              """
paises_regiones_pbi = sql^consultaSQL

# Calculo mediana de PBIs para cada region, y ordenamos
consultaSQL = """
                SELECT pr.Region, MEDIAN(Pbi) AS Mediana
                FROM paises_regiones_pbi AS pr
                GROUP BY pr.Region
                ORDER BY Mediana DESC
              """  
medianas_regiones = sql^consultaSQL

ordenCorrecto = list(medianas_regiones['Region'])


ax = sns.boxplot( y = "Region", 
                  x ="Pbi", 
                  data = paises_regiones_pbi, 
                  palette = 'Set2',
                  legend = True,
                  width = 0.6,                 
                  order=ordenCorrecto)

ax.set_title('PBI per cápita 2022 por Region \n En paises con alguna Sede', fontsize=12, fontweight='bold')
ax.set_xlabel('PBI per cápita 2022 (USD)', fontsize=10)
ax.set_ylabel('Region',fontsize=12)
ax.set_xlim(-1 ,paises_regiones_pbi["Pbi"].max() + 2000)

#grafico de las ultimas dos regiones 

ultimosDos= medianas_regiones.tail(2)

consultaSQL=""" 
               SELECT *
               FROM ultimosDos as u, paises_regiones_pbi as p
               WHERE u.Region=p.Region  
            """
soloDos=sql^ consultaSQL

ax = sns.boxplot( y = "Region", 
                  x ="Pbi", 
                  data = soloDos, 
                  
                  width = 0.6,  
                  )

ax.set_title('PBI per cápita 2022 por Region donde Argentina tiene sede',fontsize=18,fontweight='bold',pad=50)
ax.set_xlabel('PBI per cápita 2022 (USD)',fontsize=12)
ax.set_ylabel('Region',fontsize=12)

ax.set.xlim(0,7000)


# iii - Comparación entre Cantidad de sedes y Pbi.
fig, ax = plt.subplots()

plt.rcParams['font.family'] = 'sans-serif'           

ax.plot('Sedes', 'Pbi', data = sedes_pbi, marker='.', markersize = 4, linewidth = 0, color = 'crimson')

ax.set_title('Cantidad de Sedes vs PBI per capita')
ax.set_xlabel('Sedes')
ax.set_ylabel('PBI (millones de USD)')
ax.set_xlim(-1,12)
ax.set_ylim(0,120000)


#%% Funcion Auxiliar para graficar las tablas

def render_mpl_table(data, col_width=3.0, row_height=0.625, font_size=14,
                     header_color='#40466e', row_colors=['#f1f1f2', 'w'], edge_color='w',
                     bbox=[0, 0, 1, 1], header_columns=0,
                     ax=None, **kwargs):
    if ax is None:
        size = (np.array(data.shape[::-1]) + np.array([0, 1])) * np.array([col_width, row_height])
        fig, ax = plt.subplots(figsize=size)
        ax.axis('off')

    mpl_table = ax.table(cellText=data.values, bbox=bbox, colLabels=data.columns, **kwargs)

    mpl_table.auto_set_font_size(False)
    mpl_table.set_fontsize(font_size)

    for k, cell in six.iteritems(mpl_table._cells):
        cell.set_edgecolor(edge_color)
        if k[0] == 0 or k[1] < header_columns:
            cell.set_text_props(weight='bold', color='w')
            cell.set_facecolor(header_color)
        else:
            cell.set_facecolor(row_colors[k[0]%len(row_colors) ])
    return ax

render_mpl_table(result, header_columns=0, col_width=4.0)
