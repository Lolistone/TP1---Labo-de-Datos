# -*- coding: utf-8 -*-
"""
Created on Mon Feb 19 11:51:55 2024

@author: JOSELIN
"""

import pandas as pd
from inline_sql import sql, sql_val

#Ejercicios

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

#Dataframes vacios

pais = pd.DataFrame(columns = ["idPais", "Nombre"])
pbi = pd.DataFrame(columns = ["idPais", "PBI_2022"])
redes = pd.DataFrame(columns = ["idSede","URLS"])
regiones= pd.DataFrame(columns = ['idPais', 'Region'])
secciones=pd.DataFrame(columns=["idSede","Seccion"])
sede=pd.DataFrame(columns=["idSede","idPais","Descripcion","Estado"])

#importamos los datasets 

pais = pd.read_csv(carpeta+"Pais.csv")
pbi=pd.read_csv(carpeta+"PBI.csv")
redes=pd.read_csv(carpeta+"Regiones.csv")
regiones=pd.read_csv(carpeta+"Regiones.csv")
secciones=pd.read_csv(carpeta+"Secciones.csv")
sede=pd.read_csv(carpeta+"Sede.csv")

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

