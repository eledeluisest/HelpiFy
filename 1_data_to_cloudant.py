"""
25/06/2019
Automatic generation of json from .csv using pandas

names get from:
https://github.com/jvalhondo/spanish-names-surnames
"""

import pandas as pd
import cloudant
def csv2json(file, path = None,sep=';', header=0, path_to = None, obs=None):
    if path_to is None:
        path_to = path
    if path is not None:
        pd.read_csv(path+'/'+file+".csv", sep=sep, header=header, nrows=obs).to_json(path_to+'/'+file+".json")
    else:
        pd.read_csv( './data/' + file+".csv", sep=sep, header=header, nrows=obs).to_json('./' + file+".json")
csv2json("estado_catastrofe")
csv2json("estado_civil")
csv2json("estado_salud")
csv2json("idiomas")
csv2json("necesidades_ayuda")
csv2json("necesidades_medicas")
csv2json("profesiones")
csv2json("tipo_edificios")
csv2json("tipo_impacto")
csv2json("si_no")
csv2json("male_names",sep=',', obs=100)
csv2json("female_names",sep=',', obs=100)
csv2json("surnames_freq_ge_100",sep=',', obs=100)
csv2json("amenities",sep=',')

