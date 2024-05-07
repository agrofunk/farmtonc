'''
    Get Farm CAR layers in a GPKG file from a longitude latitude pair 
        right on command line, like:
        
    python3 02_get_farm_car.py -47.84671 -21.96459
'''

# CARs source folder
f_car = '/home/geodata/Vetorial/fundiario/CAR/'

# municipalities
municipios = '/home/geodata/Vetorial/municipios.parquet'

# Farms save folder
f_farms = '/home/geodata/Clientes/0FARMS/'


import argparse
parser = argparse.ArgumentParser(description='Enter X (longitude), Y (latitude)')
# Required positional argument
parser.add_argument('longitude', type=float,
                    help='A required decimal longitude')
parser.add_argument('latitude', type=float,
                    help='A required decimal latitude')
args = parser.parse_args()

coords = (args.longitude, args.latitude)
print(f'Retrieving farm CAR for {coords}')

import geopandas as gpd
from shapely import Point
from os import walk
from pathlib import Path

s_ = gpd.GeoSeries([Point(coords)])
s_ = s_.set_crs(4326)
s = gpd.GeoDataFrame(['ponto'],geometry = s_)
print(s)


muni = gpd.read_parquet(municipios)
subset = gpd.sjoin(muni, s, how='inner', predicate='contains')
print(f'{subset["NM_MUN"].iloc[0]} - {subset["SIGLA_UF"].iloc[0]}, área = {100 * subset["AREA_KM2"].iloc[0]} ha')

area_imovel_1 = gpd.read_file(f'{f_car}/{subset["SIGLA_UF"].iloc[0]}/0muni/AREA_IMOVEL_1/{subset["CD_MUN"].iloc[0]}.gpkg').to_crs(4326)

# the farm
farm = gpd.sjoin(area_imovel_1, s, how='inner', predicate='contains')
try:
    print({farm['cod_imovel'].iloc[0]})#,  modulos fiscais: {farm.iloc[0]['mod_fiscal']}, area (ha): {farm.iloc[0]['num_area']}')
    try:
        del farm[0]
    except:
        print('.')
    try:    
        del farm['index_right']
    except:
        print('.')

    cod_imovel = farm['cod_imovel'].iloc[0]
    farm_file = f'{f_farms}/{cod_imovel}/CAR.gpkg'
    Path(f'{f_farms}/{cod_imovel}').mkdir( parents = True, exist_ok = True)
    farm.to_file(farm_file, layer='AREA_IMOVEL_1', driver='GPKG', mode = 'w')

    # get layers
    layers = [x[0].split('/')[-1] for x in sorted(walk(f'{f_car}/{subset["SIGLA_UF"].iloc[0]}', topdown=False))][2:]
    layers.remove('AREA_IMOVEL_1')

    # write layers to farm
    for layer in layers:
        try:
            file_ = gpd.read_file(f'{f_car}/{subset["SIGLA_UF"].iloc[0]}/0muni/{layer}/{subset["CD_MUN"].iloc[0]}.gpkg').to_crs(4326)
            file = file_[file_['cod_imovel'] == cod_imovel]
            if len(file) > 0:
                file.to_file(farm_file, layer = layer,  driver = 'GPKG')
                print(f'>> {layer} written to {cod_imovel}.gpkg')
        except:
            print(f'-- no {layer} for {subset["CD_MUN"].iloc[0]}')

    del layers, farm, cod_imovel, subset 
except:
    print(f'-- no CAR for {coords}')