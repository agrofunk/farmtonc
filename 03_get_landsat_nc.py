#%%
farm_file = '/home/geodata/Clientes/0FARMS/MG-3102605-B4D344DBFD874F44906FCC0A5E0DCE36/CAR.gpkg'
folder_nc = farm_file.split('CAR.gpkg')[0]

import os
import boto3
import rasterio as rio
from pystac_client import Client
import xarray as xr
import numpy as np
from pathlib import Path
import geopandas as gpd
from datetime import date, datetime
from utils import *

#%% Set environment and create AWS Session
os.environ['CURL_CA_BUNDLE'] = '/etc/ssl/certs/ca-certificates.crt'
os.environ['AWS_REQUEST_PAYER'] = 'requester'

print("Creating AWS Session")
aws_session = rio.session.AWSSession(boto3.Session(), requester_pays=True)
print(aws_session)

# open Farm
farm = gpd.read_file(farm_file, layer='AREA_IMOVEL_1')
bbox = get_bbox(farm)

# %% Satellite imagery query params
today = date.today()
datetime_rangefull = str(f"2018-06-20/{str(today)}")
local_folder = '/home/geodata/Clientes/0FARMS/NC/'
max_cloud = 50
#bucketname = 'sanca'
satellite = 'Landsat'

# %% GET COLLECTION AND PARAMETERS
URL = 'https://landsatlook.usgs.gov/stac-server'
cat = Client.open(URL)
collection_id = 'landsat-c2l2-sr'
collection = cat.get_collection(collection_id)
print(collection)

query_params = {
        "eo:cloud_cover": {"lt": max_cloud},
        "platform": {"in": ["LANDSAT_8", "LANDSAT_9"]},
       "landsat:collection_category": { "in": ['T1']}
                }

assets = ['red', 'blue', 'nir08', 'swir16' ] #'green', 'qa_pixel'

y0 = datetime.strptime(datetime_rangefull.split('/')[0],'%Y-%m-%d').year
yf = datetime.strptime(datetime_rangefull.split('/')[1],'%Y-%m-%d').year
m0 = str(datetime.strptime(datetime_rangefull.split('/')[0],'%Y-%m-%d').month).zfill(2)
mf = str(datetime.strptime(datetime_rangefull.split('/')[1],'%Y-%m-%d').month).zfill(2)
d0 = str(datetime.strptime(datetime_rangefull.split('/')[0],'%Y-%m-%d').day).zfill(2)
df = str(datetime.strptime(datetime_rangefull.split('/')[1],'%Y-%m-%d').day).zfill(2)
# %%

for ano in range(y0,yf):

    datetime_range = f'{ano}-{m0}-{d0}/{ano+1}-{m0}-{d0}'
    if yf == (ano+1):
        datetime_range = f'{ano}-{m0}-{d0}/{ano+1}-{mf}-{df}'
    print(ano, datetime_range)

    datetime_range_name = datetime_range.replace('/','_')
    ds = get_cube(datetime_range, 
                                     cat, 
                                     collection_id, 
                                     bbox, 
                                     query_params, 
                                     aws_session, assets)
    ds = dropper( ds , sat = satellite ) 
    #ds.to_netcdf(f'{folder_nc}/DS_{datetime_range_name}.nc')
    # CALCULATE indices
    ndvi, sfndvi = NDVI( ds )
    ndvi = dropper(ndvi, 'Landsat')
    bsi, sfbsi = BSI( ds )
    bsi = dropper(bsi, 'Landsat')
    ndvi.to_netcdf(f'{folder_nc}/ndvi_{datetime_range_name}.nc')
    bsi.to_netcdf(f'{folder_nc}/bsi_{datetime_range_name}.nc')
    
# %%
