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
import matplotlib.pyplot as plt

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
datetime_rangefull = str(f"2013-06-20/{str(today)}")
local_folder = '/home/geodata/Clientes/0FARMS/NC/'
max_cloud = 80
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
        "platform": {"in": ["LANDSAT_5","LANDSAT_7","LANDSAT_8", "LANDSAT_9"]},
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
ndvi.clip(0,1000)
# %% TEMOS UM ds


#%% a copy
ds2 = ds.copy(deep=True)
plt.hist(np.ravel(ds2['red'].values), bins = 100)

#%% filta os principais outliers
ds2 = xr.where(ds2 > 42000, np.nan, ds2)
ds2 = xr.where(ds2 < 1500, np.nan, ds2)
# %% 
for asset in assets:
    plt.hist(np.ravel(ds2[asset].values), bins = 100)
    plt.title(asset)
    plt.show(); plt.close()

# %% centroid time-view
# farm.centroid
ts = ds2.sel(longitude = -46.64329, latitude = -22.07701, method = 'nearest')
for asset in assets:
    plt.plot(ts[asset].values, label = asset)
plt.legend()
plt.grid()

# %% Interpolate NaN
ds3 = ds2.copy()
ds3.interpolate_na(dim="time",
            method="linear", 
        )
# for asset in assets:
#     ds3[asset] = ds3[asset].interpolate_na(
#             dim="time",
#             method="linear",  # pchip
#             # limit = 7,
#             use_coordinate=True,
#         )

# %%
ts2 = ds3.sel(longitude = -46.64329, latitude = -22.07701, method = 'nearest')
for asset in assets:
    plt.plot(ts2[asset].values, label = asset)
plt.legend()
plt.grid()

# %%
from scipy.signal import savgol_filter

w = 6
ds4 = ds.rolling(time=w, center=True).mean(savgol_filter, window=w, polyorder=2)
# %% 
for asset in assets:
    plt.hist(np.ravel(ds4[asset].values), bins = 100)
    plt.title(asset)
    plt.show(); plt.close()
# %% last consequences
ds2 = ds.copy(deep=True)
ds2['swir16'] = xr.where(ds['swir16'] > 23000, np.nan, ds['swir16'])
ds2['swir16'] = xr.where(ds['swir16'] < 5000, np.nan, ds['swir16'])

ds2['nir08'] = xr.where(ds['nir08'] < 10000, np.nan, ds['nir08'])

ds2['blue'] = xr.where(ds['blue'] < 2000, np.nan, ds['blue'])
ds2['blue'] = xr.where(ds['blue'] > 22000, np.nan, ds['blue'])
ds2['red'] = xr.where(ds['red'] < 4000, np.nan, ds['red'])
ds2['red'] = xr.where(ds['red'] > 24000, np.nan, ds['red'])


# interpolate_na
# https://docs.xarray.dev/en/stable/generated/xarray.DataArray.interpolate_na.html
for asset in assets:
    ds2[asset] = ds2[asset].interpolate_na(dim = 'time', 
                                          method = 'quadratic' 
                                          )

    plt.hist(np.ravel(ds2[asset].values), 
             bins = 100)
    plt.title(asset)
    plt.show(); plt.close()
# %% TESTER xarray.DataArray.interp
https://docs.xarray.dev/en/stable/generated/xarray.DataArray.interp.html


