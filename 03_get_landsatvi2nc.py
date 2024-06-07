#%%
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
#%%
run = 'f5'

farm_file = '/home/geodata/Clientes/0FARMS/FabioRicardi-Barreiras_BA/vetorial/CARs-Fazenda_Savana.gpkg'
farm_file = '/home/geodata/Clientes/0FARMS/MG-3102605-B4D344DBFD874F44906FCC0A5E0DCE36/CAR.gpkg'
layer = 'car'
folderout = '/home/geodata/Clientes/0FARMS/FabioRicardi-Barreiras_BA/nc/'
folderout = '/home/geodata/Clientes/0FARMS/MG-3102605-B4D344DBFD874F44906FCC0A5E0DCE36/nc/'

#  Satellite imagery query params
today = date.today()
datetime_rangefull = str(f"2015-06-20/{str(today)}") #break  2017
#datetime_rangefull = str(f"1985-06-20/2013-06-20") #break  2017
max_cloud = 100
#bucketname = 'sanca'
satellite = 'Landsat'
platforms = ["LANDSAT_5","LANDSAT_8", "LANDSAT_9"] #"LANDSAT_7",


folder_nc = f'{folderout}{run}/'
print(folder_nc)

# Set environment and create AWS Session
os.environ['CURL_CA_BUNDLE'] = '/etc/ssl/certs/ca-certificates.crt'
os.environ['AWS_REQUEST_PAYER'] = 'requester'

print("Creating AWS Session")
aws_session = rio.session.AWSSession(boto3.Session(), requester_pays=True)
print(aws_session)

# open Farm
try:
    farm = gpd.read_file(farm_file, layer=layer)
except:
    farm = gpd.read_file(farm_file)

bbox = get_bbox(farm)



# %% GET COLLECTION AND PARAMETERS
URL = 'https://landsatlook.usgs.gov/stac-server'
cat = Client.open(URL)
collection_id = 'landsat-c2l2-sr'
collection = cat.get_collection(collection_id)
print(collection)

query_params = {
        "eo:cloud_cover": {"lt": max_cloud},
        "platform": {"in": platforms},
       "landsat:collection_category": { "in": ['T1']}
                }

assets = ['red', 'blue', 'nir08', 'swir16'] #'green', 'qa_pixel'

y0 = datetime.strptime(datetime_rangefull.split('/')[0],'%Y-%m-%d').year
yf = datetime.strptime(datetime_rangefull.split('/')[1],'%Y-%m-%d').year
m0 = str(datetime.strptime(datetime_rangefull.split('/')[0],'%Y-%m-%d').month).zfill(2)
mf = str(datetime.strptime(datetime_rangefull.split('/')[1],'%Y-%m-%d').month).zfill(2)
d0 = str(datetime.strptime(datetime_rangefull.split('/')[0],'%Y-%m-%d').day+1).zfill(2)
df = str(datetime.strptime(datetime_rangefull.split('/')[1],'%Y-%m-%d').day-2).zfill(2)
if int(d0)>31: d0 = '30'
if int(df)<1: df = '01'
# %%
Path(folder_nc).mkdir( parents = True, exist_ok = True)
for ano in range(y0,yf):
    '''
        TODO falta um globals aqui pra player com os dados
    '''
    d0_ = str(int(d0)-2).zfill(2)
    datetime_range = f'{ano}-{m0}-{d0}/{ano+1}-{m0}-{d0_}'
    if yf == (ano+1):
        datetime_range = f'{ano}-{m0}-{d0}/{ano+1}-{mf}-{df}'
    print(ano, datetime_range)

    #datetime_range = '2013-06-20/2024-05-08' SE QUISER FAZER A LOUCURA DE MANDAR TUDO DE UMA VEZ
    datetime_range_name = datetime_range.replace('/','_')
    try:
        ds = get_cube(datetime_range, 
                                        cat, 
                                        collection_id, 
                                        bbox, 
                                        query_params, 
                                        aws_session, assets)
        ds = dropper( ds , sat = satellite ) 


        # é aqui que a filtragem acontece
        ds2 = ds.copy()

        # valores extremos 

        for asset in assets:
            ds2[asset] = xr.where(ds2[asset] > 40000, np.nan, ds2[asset])
            ds2[asset] = xr.where(ds2[asset] < 5000, np.nan, ds2[asset])

            print(asset)
            quantiles = [0.01,0.1,0.25,0.5,0.75,0.9,0.99]
            print(np.nanquantile(ds2[asset],quantiles))
            ds2[asset] = xr.where(ds2[asset] < np.nanquantile(ds2[asset],[0.01]), np.nan, ds2[asset])
            ds2[asset] = xr.where(ds2[asset] > np.nanquantile(ds2[asset],[0.99]), np.nan, ds2[asset])
            if asset == 'blue':
                ds2[asset] = xr.where(ds2[asset] > np.nanquantile(ds2[asset],[0.84]), np.nan, ds2[asset])
            
        # interpolate_na
        ds2 = ds2.chunk(dict(time=-1))
        ds2 = ds2.interpolate_na(dim="time",
                method='linear',
                use_coordinate=True, 
            ) 

        # rolling
        w = 3
        ds2 = ds2.rolling(time=w, center=True).mean(skipna=True)


        # # REPROJECTION
        print(f'reprojecting cube for {datetime_range}')
        ds2 = ds2.rio.write_crs('epsg:4326')
        ds2 = ds2.rio.reproject('EPSG:4326')
        ds2 = ds2.rename({'x': 'longitude','y': 'latitude'})
        print('reprojecting... done')

        ds2.to_netcdf(f'{folder_nc}/raw_{datetime_range_name}.nc')
        print(f'>> {folder_nc}/raw_{datetime_range_name}.nc')

        # CALCULATE indices
        ndvi = NDVI( ds2 )
        bsi= BSI( ds2 )

        # get dates to save exact name
        t0 = str(ndvi.time[0].values).split('T')[0]
        t1 = str(ndvi.time[-1].values).split('T')[0]
        n = len(ndvi.time)
        try:
            ndvi = dropper(ndvi, 'Landsat')
            bsi = dropper(bsi, 'Landsat')
        except:
            print('not dropping, probably wont save')
        # save nc
        ndvi.to_netcdf(f'{folder_nc}/ndvi_{t0}-{t1}_{n}.nc')
        print('> ndvi saved')
        bsi.to_netcdf(f'{folder_nc}/bsi_{t0}-{t1}_{n}.nc')
        print('> bsi saved') 

    except:
        print(f'{datetime_range} FAILED')


# %% PLOTTING STUFF
# # ndvi.clip(0,1000)
# # # %% TEMOS UM ds


# #%% a copy
# ds2 = ds.copy(deep=True)

# for asset in assets:
#     plt.hist(np.ravel(ds2[asset].values), bins = 100)
#     plt.title(asset); plt.grid();plt.show(); plt.close()

# ts = ds2.sel(longitude = -46.64329, latitude = -22.07701, method = 'nearest')
# for asset in assets:
#     plt.plot(ts[asset].values, label = asset)

# #%% filta os principais outliers
# ds2 = ds.copy(deep=True)
# ds2 = xr.where(ds2 > 40000, np.nan, ds2)
# ds2 = xr.where(ds2 < 5000, np.nan, ds2)

# for asset in assets:
#     quantiles = [0.01,0.1,0.25,0.5,0.75,0.9,0.99]
#     print(quantiles)
#     print(np.nanquantile(ds2[asset],quantiles))
#     ds2[asset] = xr.where(ds2[asset] < np.nanquantile(ds2[asset],[0.01]), np.nan, ds2[asset])
#     ds2[asset] = xr.where(ds2[asset] > np.nanquantile(ds2[asset],[0.99]), np.nan, ds2[asset])
#     if asset == 'blue':
#         ds2[asset] = xr.where(ds2[asset] > np.nanquantile(ds2[asset],[0.76]), np.nan, ds2[asset])
#         #ds2[asset] = xr.where(ds2[asset] < np.nanquantile(ds2[asset],[0.01]), np.nan, ds2[asset])
#     plt.hist((np).ravel(ds2[asset].values), bins = 100)
#     plt.title(asset); plt.grid();plt.show(); plt.close()

# # farm.centroid
# ts2 = ds2.sel(longitude = -46.64329, latitude = -22.07701, method = 'nearest')
# for asset in assets:
#     plt.plot(ts2[asset].values, label = asset)
# plt.legend()
# plt.grid()


# # %% 
# #f -> Interpolate NaN
# #ds3 = ds2.copy()
# method = 'linear'
# ds3 = ds2.interpolate_na(dim="time",
#             method=method,
#             use_coordinate=True, 
#         ) # pchip  # limit = 7, use_coordinate=True,


# ts3 = ds3.sel(longitude = -46.64329, latitude = -22.07701, method = 'nearest')

# for asset in assets:
#     plt.plot(ts3[asset].values, label = asset)
# plt.grid();plt.show();plt.close()

# for asset in assets:
#     plt.hist(np.ravel(ds3[asset].values), bins = 100)
#     plt.title(asset); plt.grid();plt.show(); plt.close()

# # %% 
# # f -> rolling
# w = 3
# ds4 = ds3.rolling(time=w, center=True).mean(skipna=True)#savgol_filter, window=w, polyorder=2
# ts4 = ds4.sel(longitude = -46.64329, latitude = -22.07701, method = 'nearest')

# for asset in assets:
#     plt.plot(ts4[asset].values, label = asset)
    
# plt.grid();plt.show();plt.close()

# for asset in assets:
#     plt.hist(np.ravel(ds4[asset].values), bins = 100)
#     plt.title(asset); plt.grid();plt.show(); plt.close()
# # %% 
# for asset in assets:
#     plt.hist(np.ravel(ds4[asset].values), bins = 100)
#     plt.title(asset)
#     plt.show(); plt.close()
# ts2 = ds3.sel(longitude = -46.64329, latitude = -22.07701, method = 'nearest')

# for asset in assets:
#     plt.plot(ts2[asset].values, label = asset)
# plt.legend()
# plt.grid()

# for asset in assets:
#     plt.hist(np.ravel(ds4[asset].values), bins = 100)
#     plt.title(asset)
#     plt.title(asset); plt.grid();plt.show(); plt.close()


# %%
