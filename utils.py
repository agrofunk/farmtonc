# DUPE TEST
def dupetest(df):
    '''
        to test the consistency for time axis, looking for duplicates
    '''
    import numpy as np
    if len(df.time) != len(np.unique(df.time.values)):
        seen = set()
        dupes = [x for x in df.time.values if x in seen or seen.add(x)] 
        print(dupes)
    else:
        print('your time-series has no dupes, good job!')



def state2muni(file,outfolder):
    '''
        Split a state of CAR into municipalities and save each one as a gpkg file

    '''

    import geopandas
    import os

    if not os.path.exists(outfolder): 
        os.makedirs(outfolder)
        print(f'{outfolder} created')

    state = geopandas.read_file(file)
    state['municipio_code'] = state.cod_imovel.apply(lambda x: x.split('-')[1])

    for mun in state.municipio_code.unique():
        municipio = state[state.municipio_code == mun]
        municipio.to_file(outfolder + mun + '.gpkg', driver = 'GPKG')
        print(outfolder + mun + '.gpkg SAVED')


'''
    OLD USEFUL functions

'''

# FUNCTION get_cube() - original from starterkit
def get_cube(datetime_range, cat, collection_id, bbox, query_params, aws_session, assets):
    import rasterio as rio
    from stackstac import stack
    import pystac
    import rioxarray

    # search
    search = cat.search(
        collections = [collection_id],
        bbox = bbox,
        datetime = datetime_range,
        query = query_params,
    )
    print(f"{search.matched()} items found \n ---")
    # prepare items collection
    items_dict = search.item_collection_as_dict()['features']

    # update URLs to use s3
    for item in items_dict:
        for a in item['assets']:
            if 'alternate' in item['assets'][a] and 's3' in item['assets'][a]['alternate']:
                item['assets'][a]['href'] = item['assets'][a]['alternate']['s3']['href']
            item['assets'][a]['href'] = item['assets'][a]['href'].replace('usgs-landsat-ard', 'usgs-landsat')

    item_collection = pystac.ItemCollection(items_dict)

    # LOAD
    with rio.Env(session = aws_session, AWS_S3_ENDPOINT= 's3.us-west-2.amazonaws.com'):
        dc = stack(item_collection,
                        assets= assets,
                        chunksize = 256,
                        bounds_latlon = bbox,
                        epsg = 4326,
                        rescale=False, 
                        fill_value=0, 
                        dtype="uint16"
                        )
        
    ds = dc.to_dataset( dim = 'band' ).persist()
    del dc
    # # REPROJECTION
    # print(f'reprojecting cube for {datetime_range}')
    # ds = ds.rio.write_crs('epsg:4326')
    # ds = ds.rio.reproject('EPSG:4326')
    # ds = ds.rename({'x': 'longitude','y': 'latitude'})
    # print('reprojecting... done')

    return ds


def dropper( ds, sat = 'Landsat' ):
    '''
        Drop vars
        TODO drop attrs e.g. 'spec'
    '''

    if sat == 'Landsat':
        drops = ['landsat:correction','landsat:wrs_path',
                 'landsat:wrs_row','landsat:cloud_cover_land',
                    'landsat:collection_number','landsat:wrs_type','instruments',
                    'raster:bands','eo:cloud_cover','accuracy:geometric_x_stddev', 
                    'accuracy:geometric_y_stddev', 'accuracy:geometric_rmse'
                    ]
        
    # if sat == 'Sentinel':
    #    drops
    
    for d in drops:
        try:
            ds = ds.drop_vars(d)
            print(f'<<< {d} dropped')
        except:
            print(f'--- {d} was not here')
    try:
        del ds.attrs['spec']
        print('spec attribute deleted')
    except:
        print('no attribute spec to remove')

    return ds


def get_bbox( gdf ):
    '''
        get bbox from gdf.:GeoDataframe:

        return bbox
    '''

    bbox = (gdf.bounds.minx.min(),
            gdf.bounds.miny.min(),
            gdf.bounds.maxx.max(),
            gdf.bounds.maxy.max()
            )
    return bbox



def NDVI( ds ):
    '''
        NDVI for Landsat 8 and 9
        we apply a multiplying factor of 1000 to save data as uint16
    '''


    ndvi = ((ds['nir08'] - ds['red']) / (ds['nir08'] + ds['red'])) 
    ndvi.name = 'NDVI'
    ndvi = ndvi.astype('float32')

    return ndvi


def BSI( ds ):
    '''
        BSI (Bare Soil Index) for Landsat 8 and 9
        XXX HAVE TO FIGURE OUT HOW TO SCALE WITH NEGATIVE NUMBERS
    '''


    bsi = ((ds['swir16'] + ds['red']) - (ds['nir08'] + ds['blue'])) / ((ds['swir16'] + ds['red']) + (ds['nir08'] + ds['blue'])) 
    bsi.name = 'BSI'
    bsi = bsi.astype('float32')

    return bsi




def zscore( ds , how = 'month' ):
    '''
        Calculate zscores 
    '''
    if how == 'month':
        timeformat = "%Y-%m"
        timelabel = "year_month"
    if how == 'week':
        timeformat = "%Y-%W"
        timelabel = 'year_week'

    print('computing ...')
    ds = ds.assign_coords(year_month=ds.time.dt.strftime(timeformat))
    ds_anom = ds.groupby(timelabel) - ds.groupby(timelabel).mean("time")
    ds_z = ds_anom.groupby(timelabel) / ds.groupby(timelabel).std("time")
   
    ds_anom.compute()
    ds_z.compute()
    print('computing ... done')
    return ds_anom, ds_z


def climatology( ds ):
    '''

    '''
    ds_mean = ds.groupby("time.month").mean("time")
    ds_std = ds.groupby("time.month").std("time")
    return ds_mean, ds_std




# def get_lst(lwirband, items, dst, w=5):
#     """
#     Convert lwir to Celcius and prepare dataset for further processing
#     lwirband (str): 'lwir' for 457 and lwirband for 89
#     da (DataArray loaded from items__)
#     w (int): rolling mean window size, default is 5
#     """
#     # get lwir11 band info
#     band_info = items[0].assets[lwirband].extra_fields["raster:bands"][0]
#     print(band_info)

#     dst[lwirband] = dst[lwirband].astype(float)
#     dst[lwirband] *= band_info["scale"]
#     dst[lwirband] += band_info["offset"]
#     dst[lwirband] -= 273.15

#     # variables to drop so I can save the .nc later on
#     drops = [
#         "landsat:correction",
#         "landsat:wrs_path",
#         "landsat:wrs_row",
#         "landsat:collection_number",
#         "landsat:wrs_type",
#         "instruments",
#         "raster:bands",
#         "instruments",
#     ]
#     dst = dst.drop_vars(drops)
#     # interpolate NaNs (rechunk it first)
#     dst = dst.chunk(dict(time=-1))
#     dst[lwirband] = xr.where(dst[lwirband] < 1, np.nan, dst[lwirband])  #
#     dst[lwirband] = xr.where(dst[lwirband] > 65, np.nan, dst[lwirband])
#     dst[lwirband] = dst[lwirband].interpolate_na(dim="time", method="linear")

#     # I`m overwriting the raw data
#     dst[lwirband] = dst[lwirband].rolling(time=w, center=True).mean(savgol_filter, window=w, polyorder=2)
#     del band_info
#     return dst



