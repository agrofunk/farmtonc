
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
    # REPROJECTION
    print(f'reprojecting cube for {datetime_range}')
    ds = ds.rio.write_crs('epsg:4326')
    ds = ds.rio.reproject('EPSG:4326')
    ds = ds.rename({'x': 'longitude','y': 'latitude'})
    print('reprojecting... done')

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
                    'raster:bands','eo:cloud_cover'
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



def NDVI( ds , to_int = True ):
    '''
        NDVI for Landsat 8 and 9
        we apply a multiplying factor of 1000 to save data as uint16
    '''


    if to_int == True:
        sf = 1000
    else:
        sf = 1

    ndvi = ((ds['nir08'] - ds['red']) / (ds['nir08'] + ds['red'])) * sf
    ndvi.name = 'ndvi'
    ndvi = ndvi.astype('uint16')
    print(f'the result was multiplied by {sf}')

    return ndvi, sf


def BSI( ds, to_int = True ):
    '''
        BSI (Bare Soil Index) for Landsat 8 and 9
        XXX HAVE TO FIGURE OUT HOW TO SCALE WITH NEGATIVE NUMBERS
    '''

    if to_int == True:
        sf = 1000
    else:
        sf = 1

    bsi = ((ds['swir16'] + ds['red']) - (ds['nir08'] + ds['blue'])) / ((ds['swir16'] + ds['red']) + (ds['nir08'] + ds['blue'])) * sf
    bsi.name = 'bsi'
    bsi = bsi.astype('uint16')
    print(f'the result was multiplied by {sf}')

    return bsi, sf