
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


