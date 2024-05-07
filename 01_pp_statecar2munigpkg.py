'''
    This is por official data pre-processes

    convert state CAR layers downloade from sicar.gov.br to municipality gpkg by municipality
    - ja tenho que aprender a compor gpkg mesmo

'''

from utils import state2muni
from glob import glob

estado = 'GO'
#files = glob(f'/home/geodata/Vetorial/fundiario/CAR/{estado}/**/*.shp')
files = glob(f'/home/geodata/Vetorial/fundiario/CAR/{estado}/**/*.shp', recursive=True)
outfolder_base = f'/home/geodata/Vetorial/fundiario/CAR/{estado}/0muni/'

for f in files:
    name = f.split('/')[-1][:-4]
    print(name)
    outfolder = outfolder_base + name + '/'
    try:
        state2muni(f, outfolder)
    except:
        print(f'no stuff for {name}')
    #print(name)
