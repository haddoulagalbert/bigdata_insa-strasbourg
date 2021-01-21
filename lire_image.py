import gdal
import pandas as pd

layer = 'image.tif'
nom = layer[:-4]

ds = gdal.Open(layer)

arr1 = ds.GetRasterBand(1).ReadAsArray()
arr2 = ds.GetRasterBand(2).ReadAsArray()
arr3 = ds.GetRasterBand(3).ReadAsArray()

m,n = ds.RasterYSize, ds.RasterXSize

ind_i=[]
ind_j=[]
rouge=[]
vert=[]
bleu=[]

for i in range(m-1):
	for j in range(n-1):
		ind_i.append(i)
		ind_j.append(j)
		rouge.append(arr1[i][j])
    vert.append(arr2[i][j])
    bleu.append(arr3[i][j])
		
data = {'i':ind_i, 'j':ind_j, 'rouge':rouge, 'vert':vert, 'bleu': bleu}

pd.DataFrame(data=data).to_csv(nome+'.csv')
		
