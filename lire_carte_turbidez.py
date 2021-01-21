import gdal
import pandas as pd

layer = 'aire.tif'
nom = layer[:-4]

ds = gdal.Open(layer)

arr = ds.GetRasterBand(1).ReadAsArray()
m,n = ds.RasterYSize, ds.RasterXSize

ind_i=[]
ind_j=[]
values=[]

for i in range(m-1):
	for j in range(n-1):
		ind_i.append(i)
		ind_j.append(j)
		values.append(arr[i][j])
		
data = {'i':ind_i, 'j':ind_j, 'valeur':values}

pd.DataFrame(data=data).to_csv(nom+'.csv')
		
