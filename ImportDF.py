import os
import sys
import glob
import numpy as np
import pandas as pd
from datetime import datetime
from os.path import getmtime



def change_columnsName(df):
    for i in df.columns:
        df.rename(columns={i:i + '_' + df.name},inplace=True)
    return df


def ImportDF(pathImportSI):
    #pathImportSI = os.getcwd() + '/export/'+folderName
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    li = []
    lastData = datetime.fromtimestamp(getmtime(all_filesSI[0])).strftime('%Y%m%d')
    for filename in all_filesSI:
        fileData = datetime.fromtimestamp(getmtime(filename)).strftime('%Y%m%d')
        iter_csv = pd.read_csv(filename, index_col=None, encoding="UTF-8",header=0,skiprows=2, error_bad_lines=False,dtype=str, sep = ';',decimal=',',iterator=True, chunksize=10000 )
        df = pd.concat([chunk for chunk in iter_csv]) # & |  WORKS 
        li.append(df)
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI = frameSI.drop_duplicates()

    return frameSI 
