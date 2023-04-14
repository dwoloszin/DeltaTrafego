import os
import sys
import timeit
import ImportDF
import pandas as pd
import numpy as np
import inspect
import time
import locale
from locale import atof
locale.setlocale(locale.LC_NUMERIC, '')

script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
timeexport = time.strftime("%Y%m%d_")

def MS(TEC):
  inicio = timeit.default_timer()
  
  this_function_name = inspect.currentframe().f_code.co_name
  print(this_function_name, ' Processing...')
  pathToImport = script_dir + '/import/'+TEC
  pathToSave = script_dir + '/export/'+this_function_name +'/'
  if not os.path.exists(pathToSave):
    os.makedirs(pathToSave)
  if os.path.exists(pathToImport):   
    Frame = ImportDF.ImportDF(pathToImport)
    Frame.drop_duplicates(inplace=True)
    Frame['TEC'] = TEC
    Frame = tratarArchive(Frame)
    
    

   

    #Frame.drop(droplist, errors='ignore', axis=1,inplace=True)
    Frame.to_csv(pathToSave + TEC+'_' + this_function_name + '.csv',index=False,header=True,sep=';')
  fim = timeit.default_timer()
  print ('duracao: %.2f' % ((fim - inicio)/60) + ' min')

def tratarArchive(Frame):
  renameColuns = {'Semana do Ano':'YearWeek','Município':'Municipio','Unnamed: 3':'IBGE','Classificação Pop Urbana Anatel':'Classificacao','Célula':'Celula','Métrica':'Metrica',
                  'DISP_COUNTER_TOTAL 2G (com filtro OPER)':'DISP',
                  'VOLUME_DADOS_DLUL_ALLOP 2G': 'VOLUME',
                  'TRAFEGO_VOZ_ALLOP 2G': 'TRAFEGO',
                  'DISP_COUNTER_TOTAL 3G (com filtro OPER)':'DISP',
                  'VOLUME_DADOS_DLUL_ALLOP 3G - Mbyte': 'VOLUME',
                  'TRAFEGO_VOZ_TIM 3G':'TRAFEGO',
                  'DISP_COUNTER_TOTAL 4G (com filtro OPER)': 'DISP',
                  'VOLUME_DADOS_DLUL_TIM 4G - Gbyte': 'VOLUME',
                  'TRAFEGO_VOZ_TIM 4G':'TRAFEGO',
                  'DISP_COMB_TOTAL 5G (com filtro OPER)': 'DISP',
                  'VOLUME_TOTAL_DLUL_TIM 5G - Gbyte':'VOLUME'}
    
  for key, value in renameColuns.items():
    try:
      Frame.rename(columns={key:value},inplace=True)
    except:
      pass
  
  Frame['CELL'] = Frame['Celula'].str[-1:].map({'0':'0', '1':'1', '2':'2', '3':'3', 'A':'1', 'B':'2', 'C':'3', 'D':'4', 'E':'1', 'F':'2', 'G':'3', 'H':'4', 'I':'1', 'J':'2', 'K':'3', 'L':'4', 'M':'1', 'N':'2', 'P':'3', 'Q':'1', 'R':'2', 'S':'3', 'T':'4', 'U':'2', 'V':'3', 'W':'0', 'X':'1', 'Y':'2', 'Z':'3'})
  #Frame['RefCell'] = Frame['RAN Node'].astype(str) + Frame['CELL'].astype(str)
  

  #DropAnalise
  #Frame.loc[(Frame['Banda'].astype(str) == '2100') & (Frame['TEC'].astype(str) == '4G'),['DropAnalise']] = 'TRUE'
  Frame.loc[(Frame['ANF'].astype(str) == '11') & (Frame['Banda'].astype(str) == '700')& (Frame['TEC'].astype(str) == '5G'),['DropAnalise']] = 'TRUE'




  Frame['DISP'] = Frame['DISP'].str.strip('()%')
  #Frame['revenue'] = Frame.get('revenue', Frame['Volume'] * Frame['TRAFEGO'])
  Frame['TRAFEGO'] = Frame.get('TRAFEGO', 0) # create a column if not exist
  Frame['VOLUME'] = Frame['VOLUME'].astype(str).apply(locale.atof)
  Frame['TRAFEGO'] = Frame['TRAFEGO'].astype(str).apply(locale.atof)
  Frame['DISP'] = Frame['DISP'].astype(str).apply(locale.atof)
  Frame['DISP'] = (Frame['DISP']/100).round(2)
  Frame['YearWeek'] =  + Frame['YearWeek'].str[-4:] + Frame['YearWeek'].str[:3]    
  Frame.sort_values(by=['YearWeek','RAN Node','Celula'], ascending=True,inplace=True)
  LastDate = Frame['YearWeek'].max()
  print(LastDate)
  Frame['ref'] = Frame['RAN Node'].astype(str) + Frame['CELL'].astype(str) + Frame['YearWeek'].astype(str)
  Frame['ref2'] = Frame['RAN Node'].astype(str) + Frame['CELL'].astype(str)
  FrameSUM = Frame.groupby(['ref']).agg({'VOLUME':'sum','TRAFEGO':'sum','DISP':'mean'}).round(2).reset_index()#mean, median
  FrameSUM.rename(columns={'VOLUME':'VOLUME(sum)','TRAFEGO':'TRAFEGO(sum)','DISP':'DISP(mean)'},inplace=True)
  
  Frame = pd.merge(Frame,FrameSUM, how='left',left_on=['ref'],right_on=['ref'])

  Frame2 = Frame.loc[Frame['YearWeek'].astype(str) != LastDate]
  Frame2 = Frame2.groupby(['ref2']).agg({'VOLUME':'median','TRAFEGO':'median','DISP':'median'}).round(2).reset_index()#mean, median
  Frame2.rename(columns={'VOLUME':'VOLUME(median)','TRAFEGO':'TRAFEGO(median)','DISP':'DISP(median)'},inplace=True)

  Frame = pd.merge(Frame,Frame2, how='left',left_on=['ref2'],right_on=['ref2'])

  KPI = ['VOLUME','TRAFEGO']
  for k in KPI:
    Frame[k+'%'] = ((Frame[k+'(sum)']/Frame[k+'(median)'])-1.0).round(2)
  
  tqV = -0.6
  for k in KPI: 
    Frame.loc[(Frame['YearWeek'].astype(str) == LastDate) &(Frame['DISP'].astype(float) >= 1.0) &(Frame[k+'%'].astype(float) <= tqV),['VERIFICAR_'+k]] = k


  




  selected_cols = ['VERIFICAR_VOLUME', 'VERIFICAR_TRAFEGO'] # select the columns to concatenate
  separator = '|' # define the separator
  Frame['VERIFICAR'] = Frame[selected_cols].apply(lambda row: separator.join(map(str, row)), axis=1)
  Frame['VERIFICAR'] = Frame['VERIFICAR'].str.split('|').apply(pd.unique)
  Frame['VERIFICAR'] = ['|'.join(map(str, l)) for l in Frame['VERIFICAR']]
  Frame['VERIFICAR'] = Frame['VERIFICAR'].str.replace('nan', '')
  Frame['VERIFICAR'] = Frame['VERIFICAR'].map(lambda x: x.lstrip('|').rstrip('|'))

  #dropList = ['Tecnologia','Metrica','ref','ref2','VERIFICAR_VOLUME','VERIFICAR_TRAFEGO','VERIFICAR_DISP']
  dropList = ['Tecnologia','Metrica','ref','VERIFICAR_VOLUME','VERIFICAR_TRAFEGO','CELL']
  Frame.drop(dropList,1,inplace=True)





  '''
  Frame['ref'] = Frame['CELL'] + Frame['RAN Node']
  #Frame['ref2'] = Frame['RAN Node'] + Frame['YearWeek']
  Frame2 = Frame.loc[Frame['YearWeek'].astype(str) != LastDate]
  FrameSum = Frame2.groupby(['CELL','RAN Node']).agg({'VOLUME':'sum','TRAFEGO':'sum','DISP':'mean'}).round(2).reset_index()#mean, median

  Frame2 = Frame2.groupby(['CELL','RAN Node']).agg({'VOLUME':'median','TRAFEGO':'median','DISP':'median'}).round(2).reset_index()#mean, median
  Frame2.rename(columns={'VOLUME':'VOLUME(median)','TRAFEGO':'TRAFEGO(median)','DISP':'DISP(median)'},inplace=True)

  Frame2['ref'] = Frame2['CELL'] + Frame2['RAN Node']
  Frame2.drop(['CELL','RAN Node'], axis=1,inplace=True)

  
  #PESO
  #FramePeso = Frame.loc[Frame['YearWeek'].astype(str) != LastDate]
  FramePeso = Frame.copy()
  FramePeso = FramePeso.groupby(['RAN Node','YearWeek']).agg({'VOLUME':'sum','TRAFEGO':'sum'}).round(2).reset_index()
  FramePeso.rename(columns={'VOLUME':'VOLUME(sum)','TRAFEGO':'TRAFEGO(sum)'},inplace=True)

  FramePeso['ref2'] = FramePeso['RAN Node'] + FramePeso['YearWeek']
  FramePeso.drop(['RAN Node','YearWeek'], axis=1,inplace=True)

  FramePeso = FramePeso.groupby(['ref2']).agg({'VOLUME':'sum','TRAFEGO':'sum'}).round(2).reset_index()
  





  Frame = pd.merge(Frame,Frame2, how='left',left_on=['ref'],right_on=['ref'])
  #Frame = pd.merge(Frame,FramePeso, how='left',left_on=['ref2'],right_on=['ref2'])

  KPI = ['VOLUME','TRAFEGO','DISP']
  for k in KPI:
    Frame[k+'%'] = ((Frame[k]/Frame[k+'(median)'])-1.0).round(2)
  
  
  #PESO
  KPI2 = ['VOLUME','TRAFEGO']
  for k in KPI2:
    Frame[k+'_PESO(%)'] = ((Frame[k]/Frame[k+'(sum)'])).round(2)

  

  tqV = -0.6
  for k in KPI: 
    Frame.loc[(Frame['YearWeek'].astype(str) == LastDate) &(Frame['DISP'].astype(float) >= 1.0) &(Frame[k+'%'].astype(float) <= tqV),['VERIFICAR_'+k]] = k

  #DropAnalise
  #Frame.loc[(Frame['Banda'].astype(str) == '2100') & (Frame['TEC'].astype(str) == '4G'),['DropAnalise']] = 'TRUE'
  #Frame.loc[(Frame['ANF'].astype(str) == '11') & (Frame['Banda'].astype(str) == '700')& (Frame['TEC'].astype(str) == '5G'),['DropAnalise']] = 'TRUE'



  selected_cols = ['VERIFICAR_VOLUME', 'VERIFICAR_TRAFEGO','VERIFICAR_DISP'] # select the columns to concatenate
  separator = '|' # define the separator
  Frame['VERIFICAR'] = Frame[selected_cols].apply(lambda row: separator.join(map(str, row)), axis=1)
  Frame['VERIFICAR'] = Frame['VERIFICAR'].str.split('|').apply(pd.unique)
  Frame['VERIFICAR'] = ['|'.join(map(str, l)) for l in Frame['VERIFICAR']]
  Frame['VERIFICAR'] = Frame['VERIFICAR'].str.replace('nan', '')
  Frame['VERIFICAR'] = Frame['VERIFICAR'].map(lambda x: x.lstrip('|').rstrip('|'))

  #dropList = ['Tecnologia','Metrica','ref','ref2','VERIFICAR_VOLUME','VERIFICAR_TRAFEGO','VERIFICAR_DISP']
  dropList = ['Tecnologia','Metrica','ref','VERIFICAR_VOLUME','VERIFICAR_TRAFEGO','VERIFICAR_DISP']
  Frame.drop(dropList,1,inplace=True)
  

 
  
  #Por enquanto sem normalização dos dados
  
  CoversionVolume = {'2G':2048,'3G':1024,'4G':1,'5G':1}
  
  for key, value in CoversionVolume.items():
    Frame.loc[Frame['TEC'] == key,['Volume3']] = (Frame['Volume2']/value).round(2)
  
  #print(Frame.loc[~Frame['VERIFCAR'].isna()])
  '''
  return Frame

TEC_List = ['2G','3G','4G','5G']
for i in TEC_List:
  MS(i)






pathToImportMERGE = script_dir + '/export/MS'
pathToSave = script_dir + '/export/'+timeexport+'_MERGED'
MERGE = ImportDF.ImportDF2(pathToImportMERGE)
cols_to_convert = ['DISP','VOLUME','TRAFEGO','VOLUME(sum)','TRAFEGO(sum)','DISP(mean)','VOLUME(median)','TRAFEGO(median)','DISP(median)','VOLUME%','TRAFEGO%']
for col in cols_to_convert:
    MERGE[col] = MERGE[col].astype(float)
print(MERGE.dtypes)
MERGE.to_csv(pathToSave +'.csv',index=False,header=True,sep=';',decimal=',') #decimal to works need type as number


comparePMO = MERGE.loc[(~MERGE['VERIFICAR'].isna())&(MERGE['DropAnalise'].isna())&(MERGE['TEC'] !='2G')]
KeepListCompared = ['Regional','ANF','Municipio','IBGE','Classificacao','YearWeek','Station ID','RAN Node','TEC','DropAnalise','TRAFEGO','ref2','VOLUME(sum)','TRAFEGO(sum)','DISP(mean)','VOLUME(median)','TRAFEGO(median)','DISP(median)','VOLUME%','TRAFEGO%','VERIFICAR']
locationBase_comparePMO = list(comparePMO.columns)
DellListComparede = list(set(locationBase_comparePMO)^set(KeepListCompared))
comparePMO = comparePMO.drop(DellListComparede,1)
comparePMO = comparePMO.drop_duplicates()
comparePMO = comparePMO.reset_index(drop=True)
comparePMO.to_csv(pathToSave +'_Consolidated.csv',index=False,header=True,sep=';',decimal=',') #decimal to works need type as number




