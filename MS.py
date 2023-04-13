import os
import sys
import timeit
import ImportDF
import pandas as pd
import inspect
import locale
from locale import atof
locale.setlocale(locale.LC_NUMERIC, '')



def MS(TEC):
  inicio = timeit.default_timer()
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
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
                  'VOLUME_DADOS_DLUL_ALLOP 2G': 'Volume',
                  'TRAFEGO_VOZ_ALLOP 2G': 'TRAFEGO',
                  'DISP_COUNTER_TOTAL 3G (com filtro OPER)':'DISP',
                  'VOLUME_DADOS_DLUL_ALLOP 3G - Mbyte': 'Volume',
                  'TRAFEGO_VOZ_TIM 3G':'TRAFEGO',
                  'DISP_COUNTER_TOTAL 4G (com filtro OPER)': 'DISP',
                  'VOLUME_DADOS_DLUL_TIM 4G - Gbyte': 'Volume',
                  'TRAFEGO_VOZ_TIM 4G':'TRAFEGO',
                  'DISP_COMB_TOTAL 5G (com filtro OPER)': 'DISP',
                  'VOLUME_TOTAL_DLUL_TIM 5G - Gbyte':'Volume'}

  for key, value in renameColuns.items():
    try:
      Frame.rename(columns={key:value},inplace=True)
    except:
      pass
  Frame['DISP'] = Frame['DISP'].str.strip('()%')
  #Frame['revenue'] = Frame.get('revenue', Frame['Volume'] * Frame['TRAFEGO'])
  Frame['TRAFEGO'] = Frame.get('TRAFEGO', 0) # create a column if not exist
  Frame['Volume'] = Frame['Volume'].astype(str).apply(locale.atof)
  Frame['TRAFEGO'] = Frame['TRAFEGO'].astype(str).apply(locale.atof)
  Frame['DISP'] = Frame['DISP'].astype(str).apply(locale.atof)
  Frame['DISP'] = (Frame['DISP']/100).round(2)
  Frame['YearWeek'] =  + Frame['YearWeek'].str[-4:] + Frame['YearWeek'].str[:3]    
  Frame.sort_values(by=['YearWeek','RAN Node','Celula'], ascending=False,inplace=True)

  #Por enquanto sem normalização dos dados
  '''
  CoversionVolume = {'2G':2048,'3G':1024,'4G':1,'5G':1}
  
  for key, value in CoversionVolume.items():
    Frame.loc[Frame['TEC'] == key,['Volume3']] = (Frame['Volume2']/value).round(2)
  '''


  return Frame

TEC_List = ['2G','3G','4G','5G']
for i in TEC_List:
  MS(i)


