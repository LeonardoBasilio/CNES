
#!/usr/bin/env python
# coding: utf-8

# ## CNES_Utils
# 
# 
# 

# ## Imports

# In[1]:


import zipfile as z
import os
import requests
import json
import re

# usado nos notebooks filhos (Servidores)
from pyspark.sql.functions import lit, col, regexp_replace
from datetime import datetime, timedelta


# ### Criando pasta tmp para download

# In[2]:


def Download(origem,destino):
   data= requests.get(origem)
   with open(destino , 'wb') as file:
       file.write(data.content)


# ### Obter arquivos zips da url

# In[3]:


def ObterArquivosCNES(url, htmlSTR):
   # lógica para pegar os nomes que tem .zip no final
   urls = []
   _filter = r'<script>(.*?)</script>'
   scripts = re.findall(_filter, htmlSTR, re.DOTALL)    
   resultado = ([texto for texto in scripts if "var arquivos = []" in texto])[0]

   _filter = r'arquivos.push\((.*?)\);'

   resultado = re.findall(_filter, resultado)
  
   objetos = []
   for t in resultado:
       obj = json.loads(t)
       objetos.append(obj)
   
   objetos = [obj for obj in objetos if "_CNES" in obj['origem']]
   
   maiorAno = max(objetos, key=lambda x: (int(x['ano']), int(x['mes'])))['ano']
   maiorMes = max(objetos, key=lambda x: (int(x['ano']), int(x['mes'])))['mes']
   # print(maiorAno)
   # print(maiorMes)

   # criar uma nova lista contendo apenas os objetos com o maior ano e o maior mês
   objetos = [obj for obj in objetos if obj['ano'] == maiorAno and obj['mes'] == maiorMes]
   
   for obj in objetos:
       urls.append(url + obj['ano'] + obj['mes'] + "_" + obj['origem'])
       anoMes = obj['ano'] + obj['mes'] 
   return  (urls, anoMes )


# ### Download dos arquivos zips

# In[4]:


def MoverArquivosParaHistorico(datalakeDestino):

   pathHistorico = datalakeDestino + '/Historico'
   folders = mssparkutils.fs.ls(datalakeDestino)
   for folder in folders:

       if(folder.name != 'Historico'):
           pathComFolder = pathHistorico + '/' + folder.name
           mssparkutils.fs.mv(folder.path ,pathComFolder, create_path=True, overwrite=True )


def SalvarZipURLPortalDaTransparencia(url,tempDiretorio,datalakeDestino):

   # realiza o arquivamento dos arquivos
   #Se primeira carga comentar a chamada
   #MoverArquivosParaHistorico(datalakeDestino)

   htmlSTR = libFuncoesETL.GetResponseBody(url)
   print(htmlSTR)
   nameList = []

   # arquivos:
   (itens, anoMes) = ObterArquivosCNES(url, htmlSTR)
   # print(itens)

   for i in range(len(itens)):
       # removendo URL's Invalidas      
       if (len(itens[i].split(">")) > 1) :
           continue
           
       nomeArquivoZip = itens[i].split("/")[-1]
       nomeArquivoList = nomeArquivoZip.split("_")
       nomeArquivo = (nomeArquivoList)[1] + "_" + (nomeArquivoList)[2]

       directoryZip = tempDiretorio + nomeArquivo + "/ZIP/" + (nomeArquivoList)[0]
       directoryCSV = tempDiretorio + nomeArquivo + "/CSV/" + (nomeArquivoList)[0]
       os.makedirs(directoryZip, exist_ok=True)
       os.makedirs(directoryCSV, exist_ok=True)

       # print(directory)
       # print(nomeArquivo)
       Download(itens[i],directoryZip + nomeArquivo + ".zip")
       root = z.ZipFile(directoryZip + nomeArquivo + ".zip")
       root.extractall(directoryCSV)
       root.close()  
       
       mssparkutils.fs.cp(
         src='file:' + directoryCSV
       , dest=datalakeDestino + nomeArquivo + '/'
       , recurse=True
       )

       nameList.append(nomeArquivo)

   return anoMes   


# ## APAGAR

# In[5]:


get_ipython().run_line_magic('run', '/Config_Utils')


# In[6]:


get_ipython().run_line_magic('run', '/ETL_Utils')


# In[7]:


# obter a data e hora atuais
now = datetime.now() - timedelta(hours=3)

# formatar a data e hora no formato desejado
dataHora = now.strftime('%Y/%m/%d/%H/%M')


# In[14]:


#impoortar
url = 'https://cnes.datasus.gov.br/EstatisticasServlet?path=BASE_DE_DADOS_CNES_202508.ZIP'
#diretorioTemp = '/tmp/EstatisticasServlet/'
pathCSV = libConfig.camadasPath['stage'] + '/URL/CNES/Estabelecimentos/'


# In[9]:


#anoMes = SalvarZipURLPortalDaTransparencia(url, diretorioTemp, pathCSV)


# In[17]:


def GetResponseBody(dados):
   """
   Obtém e retorna o corpo HTML de uma página web

   Parâmetros
   ----------
   dados : str
       A URL ou o caminho dos dados a serem lidos

   Retorna
   -------
   str
       Uma string que representa o conteúdo HTML da página
   """
   
   controle = False
   print("inicio leitura html")
   

   while(controle == False):
       try:
           rawHTML = ETL.request.urlopen(dados, timeout=30).read()
           # ETL.request.urlopen(url, timeout=20) -> retorna o html da página para a leitura dos nomes dos arquivos
           # timeout : limite em segundos para bloquear a operação
           controle = True
       except:
           pass

   pageItems = ETL.bs.BeautifulSoup(rawHTML, 'html.parser')  
   # formata para um objeto BeautifulSoup que representa um documento com uma estrutura de dados aninhada
   
   htmlSTR = str(pageItems)
   # passa o objeto BeautifulSoup para string
   


# In[18]:


htmlSTR = GetResponseBody(url)


# In[11]:


pathBR = libConfig.camadasPath['bronze'] + "/URL/CNES/Estabelecimentos/"
pathSTG = libConfig.camadasPath['stage'] + "/URL/CNES/Estabelecimentos/"
nameTable = "Estabelecimentos"


# In[12]:


namePaths = mssparkutils.fs.ls(pathSTG)
# print(namePaths)

dfs = []
for namePath in namePaths:

   if(namePath.name == 'Historico'):
       print('Pasta de Historico')
       continue
   
   pathTabela = mssparkutils.fs.ls(namePath.path)
   
   for fileINFO in pathTabela:
       data = fileINFO.name.split('_')[0]
       name = fileINFO.name.split('_')[1]

       if((name.upper()).startswith((nameTable).upper())):            
           print(name)
           df = spark.read.format("csv")\
           .option("header", "true")\
           .option("encoding", "UTF-8")\
           .option("delimiter", ";")\
           .option("encoding", "latin1")\
           .option("inferSchema", "false")\
           .load(fileINFO.path)
           
           df = df.withColumn(namePath.name, lit(True)) # adicionando a nova coluna
           df = df.withColumn('dataImportacao', lit(data))
           
           dfs.append(df)

# unir dataframes
df1 = dfs[0]
for i in range(1, len(dfs)):
   df2 = dfs[i]

   # obtendo o número de colunas de cada dataframe
   numColsDF1 = len(df1.columns)
   numColsDF2 = len(df2.columns)

   # verificar se todas as colunas da df1 estão na df2
   missingColsDF2 = [col for col in df1.columns if col not in df2.columns]

   for col in missingColsDF2:
       colType = df1.schema[col].dataType
       df2 = df2.withColumn(col, lit(None).cast(colType))

   # verificar se todas as colunas da df2 estão na df1
   missingColsDF1 = [col for col in df2.columns if col not in df1.columns]
   for col in missingColsDF1:
       colType = df2.schema[col].dataType
       df1 = df1.withColumn(col, lit(None).cast(colType))

   
   # unir dataframes
   df1 = df1.unionByName(df2)

dfFinal = df1


#