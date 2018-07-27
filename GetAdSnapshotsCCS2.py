import configparser
import datetime
import os
import json
import sys
import urllib.parse
import time
import random
import requests

import psycopg2
import psycopg2.extras

config = configparser.ConfigParser()
config.read(sys.argv[1])

parameters_for_URL = {
    "__user":config['COOKIES']['USERFIELD2'],
    "__a":config['COOKIES']['AFIELD2'],
    "__dyn":config['COOKIES']['DYNFIELD2'],
}

prefix_length = len("for (;;);")

URLparameters = urllib.parse.urlencode(parameters_for_URL)

HOST = config['POSTGRES']['HOST']
DBNAME = config['POSTGRES']['DBNAME']
USER = config['POSTGRES']['USER']
PASSWORD = config['POSTGRES']['PASSWORD']
MINWAITERROR = int(config['WAIT']['MINERROR'])
MAXWAITERROR = int(config['WAIT']['MAXERROR'])
MINWAITITER = int(config['WAIT']['MINITER'])
MAXWAITITER = int(config['WAIT']['MAXITER'])
CLUSTERSIZE = int(config['PARTITION']['CLUSTERSIZE'])
BOXNUMBER = int(config['PARTITION']['BOXNUMBER'])

adPerformanceDetails = "https://www.facebook.com/politicalcontentads/insights/?ad_archive_id=%s&%s"

DBAuthorize = "host=%s dbname=%s user=%s password=%s" % (HOST, DBNAME, USER, PASSWORD)


now = datetime.datetime.now()
now_str = "".join(str(e) for e in [now.year, now.month, now.day, now.hour])
WriteDir = 'NEWcrawl_' + now_str + "Metadata" # Adding NEW so DB parser doesn't try to parse this until it's complete.

print("Writing to directory: ", WriteDir)



def ScrapePerformanceDetailsSeq(adIDs, CurrentSession):
  Start = time.time()
  AdPerformance = []
  Count = 0
  for AdID in adIDs:
    Count += 1
    PerformanceDetials = adPerformanceDetails % (AdID, URLparameters)
    data = CurrentSession.get(PerformanceDetials)
    if data:
      DataRetrievedFromLink = data.text[prefix_length:] 
      DataRetrievedFromLinkJson = json.loads(DataRetrievedFromLink)
      if "error" in DataRetrievedFromLinkJson:
        print(DataRetrievedFromLinkJson)
        print(Count)
        print("AdIDArchive : ", AdID)
        if DataRetrievedFromLinkJson['error'] == 2334010:
          time.sleep(random.randint(MINWAITERROR, MAXWAITERROR))
        #time.sleep(random.uniform(1,2))
        data = CurrentSession.get(PerformanceDetials)
        if data:
          DataRetrievedFromLink = data.text[prefix_length:] 
          DataRetrievedFromLinkJson = json.loads(DataRetrievedFromLink)
    else:
      print(Count)
      print("AdIDArchive : ", AdID)
    time.sleep(random.uniform(MINWAITITER,MAXWAITITER))
    DataRetrievedFromLinkJson['ad_archive_id'] = int(AdID)
    AdPerformance.append(DataRetrievedFromLinkJson)
    if len(AdPerformance) % 2000 == 0:
      WriteToFiles(AdPerformance, "Metadata")
  print(time.time() - Start)
  return AdPerformance





def SampleAdIDs(IDs):
    """
    Samples 200 random IDs to benchmark.
    """
    AllAdIDs = []
    IDsToTest = set()
    print(len(IDs))
    while len(IDsToTest) < 200:
      IDsToTest.add(random.randrange(0, len(IDs)))
    
    return [IDs[i] for i in IDsToTest]




def GetAdArchiveIDDB():
  IDs = []
  connection = psycopg2.connect(DBAuthorize)
  cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
  Query = """
      SELECT distinct ad_archive_id 
      from snapshots
      WHERE is_active=true and ad_archive_id % """ + str(CLUSTERSIZE) + """ 
      = """ + str(BOXNUMBER) 
  cursor.execute(Query)
  for ID in cursor.fetchall():
    IDs.append(ID[0])
  cursor.close()
  return IDs





def WriteToFiles(Payload, TypeOfPayload):
  """
  Writes to files. 
  Payload is the dictionary being written.
  Type indicates whether it is Contents of the ad or Metadata.
  Seed refers to the Search Keyword.
  Iteration Count refers to the # of file that will be written. 
    Since each file contains over 2000 entries of content/metadata
    at most. We will need multiple files to store all the data.
  """
  if not os.path.exists(WriteDir):
    os.makedirs(WriteDir)

  Path = os.path.join(WriteDir, TypeOfPayload)

  with open(Path + ".txt", 'w') as f:
    json.dump(Payload, f)




if __name__ == "__main__":
  IDs = GetAdArchiveIDDB()
  random.shuffle(IDs)
  Start = time.time()
  with requests.Session() as currentSession:
    data = {"email":config['ACCOUNT']['EMAIL2'], "pass":config['ACCOUNT']['PASS2']}
    post = currentSession.post("https://www.facebook.com/login", data)
    post = currentSession.post("https://www.facebook.com/login", data)
    #AdIDs = SampleAdIDs(IDs)
    Data = ScrapePerformanceDetailsSeq(IDs, currentSession)
    WriteToFiles(Data, "Metadata")
  print(time.time() - Start)
  os.rename(WriteDir, WriteDir[3:])

