import configparser
import csv
import datetime
import json
import os
import pickle
import shutil
import sys
import time
import urllib.parse
from multiprocessing.dummy import Pool as ThreadPool
from pprint import pprint
import itertools
import requests
import urllib3

import psycopg2
import psycopg2.extras

if len(sys.argv) < 2:
    exit("Usage:python3 FBAdScrapeScript.py crawl_config.cfg")


config = configparser.ConfigParser()
config.read(sys.argv[1])

HOST = config['POSTGRES']['HOST']
DBNAME = config['POSTGRES']['DBNAME']
USER = config['POSTGRES']['USER']
PASSWORD = config['POSTGRES']['PASSWORD']
DBAuthorize = "host=%s dbname=%s user=%s password=%s" % (HOST, DBNAME, USER, PASSWORD)


Email = config['ACCOUNT']['EMAIL']
Password = config['ACCOUNT']['PASS']

parameters_for_URL = {
    "__user":config['COOKIES']['USERFIELD'],
    "__a":config['COOKIES']['AFIELD'],
    "__dyn":config['COOKIES']['DYNFIELD'],
}

MasterSeedList = config['SEEDLIST']['MASTERSEEDFILE']

URLparameters = urllib.parse.urlencode(parameters_for_URL)

prefix_length = len("for (;;);")

now = datetime.datetime.now()
now_str = "".join(str(e) for e in [now.year, now.month, now.day, now.hour])
StartTimeStamp = 'NEWcrawl_'+ now_str # Adding NEW so DB parser doesn't try to parse this until it's complete.

LatestTimestampRecorded = 0

adMetadataLinkTemplate = "https://www.facebook.com/politicalcontentads/ads/?q=%s&count=%s&active_status=all&dpr=1&%s"

# The above link takes in q = Seed word, count = # of ads to be shown and we append the parameters at the end of the link
# The request response contains the total # of ads for a given seed under "totalCount".

adMetadataLinkNextPageTemplate = "https://www.facebook.com/politicalcontentads/ads/?q=%s&page_token=%s&count=%s&active_status=all&dpr=1&%s"
# The above link has additional parameter for the page_token that is retrieved when the inital call is made to get the metadata.

adPerformanceDetails = "https://www.facebook.com/politicalcontentads/insights/?ad_archive_id=%s&%s"


def ExtractLastTimestampExtracted():
    """
    Reads through all the crawl folders to extract the most recent crawl.
    """
    global LatestTimestampRecorded
    Max = 0
    for FolderName in os.listdir('.'):
        if FolderName != StartTimeStamp and os.path.isdir(FolderName) and \
                FolderName.startswith('crawl_'): # All folders with Crawl information are stored as crawl_XXXXX
            CrawlDate = int(FolderName[len('crawl_'):])
            if CrawlDate > LatestTimestampRecorded:
                LatestTimestampRecorded = CrawlDate





def MigrateFilesProperDirectory(src, dst):
    """
    Crawl folder is named NEWcrawl_XXX while the crawl is going on so the DB insertion script 
    doesn't interfere with the current crawl folder. Once the crawl is complete the content of the 
    folder is moved to the correct folder name and the temp folder is deleted.
    """
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d)
        else:
            shutil.copy2(s, d)
    shutil.rmtree(src)





def ScrapeAdMetadataByKeyword(CurrentSession, Seed, NumAds = 2000):
    """
    Returns a list of dictionaries that includes metadata of the Ad and 
    also includes it's performance details. Our program crawls 5000 
    ads in the first iteration and then 500 ads in the subsequent
    iterations. This is done to optimize the script and work around
    FB's load balancing.
    """
    
    AllAdMetadata = []
    totalAdCountCurrent = 0
    IterationCount = 1
    URLparameters = urllib.parse.urlencode(parameters_for_URL)
    AdMetadataLink = adMetadataLinkTemplate % (Seed, NumAds, URLparameters) # Scapes 2000 ads
    data = CurrentSession.get(AdMetadataLink)
    DataRetrievedFromLink = data.text[prefix_length:] 
    DataRetrievedFromLinkJson = json.loads(DataRetrievedFromLink)
    AllAdMetadata.append(DataRetrievedFromLinkJson)
    totalAdCount = DataRetrievedFromLinkJson['payload']['totalCount']
    totalAdCountCurrent += len(DataRetrievedFromLinkJson['payload']['results'])
    while not DataRetrievedFromLinkJson['payload']['isResultComplete'] and totalAdCountCurrent < 8000:
        # Limit ad collection to 8000 since FB kills connection after that.
        # WIP to work around the 8K ad limit.  
        time.sleep(3)
        IterationCount += 1
        nextPageToken = DataRetrievedFromLinkJson["payload"]["nextPageToken"]
        nextPageToken = urllib.parse.quote(nextPageToken)

        DataRetrievedFromLink = ""
        DataRetrievedFromLinkJson = {}
        adMetadataLinkNextPage = \
                adMetadataLinkNextPageTemplate % (Seed, nextPageToken, 2000, URLparameters)
        for attempts in range(5):
            try:
                data = CurrentSession.get(adMetadataLinkNextPage)
                DataRetrievedFromLink = data.text[prefix_length:] 
                DataRetrievedFromLinkJson = json.loads(DataRetrievedFromLink)
                AllAdMetadata.append(DataRetrievedFromLinkJson)
                totalAdCount = DataRetrievedFromLinkJson['payload']['totalCount']
                totalAdCountCurrent += len(DataRetrievedFromLinkJson['payload']['results'])
                time.sleep(1)
                break
            except:
                if attempts == 4:
                    totalAdCountCurrent = 8000
                    break
                print("Trying again")
                time.sleep(3)

    WriteToFiles(AllAdMetadata, "Metadata", Seed) #List of dictionaries returned
    return AllAdMetadata





def AddLatestAds(AdsDataJson, Seed):
    """
    Smart scraping that only scrapes ads that we haven't scraped before.
    """
    with open(MasterSeedList) as f:
        AllSeeds = set([Seed.strip() for Seed in f.readlines() if Seed.strip() != ""])
    NewResults = []
    if Seed.strip() not in AllSeeds:
        return
    for ad in AdsDataJson['payload']['results']:
        if int(ad['startDate']) > LatestTimestampRecorded:
            NewResults.append(ad)
    AdsDataJson['payload']['results'] = NewResults[:]
    return





def WriteToFiles(Payload, TypeOfPayload, Seed):
    """
    Writes to files. 
    Payload is the dictionary being written.
    Type indicates whether it is Contents of the ad or Metadata.
    Seed refers to the Search Keyword.
    Iteration Count refers to the # of file that will be written. 
        Since each file contains over 2000 entries of content/metadata
        at most. We will need multiple files to store all the data.
    """
    if len(Seed.split()) > 1:
            Seed = "".join(Seed.split(" "))

    if not os.path.exists(os.path.join(StartTimeStamp, Seed)):
        os.makedirs(os.path.join(StartTimeStamp, Seed))

    Path = os.path.join(StartTimeStamp, Seed, TypeOfPayload)

    with open(Path + ".txt", 'w') as f:
        json.dump(Payload, f)





def ScrapeAdIDs(AllAdsMetadata, IDsDB):
    """
    Iterates over the dictionaries in the list and extracts
    the AdArchiveIDs and stores them in chunk for retrieving
    the AdContents.
    """
    adIDsChunk = [] 
    AllAdIDs = []
    Chunk = 500
    for AdIDChunk in AllAdsMetadata:
        for ad in AdIDChunk['payload']['results']:
            if ad["adArchiveID"] not in IDsDB:
                AllAdIDs.append(ad["adArchiveID"])
    print("Total AdIDs ", len(AllAdIDs))
    Start = 0
    End = Chunk
    Loop = True
    while Loop:
        yield AllAdIDs[Start:End]
        if End < len(AllAdIDs):
            Loop = False
        Start += Chunk
        End += Chunk
        time.sleep(2)





def ScrapePerformanceDetails(CurrentSession, AdID):
    """
    Access the performance information per ad using AJAX call.
    """
    time.sleep(0.5)
    AdPerformance = []
    PerformanceDetials = adPerformanceDetails % (AdID, URLparameters)
    data = CurrentSession.get(PerformanceDetials).text
    print("Data: ", data)
    print("ADID: ", AdID)
    #DataRetrievedFromLink = data.text[prefix_length:] 
    #DataRetrievedFromLinkJson = json.loads(data)
    return data





def ScrapePerformanceDetailsThreadHelper(AllAdsMetadata, CurrentSession, IDsDB):
    
    for adIDs in ScrapeAdIDs(AllAdsMetadata, IDsDB):
        pool = ThreadPool(3)
        results = pool.starmap(ScrapePerformanceDetails, zip(itertools.repeat(CurrentSession), adIDs))
        print(results)
        pool.close()
        pool.join()
        



def extractSeedWords(SeedListName=MasterSeedList):
    """
    Extracts the seeds and removes redundant seed 
    that have already been accessed.
    """
    if SeedListName == MasterSeedList:
        with open(MasterSeedList) as f:
            Seeds = set([SeedWords.strip() for SeedWords in f.readlines() if SeedWords != " "])
            return Seeds

    if SeedListName[-4:] == '.txt':
        return extractSeedWordsTXT(SeedListName)
    elif SeedListName[-4:] == '.csv':
        return extractSeedWordsCSV(SeedListName)





def extractSeedWordsCSV(SeedListName, FirstName = False, LastName = True):
    """
    Names of Political Candidates in the CSV format. 
    The default parameters allow us to choose whether we want to get first names 
    or last names.
    """
    with open(SeedListName, 'r') as f, open(MasterSeedList, "a+") as f1:
        if FirstName and LastName:
            CurrentSeeds = set([' '.join(seedWord).strip() for seedWord in csv.reader(f) if seedWord != " "])
        elif LastName and not FirstName:
            CurrentSeeds = set([seedWord[1] for seedWord in csv.reader(f) if seedWord != " "])
        MasterSeeds = set([seedWord.strip() for seedWord in f1.readlines() if seedWord != " "])
        NewSeeds = list(CurrentSeeds-MasterSeeds)
        for Seed in NewSeeds:
            f1.write(Seed+'\n')
    return NewSeeds
    




def extractSeedWordsTXT(SeedListName):
    with open(SeedListName, 'r') as f, open(MasterSeedList, "a+") as f1:
        CurrentSeeds = set([seedWord.strip() for seedWord in f.readlines() if seedWord != " "])
        MasterSeeds = set([seedWord.strip() for seedWord in f1.readlines() if seedWord != " "])
        NewSeeds = list(CurrentSeeds-MasterSeeds)
        for Seed in NewSeeds:
            f1.write(Seed+'\n')
    
    return NewSeeds





def dedupMasterSeeds():
    """ 
    Master seed list has duplicates. Temporary fix to dedup the master seed list.
    """
    with open(MasterSeedList) as f:
        MasterSeeds = set([seedWord.strip() for seedWord in f.readlines() if seedWord != " "])
        
    with open(MasterSeedList, 'w') as f:    
        for Seed in MasterSeeds:
            f.write(Seed+'\n')





if __name__ == "__main__":
    ExtractLastTimestampExtracted()
    IterationCount = 0
    SeedCount = 0
    connection = psycopg2.connect(DBAuthorize)
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("SELECT distinct archive_id from ads")
    IDsDB = cursor.fetchall()
    print(IDsDB)
    Start = time.time()
    with requests.Session() as currentSession:
        data = {"email":config['ACCOUNT']['EMAIL'], "pass":config['ACCOUNT']['PASS']}
        post = currentSession.post("https://www.facebook.com/login", data)
        post = currentSession.post("https://www.facebook.com/login", data)
        if config['SEEDLIST']['SEEDFILE'] != 'XXX':
            Seeds = extractSeedWords(config['SEEDLIST']['SEEDFILE'])
        else:
            Seeds = extractSeedWords() # Routine scrape of all Seeds previously scraped.
        TotalSeeds = len(Seeds)
        print("Seeds: \n", Seeds)
        #exit()
        if not os.path.exists(StartTimeStamp):
            os.makedirs(StartTimeStamp)
        with open(os.path.join(StartTimeStamp, "Keywords.txt"), 'a+') as f:
            for Seed in Seeds:
                if Seed.strip() == "":
                    continue
                print("\nWorking on ", Seed, "\n")
                SeedCount += 1
                print("Seed %d out of %d\n" % (SeedCount, TotalSeeds))
                SkipKeyword = False
                for attempts in range(5):
                    try:        
                        AllAdsMetadata = ScrapeAdMetadataByKeyword(currentSession, Seed)
                        break
                    except:
                        if attempts == 4:
                             SkipKeyword = True
                             break
                        time.sleep(10)
                print("Done with metadata")

                ScrapePerformanceDetailsThreadHelper(AllAdsMetadata, currentSession, IDsDB) 
                

                # for attempts in range(5):
                #     try:        
                #         ScrapeAdsByAdIDs(currentSession, adIDs, Seed)
                #         break
                #     except:
                #         if attempts == 4:
                #              SkipKeyword = True
                #              break
                #         time.sleep(10)
                if not SkipKeyword:
                    f.write(Seed.strip() + '\n')
    #os.rename(StartTimeStamp, StartTimeStamp[3:]) #To remove NEW prefix
    print("EndTime: ", time.time() - Start)
    #dedupMasterSeeds()
