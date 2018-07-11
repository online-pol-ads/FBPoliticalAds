import urllib3
import urllib.parse
import requests 
from pprint import pprint
import json
import pickle
import time
import shutil
import os
import sys
import csv

# TODO Fix ASCII
# TODO OPtimize the script for 2k searches


login = "https://www.facebook.com/politicalcontentads"

adMetadataLinkTemplate = \
        "https://www.facebook.com/politicalcontentads/ads/?q=%s&count=%s&active_status=all&dpr=1&%s" 

adMetadataLinkNextPageTemplate = \
        "https://www.facebook.com/politicalcontentads/ads/?q=clinton&page_token=%s&count=%s&active_status=all&dpr=1&%s"

adContentLinkTemplate = "https://www.facebook.com/ads/political_ad_archive/creative_snapshot/?%s&dpr=1&%s"
# The above link takes in q = Seed word, count = # of ads to be shown and we append the parameters at the end of the link
# The request response contains the total # of ads for a given seed under "totalCount".
test_seed = "test"

http = urllib3.PoolManager()
prefix_length = len("for (;;);")

parameters_for_URL = {
    "__user":"100026761269396",
    "__a":"1",
    "__dyn":"7xe6Fo4OQ5E5Obx679uC1swgE98nwgU6C7UW3K2K7E5G3mewXx61rwaS12x60Vo7W0-FHwiE3awExK1RxO2u0IobEa8465oOfwjU3jwjbAyE",
}

URLparameters = urllib.parse.urlencode(parameters_for_URL)

StartTimeStamp = 'NEWcrawl_'+str(int(time.time())) # Adding NEW so DB parser doesn't try to parse this until it's complete.

MasterSeedList = "MasterSeedList.txt"

LatestTimestampRecorded = 0





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
    #print("Latest time registered: ", LatestTimestampRecorded)





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





def ScrapeAdMetadataByKeyword(CurrentSession, Seed, NumAds = 5000):
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
    AdMetadataLink = adMetadataLinkTemplate % (Seed, NumAds, URLparameters) # Scapes 5000 ads
    data = CurrentSession.get(AdMetadataLink)
    DataRetrievedFromLink = data.text[prefix_length:] 
    DataRetrievedFromLinkJson = json.loads(DataRetrievedFromLink)
    AllAdMetadata.append(DataRetrievedFromLinkJson)
    WriteToFiles(DataRetrievedFromLinkJson, "Metadata", Seed)
    totalAdCount = DataRetrievedFromLinkJson['payload']['totalCount']
    totalAdCountCurrent += len(DataRetrievedFromLinkJson['payload']['results'])
    time.sleep(5)

    while not DataRetrievedFromLinkJson['payload']['isResultComplete']:
        # Scrapes 500 ads till FB returns more ads. 
        IterationCount += 1
        nextPageToken = DataRetrievedFromLinkJson["payload"]["nextPageToken"]
        nextPageToken = urllib.parse.quote(nextPageToken)

        DataRetrievedFromLink = ""
        DataRetrievedFromLinkJson = {}
        adMetadataLinkNextPage = \
                adMetadataLinkNextPageTemplate % (nextPageToken, 500, URLparameters)

        data = CurrentSession.get(adMetadataLinkNextPage)
        DataRetrievedFromLink = data.text[prefix_length:] 
        DataRetrievedFromLinkJson = json.loads(DataRetrievedFromLink)

        AllAdMetadata.append(DataRetrievedFromLinkJson)
        totalAdCountCurrent += len(DataRetrievedFromLinkJson['payload']['results'])
        #pprint(DataRetrievedFromLinkJson)
        #print("\n\n")
        time.sleep(5)
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
        #print("Not in master seed list\n\n\n")
        return
    for ad in AdsDataJson['payload']['results']:
        if int(ad['startDate']) > LatestTimestampRecorded:
            #print("Adding new ad.")
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





def ScrapeAdIDs(AllAdsMetadata):
    """
    Iterates over the dictionaries in the list and extracts
    the AdArchiveIDs and stores them in chunk for retrieving
    the AdContents.
    """
    adIDsChunk = [] 
    AllAdIDs = []
    ChunkSize = 500

    for AdIDChunk in AllAdsMetadata:
        for ad in AdIDChunk['payload']['results']:
            adIDsChunk.append(ad["adArchiveID"])
            if len(adIDsChunk) == ChunkSize:
                AllAdIDs.append(adIDsChunk)
                adIDsChunk = []
    AllAdIDs.append(adIDsChunk)
    return AllAdIDs





def ScrapeAdsByAdIDs(currentSession, adIDs, Seed):
    """
    Retrieves Ad contents for the given adIDs by stringing them
    together in the form used in FB URLs. 
    adIDs is a list of lists containing 500 adIDs each. 
    The function makes requests for 500 ads in every iteration (for 
    optimization and load balancing) and appends the content retrieved 
    (JSON form) to a list. 
    """
    allAdsContents = []
    adIDQueryList = []
    for adIDList in adIDs:
        for adID in range(0, len(adIDList)):
            adIDQueryList.append("ids[" + str(adID) + "]=" + adIDList[adID])
        adIDQuery = "&".join(adIDQueryList)
        adIDQueryList = []
        #print(adIDQuery)
        adContentLink = adContentLinkTemplate % (adIDQuery, URLparameters)
        adContentRetrieved = currentSession.get(adContentLink)
        adContentRetrieved = adContentRetrieved.text[prefix_length:]
        
        #print(Seed)
        #print('\n\n\n\n')
        #pprint(adContentRetrieved)
        adContentRetrievedJson = json.loads(adContentRetrieved)
        time.sleep(3)
        allAdsContents.append(adContentRetrievedJson)    
    WriteToFiles(allAdsContents, "Contents", Seed)





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
            #print(CurrentSeeds)
            #exit()
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





if __name__ == "__main__":
    ExtractLastTimestampExtracted()
    IterationCount = 0
    SeedCount = 0
    with requests.Session() as currentSession:
        data = {"email":Email, "pass":Password}
        post = currentSession.post("https://www.facebook.com/login", data)
        post = currentSession.post("https://www.facebook.com/login", data)
        if len(sys.argv)>1:
            Seeds = extractSeedWords(sys.argv[1])
        else:
            Seeds = extractSeedWords() # Routine scrape of all Seeds previously scraped.
        print(Seeds)
        TotalSeeds = len(Seeds)
        #exit()
        if not os.path.exists(StartTimeStamp):
            os.makedirs(StartTimeStamp)
        with open(os.path.join(StartTimeStamp, "Keywords.txt"), 'a+') as f:

            for Seed in Seeds:
                if Seed.strip() == "":
                    continue
                print("Working on ", Seed, "\n")
                SeedCount += 1
                print("Seed %d out of %d\n" % (SeedCount, TotalSeeds))
                SkipKeyword = False
                for attempts in range(5):
                    try:
                        AllAdsMetadata = ScrapeAdMetadataByKeyword(currentSession, Seed)
                        break
                        # FB cuts off some connections randomly and doesn't return any data. 
                        # to work around presumable load balancing of FB servers, every
                        # keyword will be tried 5 times if FB resets connection with a time interval of 20secs.
                    except: 
                        if attempts == 4:
                            SkipKeyword = True
                            break
                        time.sleep(20)

                time.sleep(10)
                adIDs = ScrapeAdIDs(AllAdsMetadata)

                for attempts in range(5):
                    try:        
                        ScrapeAdsByAdIDs(currentSession, adIDs, Seed)
                        break
                    except:
                        if attempts == 4:
                             SkipKeyword = True
                             break
                        time.sleep(20)

                if not SkipKeyword:
                    f.write(Seed.strip() + '\n')
    #time.sleep(60)
    MigrateFilesProperDirectory(StartTimeStamp, StartTimeStamp[3:]) #To remove NEW prefix
