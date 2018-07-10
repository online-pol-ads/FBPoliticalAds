from collections import namedtuple
from pathlib import Path
import sys
import os
import configparser
import psycopg2
import psycopg2.extras
import datetime
import time
import json

if len(sys.argv) < 2:
    exit("Usage:python3 import_ads_to_db.py import_ads_to_db.cfg")

config = configparser.ConfigParser()
config.read(sys.argv[1])

crawl_date = datetime.date.today() - datetime.timedelta(days=10) #LAE - remove the minus

HOST = config['POSTGRES']['HOST']
DBNAME = config['POSTGRES']['DBNAME']
USER = config['POSTGRES']['USER']
PASSWORD = config['POSTGRES']['PASSWORD']
DBAuthorize = "host=%s dbname=%s user=%s password=%s" % (HOST, DBNAME, USER, PASSWORD)
connection = psycopg2.connect(DBAuthorize)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

#cache ad and related tables so we minimize inserts
ads_query = "select id from ads"
cursor.execute(ads_query)
ad_ids = set()
for row in cursor:
    ad_ids.add(row['id'])

ad_sponsor_query = "select name, nyu_id from ad_sponsors"
cursor.execute(ad_sponsor_query)
ad_sponsor_ids = {}
for row in cursor:
    ad_sponsor_ids[row['name']] = row['nyu_id']

page_query = "select id from pages"
cursor.execute(page_query)
page_ids = set()
for row in cursor:
    page_ids.add(row['id'])

print(page_ids)

category_query = "select id from categories"
cursor.execute(category_query)
category_ids = set()
for row in cursor:
    category_ids.add(row['id'])

region_query = "select name, nyu_id from regions"
cursor.execute(region_query)
regions = {}
for row in cursor:
    regions[row['name']] = row['nyu_id']

demo_query = "select age_range, gender, nyu_id from demo_group"
cursor.execute(demo_query)
demo_groups = {}
for row in cursor:
    key = row['gender'] + row['age_range']
    demo_groups[key] = row['nyu_id']

#setup some datastructures for ease of storage before we insert
AdRecord = namedtuple('AdRecord', ['archive_id', 'id', 'page_id', 'text', 'image_url', 'video_url', 'has_cards', 'sponsor_name'])
PageRecord = namedtuple('PageRecord', ['id', 'name', 'url', 'is_deleted'])
CardRecord = namedtuple('CardRecord', ['ad_archive_id', 'text', 'title', 'video_url', 'image_url'])
SnapshotRecord = namedtuple('SnapshotRecord', ['id', 'ad_archive_id', 'start_date', 'end_date', 'is_active', \
'max_spend', 'min_spend', 'max_impressions', 'min_impressions', 'currency'])
SnapshotRegionRecord = namedtuple('SnapshotRegionRecord', ['name', 'min_impressions', 'max_impressions'])
SnapshotDemoRecord = namedtuple('SnapshotDemoRecord', ['age_range', 'gender', 'min_impressions', 'max_impressions'])

ads_to_insert = {}
ad_sponsors_to_insert = set()
cards_to_insert = {}
pages_to_insert = {}
categories_to_insert = {}
page_category_links_to_insert = {}
#parse content files
crawl_folder = config['FILES']['FOLDER']
for FolderName in os.listdir(crawl_folder):
    if FolderName == 'Keywords.txt':
        continue
    content_file = Path(crawl_folder + FolderName + "/Contents.txt")
    metadata_file = Path(crawl_folder + FolderName + "/Metadata.txt")
    if not content_file.is_file() or not metadata_file.is_file():
        continue
    print("Parsing " + FolderName + " content")
    with open(crawl_folder + FolderName + "/Contents.txt", 'r') as content_file:
        Ads = json.loads(content_file.read())[0]['payload']
        for ad_id, ad_contents in Ads.items():
            if str(ad_id) in ad_ids:
                continue #we've already seen this ad_id, we don't have to reinsert its contents
            ad_archive_id = ad_contents['adArchiveID']
            ad_has_cards = False
            fields = ad_contents['fields']
            ad_sponsor_name = fields['byline']
            if ad_sponsor_name not in ad_sponsor_ids: #we've never seen this ad sponsor before, we need to add it 
                ad_sponsors_to_insert.add(ad_sponsor_name)

            if 'cards' in fields and fields['cards']:#all cards need to get inserted to the cards table
                ad_has_cards = True
                cards_to_insert[ad_archive_id] = set()
                for card in fields['cards']:
                    text = card['body']
                    title = ''
                    if 'caption' in card:
                        title = card['caption']
                        video_url = ''
                    if 'video_hd_url' in card and card['video_hd_url']:
                        video_url = card['video_hd_url']
                    image_url = ""
                    if 'original_image_url' in card:
                        image_url = card['original_image_url']
                    curr_card = CardRecord(ad_archive_id, text, title, video_url, image_url)
                    cards_to_insert[ad_archive_id].add(curr_card)
            
            ad_page_id = int(fields['page_id'])
            if ad_page_id not in page_ids:#we've never seen this page before, so we need to add it
                page_name = fields['page_name']
                page_url = fields['page_profile_uri']
                page_is_deleted = fields['page_is_deleted']
                curr_page = PageRecord(ad_page_id, page_name, page_url, page_is_deleted)
                pages_to_insert[ad_page_id] = curr_page
                page_category_dict = fields['page_categories']
                page_category_links_to_insert[ad_page_id] = []
                for category_id, category_name in page_category_dict.items():
                    category_id = int(category_id)
                    if category_id not in category_ids:#we've never seen this category before, so we need to add it
                       categories_to_insert[category_id] = category_name

                    page_category_links_to_insert[ad_page_id].append(category_id)
               
            ad_text = fields['body']['markup']['__html'].strip()
            ad_image_url = ""
            if len(fields['images']) > 0 and fields['images'][0]['original_image_url']:
                ad_image_url = fields['images'][0]['original_image_url']

            ad_video_url = ""
            if len(fields['videos']) > 0 and fields['videos'][0]['video_hd_url']: 
                ad_video_url = fields['videos'][0]['video_hd_url']
            curr_ad = AdRecord(ad_archive_id, ad_id, ad_page_id, ad_text, ad_image_url, ad_video_url, ad_has_cards, ad_sponsor_name)
            ads_to_insert[ad_archive_id] = curr_ad

snapshots_to_insert = {}
snapshot_demos_to_insert = {}
snapshot_regions_to_insert = {}
demos_to_insert = set()
regions_to_insert = set()
impression_strings = {'<1K':(0,1000), '1K - 5K':(1000, 5000), '5K - 10K':(5000, 10000), '10K - 50K':(10000, 50000), '50K - 100K':(50000, 100000), \
'100K - 200K':(100000,200000), '200K - 500K':(200000, 500000), '500K - 1M':(500000, 1000000), '>1M':(1000000,1000000)}
spend_strings = {'<$100':(0, 100), '$100 - $499':(100, 499), '$500 - $999':(500, 1000), '$1K - $5K':(1000, 5000),'$5K - $10K':(5000, 10000),\
 '$10K - $50K':(10000,50000), '$20K - $50K':(20000, 50000), '$50K - $100K':(50000, 100000)}
#parse metadata files
for FolderName in os.listdir(crawl_folder):
    if FolderName == 'Keywords.txt':
        continue
    print("Parsing " + FolderName + " metadata")
    with open(crawl_folder + FolderName + "/Metadata.txt", 'r') as metadata_file:
        snapshots = json.loads(metadata_file.read())[0]['payload']['results']
        for snapshot in snapshots:
            if not snapshot['adInsightsInfo']:
                continue
            snapshot_id = snapshot['snapshotID']
            start_date = time.gmtime(snapshot['startDate'])
            end_date = time.gmtime(snapshot['endDate'])
            if not end_date: end_date = ""
            ad_archive_id = snapshot['adArchiveID']
            is_active = snapshot['isActive']
            currency = snapshot['adInsightsInfo']['currency']
            min_spend = max_spend = min_impressions = max_impressions = 0
            if snapshot['adInsightsInfo']['spend'] not in spend_strings or  snapshot['adInsightsInfo']['impressions'] not in impression_strings:
                continue
            min_spend, max_spend = spend_strings[snapshot['adInsightsInfo']['spend']]
            min_impressions, max_impressions = impression_strings[snapshot['adInsightsInfo']['impressions']]
            curr_snapshot = SnapshotRecord(snapshot_id, ad_archive_id, start_date, end_date, is_active, max_spend, min_spend,\
            max_impressions, min_impressions, currency)
            snapshots_to_insert[snapshot_id] = curr_snapshot
            if snapshot['adInsightsInfo']['locationData']:
                region_values = []
                for location in snapshot['adInsightsInfo']['locationData']:
                    if location['region'] not in regions:
                        regions_to_insert.add(location['region'])
                    region_values.append(SnapshotRegionRecord(location['region'], min_impressions * location['reach'], \
                    max_impressions * location['reach']))

                snapshot_regions_to_insert[snapshot_id] = region_values

            if snapshot['adInsightsInfo']['ageGenderData']:
                demo_values = []
                for demo_group in snapshot['adInsightsInfo']['ageGenderData']:
                    age_range = demo_group['age_range']
                    if 'unknown' in demo_group:
                        value = demo_group['unknown']
                        if 'unknown'+ age_range not in demo_groups:
                            demos_to_insert.add(('unknown', age_range))
                        demo_values.append(SnapshotDemoRecord(age_range, 'unknown', min_impressions * value, max_impressions * value))

                    if 'male' in demo_group:
                        value = demo_group['male']
                        if 'male'+ age_range not in demo_groups:
                            demos_to_insert.add(('male', age_range))
                        demo_values.append(SnapshotDemoRecord(age_range, 'male', min_impressions * value, max_impressions * value))

                    if 'female' in demo_group:
                        value = demo_group['female']
                        if 'female'+ age_range not in demo_groups:
                            demos_to_insert.add(('female', age_range))
                        demo_values.append(SnapshotDemoRecord(age_range, 'female', min_impressions * value, max_impressions * value))
                 
                snapshot_demos_to_insert[snapshot_id] = demo_values

# insert ad_sponsors, regions, and demo groups. We have to do these first so we can get ids
if ad_sponsors_to_insert:
    print("Inserting ad_sponsors")
    sponsor_count = 0
    ad_sponsor_insert = "INSERT INTO ad_sponsors(name) VALUES "
    for ad_sponsor in ad_sponsors_to_insert:
        ad_sponsor_insert += cursor.mogrify("(%s),", (ad_sponsor,)).decode('utf-8')
        if sponsor_count >= 250:
            ad_sponsor_insert = ad_sponsor_insert[:-1]
            ad_sponsor_insert += ";"
            cursor.execute(ad_sponsor_insert)
            sponsor_count = 0
            ad_sponsor_insert = "INSERT INTO ad_sponsors(name) VALUES "
        else:
            sponsor_count += 1

    if sponsor_count:
        ad_sponsor_insert = ad_sponsor_insert[:-1]
        ad_sponsor_insert += ";"
        cursor.execute(ad_sponsor_insert)
    
if demos_to_insert:
    print("Inserting demos")
    demo_insert = "INSERT INTO demo_group(gender, age_range) VALUES "
    for demo in demos_to_insert:
        demo_insert += cursor.mogrify("(%s, %s),", (demo[0],demo[1])).decode('utf-8') 
    demo_insert = demo_insert[:-1]
    demo_insert += ";"
    cursor.execute(demo_insert)
    
if regions_to_insert:
    print("Inserting regions")
    region_count = 0
    region_insert = "INSERT INTO regions(name) VALUES "
    for region in regions_to_insert:
        region_insert += cursor.mogrify("(%s),", (region,)).decode('utf-8')
        if region_count >= 250:
            region_insert = region_insert[:-1]
            region_insert += ";"
            cursor.execute(region_insert)
            region_count = 0
            region_insert = "INSERT INTO regions(name) VALUES "
        else:
            region_count += 1
    
    if region_count:
        region_insert = region_insert[:-1]
        region_insert += ";"
        cursor.execute(region_insert)

connection.commit()

#now, re-fetch ad sponsors, demo groups, and regions so we have the ids for inserts to come
ad_sponsor_query = "select name, nyu_id from ad_sponsors"
cursor.execute(ad_sponsor_query)
ad_sponsor_ids = {}
for row in cursor:
    ad_sponsor_ids[row['name']] = row['nyu_id']

region_query = "select name, nyu_id from regions"
cursor.execute(region_query)
regions = {}
for row in cursor:
    regions[row['name']] = row['nyu_id']

demo_query = "select age_range, gender, nyu_id from demo_group"
cursor.execute(demo_query)
demo_groups = {}
for row in cursor:
    key = row['gender'] + row['age_range']
    demo_groups[key] = row['nyu_id']

#insert ads, cards, pages, categories, page_categories
if ads_to_insert:
    print("Inserting ads")
    ad_count = 0
    ad_insert = "INSERT INTO ads(archive_id, id, page_id, text, image_url, video_url, ad_sponsor_id, has_cards) VALUES "
    for ad_archive_id, ad in ads_to_insert.items():
        ad_insert += cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s),",(ad.archive_id, ad.id, ad.page_id, ad.text, ad.image_url, \
        ad.video_url, ad_sponsor_ids[ad.sponsor_name], ad.has_cards)).decode('utf-8')
        if ad_count >= 250:
            ad_insert = ad_insert[:-1]
            ad_insert += ";"
            print("Importing ads")
            cursor.execute(ad_insert)
            ad_insert = "INSERT INTO ads(archive_id, id, page_id, text, image_url, video_url, ad_sponsor_id, has_cards) VALUES "
            ad_count = 0
        else:
            ad_count += 1
    if ad_count:
        ad_insert = ad_insert[:-1]
        ad_insert += ";"
        print("Importing ads")
        cursor.execute(ad_insert)

connection.commit() 
if cards_to_insert:
    print("Inserting cards")
    card_count = 0
    card_insert = "INSERT INTO cards(ad_archive_id, text, title, video_url, image_url) VALUES "
    for ad_archive_id, cards in cards_to_insert.items():
        for card in cards:
            card_insert += cursor.mogrify("(%s, %s, %s, %s, %s),", (card.ad_archive_id, card.text, card.title, card.video_url, card.image_url)).decode('utf-8')
            if card_count >= 250:
                card_insert = card_insert[:-1]
                card_insert += ";"
                print("Importing 250 cards")
                cursor.execute(card_insert)
                card_insert = "INSERT INTO cards(ad_archive_id, text, title, video_url, image_url) VALUES "
                card_count = 0
            else:
                card_count += 1
    if card_count:
        card_insert = card_insert[:-1]
        card_insert += ";"
        print("Importing cards")
        cursor.execute(card_insert)
        
connection.commit() 
if pages_to_insert:
    print("Inserting pages")
    page_count = 0
    page_insert = "INSERT INTO pages(id, name, url, is_deleted) VALUES "
    for curr_page_id, page in pages_to_insert.items():
        page_insert += cursor.mogrify("(%s, %s, %s, %s),", (page.id, page.name, page.url, page.is_deleted)).decode('utf-8')
        if page_count >= 250:
            page_insert = page_insert[:-1]
            page_insert += ";"
            print("Importing 250 pages")
            print(cursor.mogrify(page_insert))
            cursor.execute(page_insert)
            page_insert = "INSERT INTO pages(id, name, url, is_deleted) VALUES "
            page_count = 0
        else:
            page_count += 1
    if page_count:
        page_insert = page_insert[:-1]
        page_insert += ";"
        print("Importing pages")
        print(cursor.mogrify(page_insert))
        cursor.execute(page_insert)

connection.commit() 
if categories_to_insert:
    print("Inserting Categories")
    category_count = 0
    category_insert = "INSERT INTO categories(id, name) VALUES "
    for cat_id, cat_name in categories_to_insert.items():
        category_insert += cursor.mogrify("(%s, %s),",(cat_id, cat_name)).decode('utf-8') 
        if category_count >= 250:
            category_insert = category_insert[:-1]
            category_insert += ";"
            print("Importing 250 categories")
            cursor.execute(category_insert)
            category_insert = "INSERT INTO categories(id, name) VALUES "
            category_count = 0
        else:
            category_count += 1
    if category_count:
        category_insert = category_insert[:-1]
        category_insert += ";"
        print("Importing categories")
        cursor.execute(category_insert)

connection.commit() 
if page_category_links_to_insert:
    print("Inserting page categories")
    page_category_link_count = 0
    page_category_link_insert = "INSERT INTO page_categories(page_id, category_id) VALUES "
    for page_id, category_ids in page_category_links_to_insert.items():
        for category_id in category_ids:
            page_category_link_insert +=  cursor.mogrify("(%s, %s),", (page_id,category_id)).decode('utf-8')
            if page_category_link_count >= 250:
                page_category_link_insert = page_category_link_insert[:-1]
                page_category_link_insert += ";"
                print("Importing 250 page_categorys")
                cursor.execute(page_category_link_insert)
                page_category_link_insert = "INSERT INTO page_categories(page_id, category_id) VALUES "
                page_category_link_count = 0
            else:
                page_category_link_count += 1

    if page_category_link_count:
        page_category_link_insert = page_category_link_insert[:-1]
        page_category_link_insert += ";"
        print("Importing snapshot_categories")
        cursor.execute(page_category_link_insert)
 

connection.commit() 

#insert snapshots, snapshot_demos, snapshot_regions
if snapshots_to_insert:
    print("Inserting Snapshots")
    snapshot_count = 0
    snapshot_insert = "INSERT INTO snapshots(id, ad_archive_id, date, max_spend, min_spend, max_impressions, min_impressions, currency, \
    start_date, end_date, is_active) VALUES "
    for snapshot_id, snapshot in snapshots_to_insert.items():
        snapshot_insert += cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),", (snapshot.id, snapshot.ad_archive_id, \
        crawl_date.strftime('%Y-%m-%d'), snapshot.max_spend, snapshot.min_spend, snapshot.max_impressions, snapshot.min_impressions, \
        snapshot.currency, time.strftime('%Y-%m-%d', snapshot.start_date), time.strftime('%Y-%m-%d', snapshot.end_date), snapshot.is_active)).decode('utf-8')
        if snapshot_count >= 250:
            snapshot_insert = snapshot_insert[:-1]
            snapshot_insert += ";"
            print("Importing 250 snapshots")
            cursor.execute(snapshot_insert)
            snapshot_insert = "INSERT INTO snapshots(id, ad_archive_id, date, max_spend, min_spend, max_impressions, min_impressions, \
            currency, start_date, end_date, is_active) VALUES "
            snapshot_count = 0
        else:
            snapshot_count += 1
    if snapshot_count:
        snapshot_insert = snapshot_insert[:-1]
        snapshot_insert += ";"
        print("Importing snapshots")
        cursor.execute(snapshot_insert)

connection.commit() 

#because the snapshot_demo and snapshot_region records reference the snapshot_nyu_id of the snapshots we just entered, 
#we need to fetch those so we can do the inserts below
snapshot_query = "SELECT id, nyu_id from snapshots where date = %s;"
nyu_snapshot_ids = {}
cursor.execute(snapshot_query, (crawl_date.strftime('%Y-%m-%d'),))
for row in cursor:
    nyu_snapshot_ids[row['id']] = row['nyu_id']

 
if snapshot_demos_to_insert:
    print("Inserting Snapshot Demos")
    snapshot_demos_count = 0
    snapshot_demos_insert = "INSERT INTO snapshot_demo(demo_id, nyu_snapshot_id, max_impressions, min_impressions) VALUES "
    for snapshot_id, snapshot_demos in snapshot_demos_to_insert.items():
        for snapshot_demo in snapshot_demos:
            snapshot_demos_insert += cursor.mogrify("(%s, %s, %s, %s)," %(demo_groups[snapshot_demo.gender + snapshot_demo.age_range],\
            nyu_snapshot_ids[int(snapshot_id)], snapshot_demo.max_impressions, snapshot_demo.min_impressions)).decode('utf-8')
            if snapshot_demos_count >= 250:
                snapshot_demos_insert = snapshot_demos_insert[:-1]
                snapshot_demos_insert += ";"
                print("Importing 250 snapshot_demos")
                cursor.execute(snapshot_demos_insert)
                snapshot_demos_insert = "INSERT INTO snapshot_demo(demo_id, nyu_snapshot_id, max_impressions, min_impressions) VALUES "
                snapshot_demos_count = 0
            else:
                snapshot_demos_count += 1

    if snapshot_demos_count:
        snapshot_demos_insert = snapshot_demos_insert[:-1]
        snapshot_demos_insert += ";"
        print("Importing snapshot_demos")
        cursor.execute(snapshot_demos_insert)

connection.commit() 
if snapshot_regions_to_insert: 
    print("Inserting Snapshot Regions")
    snapshot_regions_count = 0
    snapshot_regions_insert = "INSERT INTO snapshot_region(region_id, nyu_snapshot_id, max_impressions, min_impressions) VALUES "
    print(regions)
    for snapshot_id, snapshot_regions in snapshot_regions_to_insert.items():
        for snapshot_region in snapshot_regions:
            snapshot_regions_insert += cursor.mogrify("(%s, %s, %s, %s)," %(regions[snapshot_region.name], nyu_snapshot_ids[int(snapshot_id)], \
            snapshot_region.max_impressions, snapshot_region.min_impressions)).decode('utf-8')
            if snapshot_regions_count >= 250:
                snapshot_regions_insert = snapshot_regions_insert[:-1]
                snapshot_regions_insert += ";"
                print("Importing 250 snapshot_regions")
                cursor.execute(snapshot_regions_insert)
                snapshot_regions_insert = "INSERT INTO snapshot_region(region_id, nyu_snapshot_id, max_impressions, min_impressions) VALUES "
                snapshot_regions_count = 0
            else:
                snapshot_regions_count += 1

    if snapshot_regions_count:
        snapshot_regions_insert = snapshot_regions_insert[:-1]
        snapshot_regions_insert += ";"
        print("Importing snapshot_regions")
        cursor.execute(snapshot_regions_insert)

connection.commit() 
connection.close()
