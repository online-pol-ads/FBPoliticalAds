import configparser
import datetime
import json
import os
import sys
import time
from collections import namedtuple
from pathlib import Path

import psycopg2
import psycopg2.extras

if len(sys.argv) < 2:
    exit("Usage:python3 import_ads_to_db.py import_ads_to_db.cfg")

config = configparser.ConfigParser()
config.read(sys.argv[1])

crawl_date = datetime.date.today() 

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


category_query = "select id from categories"
cursor.execute(category_query)
category_ids = set()
for row in cursor:
    category_ids.add(row['id'])

#setup some datastructures for ease of storage before we insert
AdRecord = namedtuple('AdRecord', ['archive_id', 'id', 'page_id', 'text', 'image_url', 'video_url', 'has_cards', 'sponsor_name', 'start_date', 'end_date', 'is_active'])
PageRecord = namedtuple('PageRecord', ['id', 'name', 'url', 'is_deleted'])
CardRecord = namedtuple('CardRecord', ['ad_archive_id', 'text', 'title', 'video_url', 'image_url'])

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
    print("Parsing " + FolderName + " content")
    with open(crawl_folder + FolderName + "/Contents.txt", 'r') as content_file:
        json_data = json.loads(content_file.read())
        for section in json_data:
            if 'payload' not in section:
                print("no payload")
                continue
            Ads = section['payload']['results']
            for ad in Ads:
                ad_id = ad['adArchiveID']
                if int(ad_id) in ad_ids:
                    continue #we've already seen this ad_id, we don't have to reinsert its contents
                ad_archive_id = ad['adArchiveID']
                ad_has_cards = False
                fields = ad['snapshot']
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
                    page_category_dict = {}
                    if 'page_categories' in fields: page_category_dict = fields['page_categories']
                    page_category_links_to_insert[ad_page_id] = []
                    for category_id, category_name in page_category_dict.items():
                        category_id = int(category_id)
                        if category_id not in category_ids:#we've never seen this category before, so we need to add it
                           categories_to_insert[category_id] = category_name

                        page_category_links_to_insert[ad_page_id].append(category_id)
                 
                ad_text = ""
                if 'body' in fields and 'markup' in fields['body']: ad_text = fields['body']['markup']['__html'].strip()
                ad_image_url = ""
                if 'images' in fields and len(fields['images']) > 0 and fields['images'][0]['original_image_url']:
                    ad_image_url = fields['images'][0]['original_image_url']

                ad_video_url = ""
                if 'videos' in fields and len(fields['videos']) > 0 and fields['videos'][0]['video_hd_url']: 
                    ad_video_url = fields['videos'][0]['video_hd_url']

                start_date = time.gmtime(ad['startDate'])
                end_date = time.gmtime(ad['endDate'])
                if not end_date: end_date = ""
                is_active = ad['isActive']

                curr_ad = AdRecord(ad_archive_id, ad_id, ad_page_id, ad_text, ad_image_url, ad_video_url, ad_has_cards, ad_sponsor_name, start_date, end_date, is_active)
                ads_to_insert[ad_archive_id] = curr_ad


# insert ad_sponsors first so we can get ids
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
    
connection.commit()

#now, re-fetch ad sponsors, demo groups, and regions so we have the ids for inserts to come
ad_sponsor_query = "select name, nyu_id from ad_sponsors"
cursor.execute(ad_sponsor_query)
ad_sponsor_ids = {}
for row in cursor:
    ad_sponsor_ids[row['name']] = row['nyu_id']


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
        print("Importing page_categories")
        cursor.execute(page_category_link_insert)
 

connection.commit() 

connection.close()
