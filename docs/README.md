# Welcome to the codebase and home of NYU's effort to analyze [Facebook's Political Ad Archive](https://www.facebook.com/politicalcontentads/)
Our team at NYU performed an initial analysis of Facebook's archive of ads with political content that primarily span eight weeks from May 2018 - July 2018. 


## Introduction
Facebook launched their searchable archive of U.S. political advertisements on May 24, 2018. We devise a data collection methodology to obtain a major chunk of political ads from Facebook's political ad archive. In our paper attached below, we include our initial analysis of Facebook's political ad archive, our methodology for collecting archived political ads, and a detailed description of the database of political ads that we are releasing in conjunction with the report. 


## Facebook Political Ad Archive Overview
According to Facebook: "The archive includes Facebook and Instagram ads that have been classified as containing political content, or content about national issues of public importance." This archive provides an increased level of transparency of political ads on Facebook and Instagram. Any search returns an initial 30 political ads and when a user scrolls down it displays another 30 ads (This is called "infinity scroll" functionality). It appears that the ads returned are ordered chronologically with the newest ads based on an ad's start time displayed first.


## Data Collection Methodology 
We registered an account on Facebook and started reverse-engineering the AJAX calls that Facebook provides to search their political ad archive and request detailed information for a set of ad identifiers. We have a seperate python script that inserts the data into a database that is publically available.  We used search words from a keyword file we created and that we periodically update. The list includes names of U.S. states, titles of elected positions, names of major candidates, common political terms (i.e., vote and campaign) and major political issues (i.e., abortion and immigration). 


## Analysis
We investigate the following questions in analysis section of the paper:
- What is the size of political advertising on Facebook?
- Who is paying for political ads on Facebook?
- What issues are the focus of political ads on Facebook?
- Who is being shown political ads on Facebook?  

We also dig into our data to discover the top sponsors, pages, demographic groups based on minimum impressions garnered. 

## The team.
### [Laura Edelson](https://www.linkedin.com/in/laura-edelson-4654182/) 
### [Shikhar Sakhuja](https://www.linkedin.com/in/shikhar394/) 
### [Damon McCoy](http://damonmccoy.com) 

