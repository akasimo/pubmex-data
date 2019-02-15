
# coding: utf-8

# In[60]:


import requests, os,time
import pandas as pd

from parsel import Selector
from datetime import datetime, timedelta

import multiprocessing, urllib
import gzip, shutil


# In[30]:


def print_date(date):
    return date.strftime("%Y%m%d")


# In[32]:


def create_dates(starting_from):
    delta = datetime.today() - start_date         # timedelta
    date_list = [start_date + timedelta(days=x) for x in range(0, delta.days)]
    return date_list


# In[55]:


def save_links(dl_folder, links):
    for i in map(print_date, date_list):
        links.append(base_url+i+".csv.gz")
    return links


# In[59]:


def process_url(link):
    
    print(count, link)
    count += 1
    
    if "csv.gz" or ".zip" not in link:
        print( "not a dl link")
        return
        
    filename = link.split("/")[-1]
    filepath = os.path.join(dl_folder, filename)
    try:
        csvpath = os.path.join(dl_folder, filepath.split("gz")[0][:-1])
    except:
        csvpath = os.path.join(dl_folder, filepath.split("zip")[0][:-1])
        
    if filename in os.listdir(dl_folder):
        print( "already downloaded" )
        return
    elif csvpath in os.listdir(dl_folder):
        print("csv already downloaded")
    else:
        urllib.request.urlretrieve(link, filepath)
        print("downloading")
    return 


# In[ ]:


def unzip(zipfile, dl_folder, zcount):
    print(zcount, zipfile)
    zcount += 1
    if not (".gz" in zipfile or ".zip" in zipfile):
        print("Skipped\n")
        return zcount
    else:
        zippath = os.path.join(dl_folder, zipfile)
        try:
            csvpath = os.path.join(dl_folder, zipfile.split("gz")[0][:-1])
        except:
            csvpath = os.path.join(dl_folder, zipfile.split("zip")[0][:-1])
        if csvpath in os.listdir(dl_folder):
            print("csv already exists")
            return zcount
        
        with gzip.open(zippath, 'rb') as f_in:
            with open(csvpath, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print("Extracted")
        os.remove(zippath)
        print("Zipfile removed\n")
        return zcount


# In[ ]:


dl_types = ["quote", "trade"]
start_date = datetime(year = 2014, month = 11, day = 22)

for dl_type in dl_types:
    dl_folder = dl_type
    if dl_folder not in os.listdir():
        os.mkdir(dl_folder)
    base_url = "https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/"+dl_folder+"/"
    href_links = []
    href_links = save_links(dl_folder,href_links)
    count = 1
    pool = multiprocessing.Pool(processes=4) # how much parallelism?
    pool.map(process_url, href_links)
    
    zcount = 0
    for zipfile in os.listdir(dl_folder):
        zcount = unzip(zipfile, dl_folder, zcount)

