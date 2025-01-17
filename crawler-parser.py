import os
import csv
import json
import logging
import html
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from urllib.parse import urlencode
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]

OPTIONS = webdriver.ChromeOptions()
OPTIONS.add_argument("--headless")
OPTIONS.add_argument("--disable-javascript")

## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scrape_channel(channel_name, location, retries=3):
    url = f"https://www.tiktok.com/@{channel_name}"
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            driver = webdriver.Chrome(options=OPTIONS)
            
            driver.get(url)
                ## Extract Data
            json_stuff = driver.find_element(By.CSS_SELECTOR, "pre").get_attribute("innerHTML")
            page = json.loads(json_stuff)
            decoded_chunk = html.unescape(page["body"])
            
            soup = BeautifulSoup(decoded_chunk, "html.parser")
            

            script_tag = soup.select_one("script[id='__UNIVERSAL_DATA_FOR_REHYDRATION__']")

            json_data = json.loads(script_tag.text)
            user_info = json_data["__DEFAULT_SCOPE__"]["webapp.user-detail"]["userInfo"]
            stats = user_info["stats"]


            follower_count = stats["followerCount"]
            likes = stats["heartCount"]
            video_count = stats["videoCount"]

            user_data = user_info["user"]
            unique_id = user_data["uniqueId"]
            nickname = user_data["nickname"]
            verified = user_data["verified"]
            signature = user_data["signature"]

            profile_data = {
                "name": unique_id,
                "follower_count": follower_count,
                "likes": likes,
                "video_count": video_count,
                "nickname": nickname,
                "verified": verified,
                "signature": signature
            }

            print(profile_data)
            success = True   
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
            tries+=1

        finally:
            driver.quit()

    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")



def start_scrape(channel_list, location, data_pipeline=None, retries=3):
    for channel in channel_list:
        scrape_channel(channel, location, data_pipeline=data_pipeline, retries=retries)


if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    LOCATION = "uk"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    channel_list = [
        "paranormalpodcast",
        "theparanormalfiles",
        "jdparanormal",
        "paranormal.com7",
        "paranormal064",
        "marijoparanormal",
        "paranormal_activityghost",
        "youtube_paranormal"
        ]

    ## Job Processes
    start_scrape(channel_list, LOCATION, retries=MAX_RETRIES)
    logger.info(f"Crawl complete.")