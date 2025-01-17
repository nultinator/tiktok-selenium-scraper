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



@dataclass
class ProfileData:
    name: str = ""
    follower_count: int = 0
    likes: int = 0
    video_count: int = 0
    nickname: str = ""
    verified: bool  = False
    signature: str = ""

    def __post_init__(self):
        self.check_string_fields()
        
    def check_string_fields(self):
        for field in fields(self):
            # Check string fields
            if isinstance(getattr(self, field.name), str):
                # If empty set default text
                if getattr(self, field.name) == "":
                    setattr(self, field.name, f"No {field.name}")
                    continue
                # Strip any trailing spaces, etc.
                value = getattr(self, field.name)
                setattr(self, field.name, value.strip())


class DataPipeline:
    
    def __init__(self, csv_filename="", storage_queue_limit=50):
        self.names_seen = []
        self.storage_queue = []
        self.storage_queue_limit = storage_queue_limit
        self.csv_filename = csv_filename
        self.csv_file_open = False
    
    def save_to_csv(self):
        self.csv_file_open = True
        data_to_save = []
        data_to_save.extend(self.storage_queue)
        self.storage_queue.clear()
        if not data_to_save:
            return

        keys = [field.name for field in fields(data_to_save[0])]
        file_exists = os.path.isfile(self.csv_filename) and os.path.getsize(self.csv_filename) > 0
        with open(self.csv_filename, mode="a", newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=keys)

            if not file_exists:
                writer.writeheader()

            for item in data_to_save:
                writer.writerow(asdict(item))

        self.csv_file_open = False
                    
    def is_duplicate(self, input_data):
        if input_data.name in self.names_seen:
            logger.warning(f"Duplicate item found: {input_data.name}. Item dropped.")
            return True
        self.names_seen.append(input_data.name)
        return False
            
    def add_data(self, scraped_data):
        if self.is_duplicate(scraped_data) == False:
            self.storage_queue.append(scraped_data)
            if len(self.storage_queue) >= self.storage_queue_limit and self.csv_file_open == False:
                self.save_to_csv()
                       
    def close_pipeline(self):
        if self.csv_file_open:
            time.sleep(3)
        if len(self.storage_queue) > 0:
            self.save_to_csv()


def scrape_channel(channel_name, location, data_pipeline=None, retries=3):
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

            profile_data = ProfileData(
                name=unique_id,
                follower_count=follower_count,
                likes=likes,
                video_count=video_count,
                nickname=nickname,
                verified=verified,
                signature=signature
            )

            data_pipeline.add_data(profile_data)
            success = True   
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
            tries+=1

        finally:
            driver.quit()

    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")



def start_scrape(channel_list, location, data_pipeline=None, max_threads=5, retries=3):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        executor.map(
            scrape_channel,
            channel_list,
            [location] * len(channel_list),
            [data_pipeline] * len(channel_list),
            [retries] * len(channel_list)
        )


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
    crawl_pipeline = DataPipeline(csv_filename="channels.csv")
    start_scrape(channel_list, LOCATION, data_pipeline=crawl_pipeline, max_threads=MAX_THREADS, retries=MAX_RETRIES)
    crawl_pipeline.close_pipeline()
    logger.info(f"Crawl complete.")