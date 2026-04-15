from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
import os
import time
import sys
import traceback
import subprocess
import shutil
from dotenv import load_dotenv

load_dotenv()


# ========== CONFIGURATION ==========
PROFILE_PATH = os.getenv("PROFILE_PATH")  # <-- adjust to correct
OUTPUT_FILE = "data/sector/sector_a_z.csv"
WAIT_TIME = 3           
MAX_RETRY = 2            
MAX_PAGES = 2000     
HEADLESS = False     
# ===================================

if not os.path.exists(OUTPUT_FILE):
    os.mkdir(OUTPUT_FILE)

columns = ["NO", "Symbol", "Name", "Sector", "Market", "NY/DKGD volume"]

def kill_firefox_processes():
    try:
        subprocess.run(["pkill", "-f", "firefox"], check=False)
        time.sleep(1.0)
    except Exception:
        pass

def remove_profile_locks(profile_path):
    try:
        lock_files = ["parent.lock", "lock", "sessionstore.js", "sessionstore.bak"]
        for name in lock_files:
            p = os.path.join(profile_path, name)
            if os.path.exists(p):
                try:
                    os.remove(p)
                    # print("Removed lock:", p)
                except Exception:
                    pass
    except Exception:
        pass

def init_driver(profile_path, headless=False):
    if not os.path.isdir(profile_path):
        raise FileNotFoundError(f"Profile path không tồn tại: {profile_path}")

    kill_firefox_processes()
    
    remove_profile_locks(profile_path)

    options = Options()
    if headless:
        options.add_argument("--headless")

    options.add_argument("--profile")
    options.add_argument(profile_path)

    # service = Service(log_path="geckodriver.log")
    service = Service()

    driver = webdriver.Firefox(service=service, options=options)
    return driver

def save_partial(collected, output_file=OUTPUT_FILE):
    df = pd.DataFrame(collected, columns=columns)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"The {len(df)} line has been temporarily saved to the {output_file}.")

def parse_table_from_source(html):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="table table-striped table-bordered table-hover table-middle pos-relative m-b")
    
    if not table:
        return []
    
    tbody = table.find("tbody")
    if not tbody:
        return []
    rows = tbody.find_all("tr")
    data = []
    
    for r in rows:
        cells = [c.text.strip() for c in r.find_all("td")]
        if len(cells) >= len(columns):
            data.append(cells[:len(columns)])
    return data

def main():
    if not os.path.isdir(PROFILE_PATH):
        print(f"ERROR: Profile path does not exists: {PROFILE_PATH}")
        print("Please check the profile path. (ls ~/.mozilla/firefox/).")
        sys.exit(1)

    if os.path.exists(OUTPUT_FILE):
        try:
            df_existing = pd.read_csv(OUTPUT_FILE)
            collected = df_existing.values.tolist()
            print(f"Continue from the existing {len(collected)} row.")
        except Exception:
            print("Unable to read existing file, start a new one.")
            collected = []
    else:
        collected = []

    # Init driver
    print("Initialize Firefox (using a profile) ...")
    try:
        driver = init_driver(PROFILE_PATH, headless=HEADLESS)
    except Exception as e:
        print("Unable to initialize Firefox with the given profile")
        traceback.print_exc()
        sys.exit(1)

    try:
        driver.get("https://finance.vietstock.vn/doanh-nghiep-a-z")
        time.sleep(WAIT_TIME + 2)

        page = 1
        pbar = tqdm(total=MAX_PAGES, desc="Crawling", unit="page")
        try:
            pbar.n = max(0, (len(collected) // 20))
        except:
            pbar.n = 0
        pbar.refresh()

        consecutive_no_table = 0

        while page <= MAX_PAGES:
            print(f"\nGet page {page} ...")
            
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(WAIT_TIME)

            html = driver.page_source
            rows = parse_table_from_source(html)

            if not rows:
                consecutive_no_table += 1
                print(f"Table not found on page {page} (try again) {consecutive_no_table}/{MAX_RETRY})")
                if consecutive_no_table <= MAX_RETRY:
                    time.sleep(2)
                    continue
                else:
                    print("No table after multiple attempts. Stop.")
                    break
            else:
                consecutive_no_table = 0

            
            for r in rows:
                collected.append(r)

            save_partial(collected, OUTPUT_FILE)

            # Click Next
            try:
                next_btn = driver.find_element(By.ID, "btn-page-next")
                cls_attr = next_btn.get_attribute("class") or ""
                if "disabled" in cls_attr:
                    print("We've reached the last page (Next is disabled).")
                    break

                # Safe click
                driver.execute_script("arguments[0].click();", next_btn)
                page += 1
                pbar.update(1)
                time.sleep(WAIT_TIME)
                
            except Exception as e:
                print("Error when searching/clicking Next:", e)
                traceback.print_exc()
                break

        pbar.close()
        print(f"\nCompleted! The total of {len(collected)} data rows has been saved. {OUTPUT_FILE}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    main()
    
    
    
    
    