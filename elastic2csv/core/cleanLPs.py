import logging
log = logging.getLogger(__name__)
import os
import pandas as pd
from dotenv import load_dotenv
import config

def clean_lps(file):
    raw = pd.read_csv(file)
    raw.dropna(subset=['url'], inplace=True)
    raw["url"] = raw["url"].str.split("?")
    print(raw.head())
    raw.url = raw.url.apply(lambda x : x[0])
    print(f'Started at: {len(raw)}')

    # remove date from url
    raw["url"] = raw.url.str.replace("[0-9]{4}-[0-9]{2}-[0-9]{2}(.*)", "", regex=True)

    agg = {"value":"sum", "sessions":"sum"}
    raw = raw.groupby(raw["url"], as_index=True).agg(agg).reset_index()

    print(f'reduced to: {len(raw)}')

    com = raw[raw["url"].str.contains(f"{os.environ['URL']}/", regex=True)]
    com.reset_index(inplace=True, drop=True)
    print(f'reduced to: {len(com)}')

    com["url"] = com.url.str.replace("#(.*)", "", regex=True)

    com = com.groupby(com["url"], as_index=True).agg(agg).reset_index()
    print(f'reduced to: {len(com)}')

    # keep only urls with certain sub pages
    com = com[com["url"].str.contains("|".join(config.transports_com))]
    print(f'reduced to: {len(com)}')

    print(com.head())

    com.to_csv("finalLPList.csv", index=False)


if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(filename=f'std.log', level=logging.INFO, filemode='w', format='[%(asctime)s-%(levelname)s] %(name)s: %(message)s')
    log.info(f'Starting cleaning')
    clean_lps("LPList.csv")
