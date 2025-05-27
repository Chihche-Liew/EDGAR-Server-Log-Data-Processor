import os
import time
import glob
import zipfile
import requests
import warnings
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from tqdm import tqdm
warnings.filterwarnings("ignore")

class EdgarLogsProcessor:
    def __init__(self, data_base_dir='./data',
                 start_year_logs=2011, end_year_logs=2017,
                 start_year_index=2010, end_year_index=2017,
                 target_ip_list_path=None, # Path to CSV with 'netblock' column for IP filtering
                 processed_log_prefix='processed_'):
        self.data_base_dir = data_base_dir
        self.output_pickle_path = os.path.join(self.data_base_dir, f'{processed_log_prefix}visiting_details_{start_year_logs}_{end_year_logs}.pkl')

        self.log_years = list(range(start_year_logs, end_year_logs + 1))
        self.index_years = list(range(start_year_index, end_year_index + 1))

        self.headers = {
            'Host': 'www.sec.gov',
            'Connection': 'close',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
        }

        self.target_ip_list_path = target_ip_list_path
        self.target_ip_prefixes = self._load_target_ip_prefixes()
        self.processed_log_prefix = processed_log_prefix

        os.makedirs(self.data_base_dir, exist_ok=True)
        os.makedirs(os.path.join(self.data_base_dir, 'sec_master'), exist_ok=True)
        for year in self.log_years:
            os.makedirs(os.path.join(self.data_base_dir, str(year)), exist_ok=True)

    def _load_target_ip_prefixes(self):
        if not self.target_ip_list_path:
            print("INFO: No target IP list path provided. IP filtering will be skipped.")
            return []
        if not os.path.exists(self.target_ip_list_path):
            print(f"WARNING: Target IP list file not found at '{self.target_ip_list_path}'. IP filtering will be skipped.")
            return []
        try:
            ip_df = pd.read_csv(self.target_ip_list_path)
            if 'netblock' not in ip_df.columns:
                print(f"WARNING: 'netblock' column not found in '{self.target_ip_list_path}'. IP filtering will be skipped.")
                return []

            prefixes = ip_df['netblock'].apply(lambda x: str(x).split('/')[0][:-2]).values.tolist()
            print(f"INFO: Loaded {len(prefixes)} IP prefixes from '{self.target_ip_list_path}'.")
            return prefixes
        except Exception as e:
            print(f"ERROR: Error loading target IP prefixes from '{self.target_ip_list_path}': {e}. IP filtering will be skipped.")
            return []

    def _download_file(self, url, local_path):
        try:
            response = requests.get(url, stream=True, headers=self.headers)
            response.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return True
        except requests.exceptions.RequestException as e:
            print(f"WARNING: Failed to download {url}: {e}")
            return False

    def _process_log_file(self, csv_path, year_dir):
        raw_file_name = os.path.basename(csv_path)
        try:
            logs = pd.read_csv(csv_path)

            logs = logs.loc[
                ~(logs['crawler'] == 1) &
                ~(logs['idx'] == 1) &
                ~(logs['code'] < 300)
                ]
            logs = logs.dropna(subset=['cik', 'accession', 'date', 'ip'])

            if logs.empty:
                if os.path.exists(csv_path): os.remove(csv_path)
                return

            if self.target_ip_prefixes:
                logs['target_ip_hit'] = logs['ip'].apply(lambda x: any(str(x).startswith(prefix) for prefix in self.target_ip_prefixes))
                logs = logs.loc[logs['target_ip_hit'] == True]
                if logs.empty:
                    if os.path.exists(csv_path): os.remove(csv_path)
                    return

            if logs.empty:
                if os.path.exists(csv_path): os.remove(csv_path)
                return

            logs['cik'] = logs['cik'].astype(int)
            logs['extention'] = logs['extention'].apply(lambda x: str(x).split('.')[-1].lower())

            logs['htm'] = (logs['extention'] == 'htm').astype(int)
            logs['txt'] = (logs['extention'] == 'txt').astype(int)
            logs['xbrl'] = (logs['extention'] == 'xbrl').astype(int)
            logs['other'] = np.where(logs['extention'].isin(['htm', 'txt', 'xbrl']), 0, 1)

            logs_grouped = logs.groupby(['date', 'cik', 'accession'])[['htm', 'txt', 'xbrl', 'other']].sum().reset_index()
            logs_grouped['nr_total'] = logs_grouped['htm'] + logs_grouped['txt'] + logs_grouped['xbrl'] + logs_grouped['other']

            final_log_cols = ['date', 'cik', 'accession', 'nr_total', 'htm', 'txt', 'xbrl', 'other']
            logs_output = logs_grouped[final_log_cols]

            processed_file_name = f"{self.processed_log_prefix}{raw_file_name.replace('log', '')}"
            processed_file_path = os.path.join(year_dir, processed_file_name)
            logs_output.to_csv(processed_file_path, index=False)

        except pd.errors.EmptyDataError:
            pass
        except Exception as e:
            print(f"ERROR: Failed to process {raw_file_name}: {e}")
        finally:
            if os.path.exists(csv_path):
                os.remove(csv_path)

    def run_log_processing(self):
        print("INFO: Starting EDGAR log downloading and processing...")
        for year in self.log_years:
            print(f"INFO: Processing logs for {year}...")
            year_download_dir = os.path.join(self.data_base_dir, str(year))

            try:
                index_page_url = f'https://www.sec.gov/files/edgar{year}.html'
                response = requests.get(index_page_url, headers=self.headers)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"ERROR: Failed to fetch log index page for {year} from {index_page_url}: {e}")
                continue

            soup = BeautifulSoup(response.content, 'html.parser')
            log_zip_urls = []
            # Example URL: https://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2011/log20110101.zip
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if href.startswith('https://www.sec.gov/dera/data/Public-EDGAR-log-file-data/') and href.endswith('.zip'):
                    log_zip_urls.append(href)

            if not log_zip_urls:
                print(f"WARNING: No log files found for {year} on the index page: {index_page_url}")
                continue

            for url in tqdm(log_zip_urls, desc=f"Logs for {year}"):
                file_name_zip = os.path.basename(url)
                local_zip_path = os.path.join(year_download_dir, file_name_zip)

                if not self._download_file(url, local_zip_path):
                    continue

                try:
                    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                        csv_files_in_zip = [member for member in zip_ref.namelist() if member.endswith('.csv')]
                        if not csv_files_in_zip:
                            print(f"WARNING: No CSV files found in {file_name_zip}. Skipping.")
                            continue
                        for csv_file_member in csv_files_in_zip:
                            zip_ref.extract(csv_file_member, year_download_dir)
                            extracted_csv_path = os.path.join(year_download_dir, csv_file_member)
                            self._process_log_file(extracted_csv_path, year_download_dir)
                except zipfile.BadZipFile:
                    print(f"ERROR: Bad zip file encountered: {local_zip_path}. Skipping.")
                except Exception as e:
                    print(f"ERROR: Failed during extraction or processing of {file_name_zip}: {e}")
                finally:
                    if os.path.exists(local_zip_path):
                        os.remove(local_zip_path)
        print("INFO: EDGAR log downloading and processing finished.")

    def run_master_index_download(self):
        print("INFO: Starting EDGAR master index download...")
        master_index_dir = os.path.join(self.data_base_dir, 'sec_master')
        all_index_dfs = []

        for year in self.index_years:
            print(f"INFO: Downloading EDGAR Master Index for {year}...")
            for quarter in tqdm(['QTR1', 'QTR2', 'QTR3', 'QTR4'], desc=f"Quarters for {year}"):
                url = f'https://www.sec.gov/Archives/edgar/full-index/{year}/{quarter}/master.idx'
                file_path = os.path.join(master_index_dir, f'master_index_{year}_{quarter}.txt')

                retries = 0
                max_retries = 3
                while retries < max_retries:
                    if self._download_file(url, file_path):
                        try:
                            skip_lines = 0
                            header_found = False
                            with open(file_path, 'r', encoding='latin1') as f_idx:
                                for i, line in enumerate(f_idx):
                                    if 'CIK|Company Name|Form Type|Date Filed|Filename' in line:
                                        skip_lines = i
                                        header_found = True
                                        break
                            if not header_found:
                                print(f"WARNING: Standard header not found in {file_path}, using default skiprows=9.")
                                skip_lines = 9

                            df = pd.read_csv(file_path, sep='|', skiprows=skip_lines, dtype=str, engine='python', encoding='latin1')

                            df.columns = df.columns.str.strip()

                            expected_cols = ['CIK', 'Form Type', 'Date Filed', 'Filename']
                            if not all(col in df.columns for col in expected_cols):
                                print(f"ERROR: Master index {file_path} is missing one or more expected columns ({expected_cols}). Found: {df.columns.tolist()}. Skipping this file.")
                                break

                            if not df.empty and df.iloc[0]['CIK'] and df.iloc[0]['CIK'].isdigit():
                                pass
                            else:
                                df = df.iloc[1:]

                            all_index_dfs.append(df)
                            break
                        except Exception as e:
                            print(f"ERROR: Error processing master index {file_path}: {e}")
                            retries += 1
                            if retries < max_retries:
                                print(f"Retrying download/processing for {url} (attempt {retries+1}/{max_retries})...")
                                time.sleep(2)
                            else:
                                print(f"ERROR: Failed to process {url} after {max_retries} retries.")
                            if os.path.exists(file_path): os.remove(file_path)
                    else:
                        retries += 1
                        if retries < max_retries:
                            print(f"Retrying download for {url} (attempt {retries+1}/{max_retries})...")
                            time.sleep(2)
                        else:
                            print(f"ERROR: Failed to download {url} after {max_retries} retries.")
                            break

        if not all_index_dfs:
            print("ERROR: No master index data was successfully downloaded or processed.")
            return pd.DataFrame()

        index_df = pd.concat(all_index_dfs, ignore_index=True)

        if 'Filename' in index_df.columns and 'Form Type' in index_df.columns and 'Date Filed' in index_df.columns:
            index_df['acc_path'] = index_df['Filename'].apply(lambda x: str(x).split('.')[0])
            index_df = index_df[['Form Type', 'Date Filed', 'acc_path']]
            index_df = index_df.drop_duplicates().reset_index(drop=True)
        else:
            print("ERROR: Concatenated master index is missing 'Filename', 'Form Type', or 'Date Filed'. Cannot finalize index processing.")
            return pd.DataFrame()

        print("INFO: EDGAR master index download and initial processing finished.")
        return index_df

    def merge_logs_and_index(self, master_index_df):
        if master_index_df.empty:
            print("ERROR: Master index is empty. Cannot merge with logs.")
            return pd.DataFrame()

        print("INFO: Merging processed logs with master index...")
        all_merged_logs = []
        for year in tqdm(self.log_years, desc="Merging logs by year"):
            year_dir = os.path.join(self.data_base_dir, str(year))
            csv_files = glob.glob(os.path.join(year_dir, f"{self.processed_log_prefix}*.csv"))

            if not csv_files:
                continue

            df_list = []
            for file in csv_files:
                try:
                    df_list.append(pd.read_csv(file))
                except pd.errors.EmptyDataError:
                    pass
                except Exception as e:
                    print(f"ERROR: Error reading processed log file {file}: {e}")

            if not df_list:
                continue

            year_logs_df = pd.concat(df_list, ignore_index=True)

            if not year_logs_df.empty:
                if 'cik' not in year_logs_df.columns or 'accession' not in year_logs_df.columns:
                    print(f"ERROR: Logs for year {year} are missing 'cik' or 'accession' columns. Skipping merge for this year.")
                    continue

                year_logs_df['accession'] = year_logs_df['accession'].astype(str).str.replace(r'\.0$', '', regex=True)
                year_logs_df['cik'] = year_logs_df['cik'].astype(int)

                year_logs_df['acc_path'] = year_logs_df.apply(lambda x: f"edgar/data/{x['cik']}/{str(x['accession']).replace('-', '')}", axis=1)

                merged_df = year_logs_df.merge(master_index_df, on='acc_path', how='left')
                all_merged_logs.append(merged_df)

        if not all_merged_logs:
            print("ERROR: No log data could be merged with the index across all years.")
            return pd.DataFrame()

        final_logs = pd.concat(all_merged_logs, ignore_index=True)
        final_logs = final_logs.dropna(subset=['Form Type', 'Date Filed'])

        if final_logs.empty:
            print("ERROR: Resulting merged log data is empty after dropping NaNs from merge keys (Form Type, Date Filed).")
            return pd.DataFrame()

        final_logs['date'] = pd.to_datetime(final_logs['date'])
        final_logs = final_logs.rename(columns={'Form Type': 'form', 'Date Filed': 'filing_date'})
        if 'acc_path' in final_logs.columns:
            final_logs = final_logs.drop(columns=['acc_path'])

        final_logs = final_logs.sort_values(['cik', 'date', 'form']).reset_index(drop=True)

        final_logs['cik'] = final_logs['cik'].astype(int)
        for col in ['nr_total', 'htm', 'txt', 'xbrl', 'other']:
            if col in final_logs.columns:
                final_logs[col] = final_logs[col].astype(int)
            else:
                print(f"WARNING: Column {col} not found in final_logs for type conversion during merge step.")

        try:
            final_logs.to_pickle(self.output_pickle_path, compression='zip')
            print(f"INFO: Saved merged logs to Pickle: {self.output_pickle_path}")
        except Exception as e:
            print(f"ERROR: Failed to save merged logs to Pickle ({self.output_pickle_path}): {e}")

        print("INFO: Merging logs with index finished.")
        return final_logs

    def run_pipeline(self):
        print("INFO: Starting EDGAR data processing pipeline...")

        self.run_log_processing()

        master_index_df = self.run_master_index_download()
        if master_index_df.empty:
            print("ERROR: Master index download/processing failed or produced no data. Halting pipeline.")
            return

        merged_logs_df = self.merge_logs_and_index(master_index_df)
        if merged_logs_df.empty:
            print("WARNING: Merging logs with index produced no data or failed.")


if __name__ == '__main__':
    DATA_BASE_DIRECTORY = './data'

    LOG_START_YEAR = 2003
    LOG_END_YEAR = 2017

    INDEX_START_YEAR = 2003
    INDEX_END_YEAR = 2017

    TARGET_IP_CSV_PATH = None

    PROCESSED_LOG_FILE_PREFIX = 'edgar_visits_'

    processor = EdgarLogsProcessor(
        data_base_dir=DATA_BASE_DIRECTORY,
        start_year_logs=LOG_START_YEAR,
        end_year_logs=LOG_END_YEAR,
        start_year_index=INDEX_START_YEAR,
        end_year_index=INDEX_END_YEAR,
        target_ip_list_path=TARGET_IP_CSV_PATH,
        processed_log_prefix=PROCESSED_LOG_FILE_PREFIX
    )

    processor.run_pipeline()