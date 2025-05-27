# EDGAR Server Log Data Processor

## Description

This project provides a Python script to download, process, and merge server logs and master index files from the U.S. Securities and Exchange Commission (SEC) EDGAR system. The script allows users to filter logs based on an optional list of IP address prefixes, enabling tracking of access records from specific sources. If no IP list is provided, it processes all logs that meet other basic filtering criteria. The processed data includes access date, company identifier (CIK), accession number, total access count, and access counts for different file types (HTM, TXT, XBRL, Other). This data is then merged with master index files to obtain form type and filing date.

The core logic is encapsulated within the `EdgarLogsProcessor` class for ease of configuration and reuse.

## Features

- **Download EDGAR Server Logs**: Automatically downloads daily EDGAR server log ZIP files for a specified range of years.
- **Download** EDGAR **Master Index Files**: Automatically downloads EDGAR master index files (`master.idx`) for a specified range of years and quarters.
- **Log File Preprocessing**:
  - Filters out records from web crawlers (`crawler=1`), index page requests (`idx=1`), and server response codes greater than or equal to 300, following recommendations by Loughran and McDonald.
  - Removes records missing CIK, accession number, IP address, or date.
  - **Flexible IP Address Filtering**:
    - Users can provide a CSV file containing IP address segments (netblocks). The script will then process only access records from these IP addresses. The IP segment processing method is based on Stice-Lawrence (2023).
    - If no IP address list is provided, this filtering step is skipped, and logs from all IP addresses that meet other filtering criteria are processed.
  - Extracts file extensions of accessed files and counts accesses for `htm`, `txt`, `xbrl`, and `other` file types.
  - Aggregates access data by date, CIK, and accession number.
- **Data Merging**: Merges processed server logs with master index file data to add form type and filing date information to each access record.
- **Output Format**: Saves the final merged dataset in Pickle (`.pkl`) format.
- **Modular Design**: Main functionalities are encapsulated in the `EdgarLogsProcessor` class, making it easy to understand and extend.
- **Parameterized Configuration**: Supports configuration of data storage directory, processing year range, IP address list file path, and output file prefix through parameters.

## Dependencies

Ensure you have the following Python libraries installed in your environment:

```
pip install pandas numpy requests beautifulsoup4 tqdm
```

Standard libraries used in the script include `os`, `zipfile`, `warnings`, `time`, and `glob`.

## Usage

1. **Preparation**:

   - Save the script (e.g., as `edgar_processor.py`) in your project.
   - Modify the configuration parameters in the `if __name__ == '__main__':` section of the script as needed:
     - `DATA_BASE_DIRECTORY`: Base directory for data input and output.
     - `LOG_START_YEAR`, `LOG_END_YEAR`: Start and end years for EDGAR server log download and processing.
     - `INDEX_START_YEAR`, `INDEX_END_YEAR`: Start and end years for EDGAR master index file download.
     - `TARGET_IP_CSV_PATH`: (Optional) Path to a CSV file used for IP address filtering.
       - This CSV file should contain a column named `netblock`, with each row being an IP address segment (e.g., "123.45.67.0/24" or "192.168.1.0"). The script will extract its prefix (e.g., "123.45.67.") for matching.
       - If set to `None`, the script will not perform IP list-based filtering.
       - For example, to use `sec_ip.csv` (from https://ipinfo.io/AS26229) for filtering, ensure the file is in `DATA_BASE_DIRECTORY` and set `TARGET_IP_CSV_PATH = os.path.join(DATA_BASE_DIRECTORY, 'sec_ip.csv')`.
     - `PROCESSED_LOG_FILE_PREFIX`: Prefix for the processed daily log files and the final merged output filenames.

2. Run the Script:

   Execute the script from your terminal:

   ```
   python edgar_processor.py
   ```

3. **Input Files**:

   - **(Optional)** IP Address List CSV File: If `TARGET_IP_CSV_PATH` is provided, the script will read this file for IP filtering. The file must contain a `netblock` column.

4. **Output Files**:

   - **Processed Daily Logs**: Stored in the `DATA_BASE_DIRECTORY/<year>/` directory, with filenames in the format `{PROCESSED_LOG_FILE_PREFIX}{YYYYMMDD}.csv`.
   - **Master Index Files**: Original downloaded `master.idx` files are stored in the `DATA_BASE_DIRECTORY/sec_master/` directory.
   - **Final Merged Data**:
     - Pickle format: `DATA_BASE_DIRECTORY/{PROCESSED_LOG_FILE_PREFIX}visiting_details_{start_year_logs}_{end_year_logs}.pkl`

5. **Output Dataset Variable Description** (aligned with Loughran & McDonald's [dataset variables](https://sraf.nd.edu/data/edgar-server-log/)):

   - `date`: The date the form was requested from the SEC site (YYYY-MM-DD).
   - `cik`: SEC-assigned Central Index Key.
   - `accession`: Accession number assigned by the SEC. Each filing has a unique accession number.
   - `nr_total`: Total count of non-robot downloads.
   - `htm`: Web form associated with most filings. Arguably the most relevant format if measuring downloads read by individuals.
   - `txt`: The complete form, including all documents and HTML markup. Typically used for computational consumption of documents.
   - `xbrl`: eXtensible Business Reporting Language files (XML and XSD), used by the filer to provide tables of financial data.
   - `other`: Any form whose file type is not htm, txt, or xbrl.
   - `form`: Form type (e.g., 10-K, 10-Q, 8-K, 4, etc.). The SEC provides [a list of current form types here](https://www.sec.gov/forms), but note that many forms (like the 10-K405) no longer exist but appear in prior years' data.
   - `filing_date`: The date on which the form was filed (YYYY-MM-DD).

## Notes

* The script downloads data based on the SEC website structure (e.g., `https://www.sec.gov/files/edgar{year}.html` for yearly log indexes and `https://www.sec.gov/Archives/edgar/full-index/{year}/{quarter}/master.idx` for master index files). If the SEC changes its website structure, the script may need updates.
* Downloading large amounts of data may take a considerable amount of time and requires a stable internet connection.
* Ensure that the `DATA_BASE_DIRECTORY` has write permissions.

## Authors

Zhizhe Liu and Andrea Tillet
