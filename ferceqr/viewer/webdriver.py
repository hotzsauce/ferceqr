"""
Querying the FERC EQR Report Viewer webpage.

It's dynamically rendered so we have to use a `Selenium/WebDriver`-based
approach to download data, rather than using `requests`. Executive decision
has been made to just use the Chrome webdriver
"""
from __future__ import annotations

from ferceqr.viewer.config import ViewerConfig
import pathlib
import re
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

_HEADLESS = True # run browser invisibly (no GUI windows)
_QUARTERLY_REQUEST_PATTERN = re.compile(r"(\d{4}) Q([1-4])", re.IGNORECASE)
_QUARTERLY_FILENAME_PATTERN = re.compile(
    r"^(CSV|XML)_(\d{4})_Q[1-4]\.zip$",
    re.IGNORECASE,
)



class FercEqrFilings(object):

    def __init__(
        self,
        config: [ViewerConfig | dict],
        target_dir: str | pathlib.Path = "",
    ):
        if isinstance(config, ViewerConfig):
            self.config = config
        else:
            self.config = ViewerConfig.from_dict(config)

        self.options = Options()
        if _HEADLESS:
            self.options.add_argument("--headless=new")

        # should this be set in the `ViewerConfig`?...
        if target_dir:
            self.target_dir = pathlib.Path(str(target_dir)).resolve()
        else:
            self.target_dir = (pathlib.Path.cwd() / "eqr_data").resolve()

        prefs = {
            "download.default_directory": str(self.target_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        }
        self.options.add_experimental_option("prefs", prefs)

        self.service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(
            service=self.service,
            options=self.options,
        )

    def download(
        self,
        request: str,
        format: str = "csv",
    ):
        """
        Download a CSV or XML from the FERC EQR page
        """
        maybe_match = _QUARTERLY_REQUEST_PATTERN.match(request)
        if not maybe_match:
            raise ValueError("unrecognized 'request' format:", request)

        year, quarter = maybe_match.groups()
        qf = self._download_quarterly_filing(year, quarter, format)
        print(qf)


    def view(
        self,
        filings: "quarterly",
        *args,
        **kwargs,
    ):
        if filings == "quarterly":
            self._get_quarterly_filing_links(*args, **kwargs)
        else:
            raise ValueError("unrecognized value of 'filings':", filings)

    #
    # private helper methods
    #

    def _click_tab(self, text: str):
        """
        Click a tab on the current page
        """
        tab = (
            # this waits for up to 10 seconds until the element is clickable
            WebDriverWait(self.driver, 10)
            .until(
                EC
                .element_to_be_clickable((
                    By.XPATH,
                    f"//a[normalize-space()='{text}']",
                ))
            )
        )
        tab.click()

    def _collect_zip_links(
        self,
        pattern: str,
    ) -> List[str]:
        """
        Collect all the visible zip anchors on the current page
        """
        _ = (
            # wait until the table loads
            WebDriverWait(self.driver, 10)
            .until(
                EC
                .presence_of_element_located((
                    By.XPATH,
                    "//a[contains(translate(., 'ZIP', 'zip'), '.zip')]",
                ))
            )
        )

        anchors = (
            self.driver
            .find_elements(
                By.XPATH,
                (
                    "//a[contains(translate(@href, 'ZIP', 'zip'), '.zip') or "
                    "contains(translate(., 'ZIP', 'zip'), '.zip')]"
                )
            )
        )

        urls = []
        for a in anchors:
            href = a.get_attribute("href")
            text = (a.text or "").strip()

            # prefer the filename from either the href or the text
            candidate = pathlib.Path(href).name if href else text
            if not candidate:
                continue
            if pattern.match(candidate):
                urls.append(href)

        # de-duplicate but preserve order
        seen = set()
        output = []
        for url in urls:
            if url and url not in seen:
                output.append(url)
                seen.add(url)

        return output

    def _download_element_at_url(
        self,
        url: str,
        out_name: str,
    ):
        """
        Download a CSV or XML file
        """
        print(f"Downloading \n{url}")
        out_path = self.target_dir / out_name
        session = requests.Session()

        response = session.get(url, stream=True)
        response.raise_for_status()

        # get file size
        file_size = None
        if 'content-length' in response.headers:
            file_size = int(response.headers['content-length'])
            print(f"   Size: {file_size:,} bytes ({file_size/1024/1024:.1f} MB)")

        # download with progress
        mb_128 = 128 * 1_024 * 1_024
        with open(out_path, 'wb') as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)

                    if file_size and downloaded % mb_128 == 0: # Update every 128MB 
                        percent = (downloaded / file_size) * 100
                        print(f"   Progress: {percent:.1f}%")

    def _download_quarterly_filing(
        self,
        year: int | str,
        quarter: int | str,
        format: str,
    ):
        desired_pattern = re.compile(
            rf"{format.upper()}_{year}_Q{quarter}\.zip$",
            re.IGNORECASE,
        )
        out_name = f"{format.lower()}_{year}_q{quarter}.zip"

        self.target_dir.mkdir(exist_ok=True, parents=True)
        for link in self._get_quarterly_filing_links():
            maybe_match = desired_pattern.search(link)
            if maybe_match:
                return self._download_element_at_url(link, out_name)

        raise FileNotFoundError(
            f"No '{format}' file found for {year} q{quarter}"
        )

    def _get_quarterly_filing_links(self, *args, **kwargs):
        """
        View quarterly filings
        """
        self.driver.get(self.config.root)
        self._click_tab("Downloads")
        try:
            self._click_tab("Quarterly Filings")
        except Exception:
            import warnings
            warnings.warn("Quarterly Filings tab is unclickable")

        links = self._collect_zip_links(_QUARTERLY_FILENAME_PATTERN)
        if not links:
            raise SystemExit(
                "No ZIP links found on the Downloads -> Quarterly "
                "Filings tab"
            )

        return links
