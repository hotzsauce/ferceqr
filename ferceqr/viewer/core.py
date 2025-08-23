"""
Implementing a "snapshot" of the FERC EQR Report Viewer page
"""
from __future__ import annotations

from bs4 import BeautifulSoup
from ferceqr.viewer.config import ViewerConfig
import re
import requests



class ReportViewer(object):

    def __init__(
        self,
        config: [ViewerConfig | dict] = None,
    ):
        if config is None:
            self.config = ViewerConfig()
        elif isinstance(config, ViewerConfig):
            self.config = config
        else:
            self.config = ViewerConfig.from_dict(config)

    def read_page(self):
        response = requests.get(self.config.root)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        soup_str = str(soup)

        with open("soup.txt", "w") as file:
            file.write(soup_str)

        # Look for anchors whose VISIBLE TEXT is like CSV_2019_Q3.zip or XML_2019_Q3.zip
        pat = re.compile(r"^(CSV|XML)_(\d{4})_Q([1-4])\.zip$", re.I)
        links = []
        for a in soup.find_all("a", href=True):
            text = (a.get_text() or "").strip()
            m = pat.match(text)
            if m:
                fmt, year, q = m.group(1).upper(), int(m.group(2)), int(m.group(3))
                links.append({
                    "format": fmt,
                    "year": year,
                    "quarter": q,
                    "text": text,
                    "url": urljoin(BASE, a["href"]),  # resolve relative → absolute
                })
        print(links)


if __name__ == "__main__":
    rv = ReportViewer()
    rv.read_page()
