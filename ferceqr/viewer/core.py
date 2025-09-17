"""
Implementing a "snapshot" of the FERC EQR Report Viewer page
"""
from __future__ import annotations

from ferceqr.viewer.config import ViewerConfig
from ferceqr.viewer.webdriver import FercEqrFilings



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

    def download(self, *args, **kwargs):
        """
        Download a CSV or XML from the ERC EQR page
        """
        target_dir = kwargs.pop("target_dir", "")
        filing = FercEqrFilings(self.config, target_dir)
        filing.download(*args, **kwargs)
