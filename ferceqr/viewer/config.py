"""
Global variable manager for EQR viewer-related things
"""
from __future__ import annotations

_ROOT_URL = "https://eqrreportviewer.ferc.gov"



class ViewerConfig(object):

    def __init__(
        self,
        root: str = _ROOT_URL,
    ):
        self.root = root

    @classmethod
    def from_dict(cls, dictionary: dict) -> ViewerConfig:
        defaults = {
            "root": _ROOT_URL,
        }
        defaults.update(dictionary)
        return ViewerConfig(**defaults)
