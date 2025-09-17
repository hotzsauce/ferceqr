# FERC EQR

A toolkit for ingesting and managing FERC's Electric Quarterly Reports data

# Installing

All of these require having a Github SSH key somewhere on your system.

## uv

```bash
>>> uv init ferc_project
>>> cd ferc_project
>>> uv add git+ssh://git@github.com/hotzsauce/ferceqr.git --branch main
```

## poetry

```bash
>>> poetry new ferc_project
>>> cd new_project
>>> eval $(poetry env activate)
>>> poetry add git+ssh://git@github.com/hotzsauce/ferceqr.git@main
```

## pip

```bash
>>> python3 -m venv venv
>>> source venv/bin/activate
>>> python3 -m pip install "git+ssh://git@github.com/hotzsauce/ferceqr.git@main#egg=ferceqr"
```

After installing `ferceqr` via one of these routes, you'll be able to treat it
like any other python package. Just add it into the block of imports:
```python
...
import io

import ferceqr
import numpy as np
import pandas as pd
...
```

# Usage

See the `example.py` script to see an example of how to use the module.
