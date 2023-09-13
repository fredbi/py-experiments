#! /bin/bash
# pip install pdoc
# -e module=url, --edit-url module=url
#                        A mapping between module names and URL prefixes, used to display an 'Edit' button. May be passed multiple times.
#                        Example: pdoc=https://github.com/mitmproxy/pdoc/blob/main/pdoc/ (default: [])

PYTHONPATH=$(pwd) pdoc etl --docformat markdown --no-browser -o docs
