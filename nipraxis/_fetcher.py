""" Fetch data from repository, or maybe local cache
"""

import pkg_resources

import unscrewed

_config_file = pkg_resources.resource_filename("nipraxis", "registry.yaml")
_fetcher = unscrewed.Fetcher(_config_file)
fetch_file = _fetcher.fetch_file
