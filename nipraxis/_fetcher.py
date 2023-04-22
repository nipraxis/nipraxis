""" Fetch data from repository, or maybe local cache
"""

import os
from pathlib import Path

import yaml

import pooch


def get_registry(config):
    """ Build and return Pooch image registry object
    """
    return pooch.create(
        # Use the default cache folder for the operating system
        path=pooch.os_cache(config['pkg_name']),
        # The remote data is on Github
        base_url=config['base_url'],
        version=config['data_version'],
        # If this is a development version, get the data from the "main" branch
        version_dev=config.get('version_dev', 'main'),
        registry=config['files'],
        # The name of an environment variable that can overwrite the cache path.
        env=config.get('cache_env_var')
    )


with open(Path(__file__).parent / 'registry.yaml') as fobj:
    CONFIG = yaml.load(fobj, Loader=yaml.SafeLoader)
REGISTRY = get_registry(CONFIG)


def from_staging_cache(rel_url, staging_cache):
    known_hash = REGISTRY.registry.get(rel_url)
    if not known_hash:
        return None
    data_version = CONFIG['data_version']
    pth = Path(staging_cache).resolve() / data_version / rel_url
    action, verb = pooch.core.download_action(pth, known_hash)
    if action == 'update':
        pooch.utils.get_logger().info(
            f"'{rel_url}' in '{staging_cache}/{data_version}' "
            "but hash does not match; looking in local cache / registry.")
        return None
    if action == 'fetch':
        return str(pth)


def fetch_file(rel_url):
    """ Fetch data file from local cache, or registry

    Parameters
    ----------
    rel_url : str
        Location of file to fetch, relative to repository base URLs.  Use
        forward slashes to separate paths, on Windows or Unix.

    Returns
    -------
    local_fname : str
        The absolute path (including the file name) of the file in the local
        storage.
    """
    staging_cache = os.environ.get(CONFIG.get('staging_env_var'))
    if staging_cache:
        cache_fname = from_staging_cache(rel_url, staging_cache)
        if cache_fname:
            return cache_fname
    return REGISTRY.fetch(rel_url)
