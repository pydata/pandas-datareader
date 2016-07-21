#!/usr/bin/env python

import click

import os
import requests_cache
import pandas as pd
import pandas_datareader as pdr


def init_session(cache_name, cache_backend, expire_after):
    if expire_after == '0':
        expire_after = None
        print("expire_after==0 no cache")
        return None
    else:
        if expire_after == '-1':
            expire_after = 0
            print("Installing cache '%s.sqlite' without expiration" % cache_name)
        else:
            expire_after = pd.to_timedelta(expire_after, unit='s')
            print("Installing cache '%s.sqlite' with expire_after=%s (d days hh:mm:ss)" % (cache_name, expire_after))
        session = requests_cache.CachedSession(cache_name=cache_name, backend=cache_backend, expire_after=expire_after)
        return session


def save(data, filename):
    if filename != '':
        filename_wo_ext, file_extension = os.path.splitext(filename)
        file_extension = file_extension.lower()
        if file_extension == '.csv':
            data.to_csv(filename)
        elif file_extension in ['.xls', '.xlsx']:
            data.to_excel(filename)
        else:
            raise(NotImplementedError("Unsupported output to '%s'" % filename))


@click.command()
@click.option('--max_rows', default=20, help='Maximum number of rows displayed')
@click.option('--cache_name', default='', help="Cache name (file if backend is sqlite)")
@click.option('--cache_backend', default='sqlite', help="Cache backend - default 'sqlite'")
@click.option('--expire_after', default='24:00:00.0', help=u"Cache expiration (0: no cache, -1: no expiration, d: d seconds expiration cache)")
@click.option('--names', default='F', help='Name of the dataset. Some data sources (yahoo, google, fred) will accept a list of names. (use coma (,) to give several names')
@click.option('--data_source', default='yahoo', help='the data source ("yahoo", "yahoo-actions", "yahoo-dividends", "google", "fred", "ff", or "edgar-index")')
@click.option('--start', default='', help='Start')
@click.option('--end', default='', help='End')
@click.option('--filename', default='', help='Filename output')
def main(max_rows, cache_name, cache_backend, expire_after, names, data_source, start, end, filename):
    pd.set_option('max_rows', max_rows)
    if cache_name == '':
        cache_name = "requests_cache"
    else:
        if cache_backend.lower() == 'sqlite':
            cache_name = os.path.expanduser(cache_name)

    if ',' in names:
        names = names.split(',')

    session = init_session(cache_name, cache_backend, expire_after)

    data = pdr.DataReader(names, data_source, start, end, session=session)
    print(data)

    save(data, filename)

if __name__ == "__main__":
    main()
