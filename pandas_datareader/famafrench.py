import tempfile
import numpy as np
from pandas.io.common import urlopen, ZipFile
from pandas.compat import lmap
from pandas import DataFrame


_FAMAFRENCH_URL = 'http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp'


def get_data_famafrench(name):
    # path of zip files
    zip_file_path = '{0}/{1}_TXT.zip'.format(_FAMAFRENCH_URL, name)

    with urlopen(zip_file_path) as url:
        raw = url.read()

    with tempfile.TemporaryFile() as tmpf:
        tmpf.write(raw)

        with ZipFile(tmpf, 'r') as zf:
            data = zf.open(zf.namelist()[0]).readlines()

    line_lengths = np.array(lmap(len, data))
    file_edges = np.where(line_lengths == 2)[0]

    datasets = {}
    edges = zip(file_edges + 1, file_edges[1:])
    for i, (left_edge, right_edge) in enumerate(edges):
        dataset = [d.split() for d in data[left_edge:right_edge]]
        if len(dataset) > 10:
            ncol_raw = np.array(lmap(len, dataset))
            ncol = np.median(ncol_raw)
            header_index = np.where(ncol_raw == ncol - 1)[0][-1]
            header = dataset[header_index]
            ds_header = dataset[header_index + 1:]
            # to ensure the header is unique
            header = ['{0} {1}'.format(j, hj) for j, hj in enumerate(header,
                                                                     start=1)]
            index = np.array([d[0] for d in ds_header], dtype=int)
            dataset = np.array([d[1:] for d in ds_header], dtype=float)
            datasets[i] = DataFrame(dataset, index, columns=header)

    return datasets
