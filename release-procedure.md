*   Tag commit

        git tag -a x.x.x -m 'Version x.x.x'

*   and push to github

        git push pandas-datareader main --tags

*  Upload to PyPI

        git clean -xfd
        python -m build
        twine upload dist/*

*  Do a pull-request to the feedstock on [pandas-datareader-feedstock](https://github.com/conda-forge/pandas-datareader-feedstock/)

        update the version
        update the SHA256 (retrieve from PyPI)
