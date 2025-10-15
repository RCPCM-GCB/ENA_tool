from setuptools import setup
from os import path

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), "r") as fh:
    long_description = fh.read()

setup(
    name='ENATool',
    description='Convenient downloader of raw files from ENA',
    long_description=long_description,
    long_description_content_type="text/markdown",
    version="0.0.1a",
    license='MIT',
    author='P.Tikhonova',
    author_email='tikhonova.polly@mail.ru',
    url='https://github.com/RCPCM-GCB/ENA_tool',
    packages=[
        'ENATool'
    ],
    package_data={'': ['README.md']},
    install_requires=[
        'pandas>=0.23.4',
        'requests',
        'tqdm',
        'numpy'
    ],
)