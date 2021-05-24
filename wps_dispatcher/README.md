# WPS Dispatcher

This section of the code base implements a requests + polling based
WPS dispatch mechanism to process subsections of the wetlands being
bulk analysed at a time.

Please see internal docs for design details.

## Development setup

To run locally using Jupyter Notebooks follow these steps.
- Create a conda environment : `conda create -n wit python=3.8`
- Activate the environment : `conda activate wit`
- Install Jupyter : `pip install jupyter tqdm`
- Install GDAL : `conda install gdal`