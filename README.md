# data_playground
Playing with data and data visualization

## get_cat_indices.sh
This script collects the index stats from a list of cluster ids. The cluser ids are probvided in a newline sepearted file.


### Pre-preqs

1. Install [Cloud CLI](https://github.com/elastic/cloud-cli) for your platform
1. Get your [API Key](https://admin.found.no/keys)
1. Set the following Environment variables
   `export EC_API_KEY=<your key>`
   `export EC_HOST=https://admin.found.no`

### Usage
```
./get_cat_indices.sh <file with line delimiated cluster Ids>
```
## index-analysis.py

### Pre-reqs

1. Install packages
```
pip install -r requirement.txt
```


### Usage
```
python index-analysis.py --idxfile <index stats from get_cat_indices.sh>  --ramfile <file with cluster memory usage>
```
