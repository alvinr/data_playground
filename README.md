# data_playground
Playing with data and data visualization

## get_cat_indices.sh
This script collects the index stats from a list of cluster ids. The cluser ids are provided in a newline seperated file.


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

## para_cat.sh
The script takes file of cluster ids, spilts the file into 10 segments and then runs get_cat_indices.sh in paralel

### Pre-reqs
None

## Usage
```
./para_cat.sh <file with line delimiated cluster Ids>
```

## index-analysis.py
This python script takes the output of `get_cat_indices.sh` and performs the analysis of the data nd generates a set of graphs for visualizations. Thsi script allows changes for the number and sizes of buckets from the command line.

### Pre-reqs

1. Install packages
```
pip install -r requirement.txt
```

### Usage
```
python index-analysis.py --idxfile <index stats from get_cat_indices.sh> 
```
