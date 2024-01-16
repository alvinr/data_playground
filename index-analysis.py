idx_summary = []
idx_ds_summary = []
idx_ri_summary = []
cluster_summary = []
cluster_ram_summary = []

cluster_size_bins = []
cluster_size_labels  = []
cluster_size_summary = []

cluster_ram_size_bins = []
cluster_ram_size_labels  = []
cluster_ram_size_summary = []
cluster_ram_size_summary_ids = []

shard_dits = []

ram_clusters_misses = 0

def process(idxfile, version_match):
  import csv
  import sys
  import numpy as np
  import re

  def lookup(l, k, v, ret, default):
    res = [x[ret] for x in l if x[k] == v]
    if ( len(res) == 0 ):
      global ram_clusters_misses
      ram_clusters_misses += 1
      return default
    else:
      return res[0]

  def update_hist(values, bins, hist):
    l = 0
    for i in range(len(values)):
      for j in range(len(bins)):
        if values[i] > bins[j]:
          continue
        else:
          hist[j-1] += 1
          l = j
          break
    return l

  def convert_to_bytes(size_list):
    byte_sizes = {'b': 1, 'kb': 1024, 'mb': 1024 ** 2, 'gb': 1024 ** 3, 'tb': 1024 ** 4}
    def parse_size(size_str):
      if (len(size_str) == 0):
        return 0
      num_index = len(size_str) - next((num_index for num_index, char in enumerate(size_str) if not (char.isdigit() or char == '.')), len(size_str))
      num, unit = float(size_str[:-num_index]), size_str[-num_index:].lower()
      return int(num * byte_sizes[unit])
    return [parse_size(i) for i in size_list ]

  p = re.compile(version_match, re.IGNORECASE) 
  cluster_details = []
  max_index_size_found=0
  largest_cluster_found=0
  max_ram_found=0

  # Raw index size file
  clusters_examined = 0
  no_indexes_reported = 0
  no_indexes_over_1mb = 0
  not_version_matched = 0
  tp = [str, int, int]
  linenum = 0

  csv.field_size_limit(sys.maxsize)
  records_read = 0

  with open(idxfile, newline='') as f:
    reader = csv.reader(f, delimiter='|', quotechar='"')
    for cluster_id, version, nodes, indexes in reader:

      records_read += 1
      if ( p.match(version) == None ):
        not_version_matched +=1
        continue

      cluster_ram_total = 0
      if ( len(nodes) !=0 ):
        cluster_ram = nodes.split(',')
        cluster_ram = [int(i)/(1024) for i in cluster_ram]
        cluster_ram_total = sum(cluster_ram)

      # Take a string of [[index name, shard count, size];] (e.g. 'metrics-endpoint.metadata_current_default,1,225)' and
      # create three lists of index name, shard count, index size
      
      if ( len(indexes) == 0 ):
        no_indexes_reported += 1
        continue
	
      k = [i for i in [i.split(',') for i in indexes.split(';')[:-1]]]
      k = [i for i in k if len(i) == 3]
      k = [l for l in [[j(k or 0) for j, k in zip(tp, i)] for i in k] if l[2] > 0]

#      k = [l for l in [l for l in [[j(k or 0) for j, k in zip(tp, i)] for i in [i.split(',') for i in indexes.split(';')[:-1]]] if len(l) == 3] if l[2] > 0 ]

      if len(k) == 0:
        no_indexes_over_1mb += 1
        continue

      idx_names, idx_shards, idx_sizes =  [list(i) for i in zip(*k)]
      idx_sizes = [i/(1024) for i in idx_sizes]
 
      clusters_examined += 1
      idx_counts = [0] * len(index_size_bins)
      update_hist(idx_sizes, index_size_bins, idx_counts)
      global idx_summary
      idx_summary = np.add(idx_counts, idx_summary)

      idx_ds_sizes = [idx_sizes[i] for i,v in enumerate(idx_names) if ".ds" in v]
      idx_ri_sizes = [idx_sizes[i] for i,v in enumerate(idx_names) if ".ds" not in v]
      update_hist(idx_ds_sizes, index_size_bins, idx_ds_summary)
      update_hist(idx_ri_sizes, index_size_bins, idx_ri_summary)

      non_zero = [i for i, idx_size in enumerate(idx_counts) if idx_size != 0 ]
      cluster_summary[non_zero[-1]] += 1
      cluster_ram_summary[non_zero[-1]] += cluster_ram_total
      cluster_total = sum(idx_sizes)
      update_hist([cluster_total], cluster_size_bins, cluster_size_summary)
      bin = update_hist([cluster_ram_total], cluster_ram_size_bins, cluster_ram_size_summary)
      cluster_ram_size_summary_ids[bin].append(cluster_id)

      if ( max(idx_sizes) > max_index_size_found ):
        max_index_size_found = max(idx_sizes)
      if ( cluster_total > largest_cluster_found ):
        largest_cluster_found = cluster_total
      if ( cluster_ram_total > max_ram_found ):
        max_ram_found = cluster_ram_total

  print("=== Index Size Distributions")
  print(*index_size_labels[:-1], sep='\t')
  print(*idx_summary[:-1], sep='\t')
  print("=== Cluster Count by Max Index Size, Sum of RAM by max index size")
  print(*index_size_labels[:-1], sep='\t')
  print(*cluster_summary[:-1], sep='\t')
  print(*[round(i) for i in cluster_ram_summary[:-1]], sep='\t')
  print("=== Cluster Index Total Size Distribution")
  print(*cluster_size_labels[:-1], sep='\t')
  print(*cluster_size_summary[:-1], sep='\t')
  print("=== Cluster RAM Total Size Distribution")
  print(*cluster_ram_size_labels[:-1], sep='\t')
  print(*cluster_ram_size_summary[:-1], sep='\t')
  non_zero = [i for i, cluster_ids in enumerate(cluster_ram_size_summary_ids) if len(cluster_ids) != 0 ]
  print("=== Largest Clusters by RAM %s" % cluster_ram_size_labels[non_zero[-1]])
  print(cluster_ram_size_summary_ids[non_zero[-1]])

  print("=== Stats ")
  print("Clusters examined                   %d" % clusters_examined)
  print("Not version matched with '%s'       %d" % (version_match, not_version_matched))
  print("Clusters with no indexes            %d" % no_indexes_reported)
  print("Clusters with no indexes over 1MB   %d" % no_indexes_over_1mb)
  print("Largest Cluster by Total Index Size %d (GB)" % largest_cluster_found)
  print("Largest Index                       %d (GB)" % max_index_size_found)
  print("Largest Cluster by RAM              %d (GB)" % max_ram_found)
  print("RAM clusters misses                 %d" % ram_clusters_misses)
  print("Percentage of Data Streams          %d PCT" % (( sum(idx_ds_summary) / ( sum(idx_ds_summary) + sum(idx_ri_summary)))*100) )

def plot():
  import pandas as pd
  import matplotlib.pyplot as plt
  import numpy as np

  def calc_by_pct(source):
    total=sum(source)
    pct_of_total = [round(i/total*100, 2) for i in source]
    return pct_of_total


  def plot_pct_stacked(labels, values, value_names, title, xlabel, ylabel, color=['limegreen', 'orange']):
    cols = []
    d = { 'X': labels[:-1] }
    for i in range(len(values)):
      lbl = 'Y' + str(i)
      d[lbl] = values[i][:-1]
      cols.append(lbl)

    df = pd.DataFrame(d,  index=labels[:-1])
    df['Total'] = df[cols].sum(axis=1)
    df[cols] = df[cols].div(df[cols].sum(axis=1), axis=0).multiply(100)
    ax = df.plot(kind='bar', stacked=True, figsize=(12, 10), title=title, y=cols, xlabel=xlabel, ylabel=ylabel, legend=False, color=color, edgecolor='white', linewidth=1.75)
    for c in ax.containers:
      xlabels = [f'{w:.0f}%' if (w := v.get_height()) > 0 else '' for v in c ]
      ax.bar_label(c, labels=xlabels, label_type='center', rotation=90, padding=5, fmt='{:,.0f}%')
    ax.bar_label(ax.containers[-1], labels=df['Total'], label_type='edge', rotation=45, padding=5)
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.legend(value_names);
    ax.margins(y=0.1)
    
    return df

  def plot_hist(labels, values, title, xlabel, ylabel, asPct=False, runningTot=False, color=['blue', 'orange']):
    cols = ['left']
    plot_vals = values
    secondary = False

    if ( asPct == True ):
      plot_vals = calc_by_pct(values)

    contents = { 'X': labels[:-1], 'left': plot_vals[:-1] }

    if ( runningTot == True ):
      rt = np.cumsum(plot_vals)     
      contents.update({ 'right': rt[:-1] })
      cols.append('right')
      secondary = True

    df = pd.DataFrame(contents, index=labels[:-1])
#    if ( asPct == True ):
#      df[cols] = df[cols].div(df[cols].sum(axis=1), axis=0).multiply(100)

    ax = df.plot(kind='bar', figsize=(12, 10), title=title, y=cols, xlabel=xlabel, ylabel=ylabel, legend=False, color=color, edgecolor='white', linewidth=1.75)
    rot = 90
    if ( secondary != True ):
      rot = 45
    ax.bar_label(ax.containers[0], label_type='edge', rotation=rot, padding=5)
    ax.set_xticklabels(df['X'], rotation=90, ha='right')
    ax.margins(y=0.1)

    if ( secondary == True ):
      ax.bar_label(ax.containers[1], label_type='edge', rotation=45, padding=5, fmt='{:,.0f}%')

    return df

  plot_hist(index_size_labels, idx_summary, 'Index Count Distribution by Index Size', 'Index Size (GB)', 'Index Count')
  plot_hist(index_size_labels, cluster_summary, 'Cluster Count by Max Index Size', 'Max Index Size (GB) in cluster', 'Cluster Count')
  plot_hist(cluster_size_labels, cluster_size_summary, 'Cluster Size Distribution', 'Cluster Size (TB)', 'Cluster Count')
  plot_hist(cluster_ram_size_labels, cluster_ram_size_summary, 'Cluster RAM Size Distribution', 'Cluster RAM Size (GB)', 'Cluster Count')
  plot_hist(index_size_labels, cluster_ram_summary, 'Cluster RAM total by Cluster Max Index Size', 'Max Index Size (GB) in cluster', 'RAM (GB)')
  plot_hist(index_size_labels, idx_summary, 'Index Size Distribution (as percentage) across all Clusters', 'Index Size (GB)', 'Percentage', True, color=['orange'])
  plot_hist(index_size_labels, cluster_ram_summary, 'Cluster RAM Size Distribution (as percentage) by Largest Index in Cluster', 'Index Size (GB)', 'Percentage', True, True)
  plot_hist(cluster_ram_size_labels, cluster_ram_size_summary, 'Cluster Count (as percentage) by RAM Size', 'Cluster RAM Size (GB)', 'Percentage', True)
  plot_hist(index_size_labels, cluster_summary, 'Cluster Count Distribution (as percentage) by Cluster Max Index Size', 'Max Index Size (GB) in cluster', 'Percentage', True, 
            color=['green'])
  plot_pct_stacked(index_size_labels, [idx_ds_summary, idx_ri_summary], ["Data Streams", "Regular Indexes"], "Data Streams vs. Regular Indexes by Index Size as Percentage (with counts)", 'Index Size (GB)', 'Percentage')

  plt.show()

def doit():
  import argparse

  parser=argparse.ArgumentParser()
  parser.add_argument("--idxfile", help="Like the file to load", default="cat_indices_output.txt")
  parser.add_argument("--numidxbuckets", help="Number of Index Buckets", default=8, type=int)
  parser.add_argument("--idxbucketsize", help="Size of Index Buckets", default=30, type=int)
  parser.add_argument("--version", help="Version to filter on", default="")
  parser.add_argument("--numshardbuckets", help="NUmber of Shard Buckets", default="10", type=int)
  args=parser.parse_args()

  import numpy as np

  global index_size_bins 
  global index_size_labels
  global idx_summary
  global idx_ds_summary
  global idx_ri_summary
  global cluster_summary
  global cluster_ram_summary

  global cluster_size_bins
  global cluster_size_labels
  global cluster_size_summary

  global cluster_ram_size_bins
  global cluster_ram_size_labels
  global cluster_ram_size_summary
  global cluster_ram_size_summary_ids

  index_size_bins = [0, 1] + [int(x)*args.idxbucketsize for x in range(1,args.numidxbuckets)] + [np.inf]
  index_size_labels = [">"+ str(x)+"GB" for x in index_size_bins]
  index_size_labels[0] = ">1MB"
  index_size_labels[-1] = ">" + str(index_size_bins[-2] + args.idxbucketsize) + "GB"
  idx_summary = [0] * len(index_size_bins) 
  idx_ds_summary = [0] * len(index_size_bins)
  idx_ri_summary = [0] * len(index_size_bins)
  cluster_summary = [0] * len(index_size_bins)
  cluster_ram_summary = [0] * len(index_size_bins)

  cluster_bucket_size = 50
  cluster_size_bins = [0] + [int(x)*cluster_bucket_size for x in range(1,20)] + [np.inf]
  cluster_size_labels  = [">"+ str(x)+"GB" for x in cluster_size_bins]
  cluster_size_labels[-1] = ">" + str(cluster_size_bins[-2] + cluster_bucket_size) + "GB"
  cluster_size_labels[0] = ">0GB"
  cluster_size_summary = [0] * len(cluster_size_bins)

  cluster_ram_size_bins = [0] + [int(x)*args.idxbucketsize for x in range(1,10)] + [np.inf]
  cluster_ram_size_labels  = [">"+ str(x)+"GB" for x in cluster_ram_size_bins]
  cluster_ram_size_labels[-1] = ">" + str(cluster_ram_size_bins[-2] + args.idxbucketsize) + "GB"
  cluster_ram_size_labels[0] = ">0GB"
  cluster_ram_size_summary = [0] * len(cluster_ram_size_bins)
  cluster_ram_size_summary_ids = [ [ ] for j in range(len(cluster_ram_size_bins))] 

  shard_dist = [ [ [ ] for j in range(len(index_size_bins)) ] for k in range(args.numshardbuckets) ]

  process(args.idxfile, args.version)
  plot()

if __name__ == '__main__':
    doit()

