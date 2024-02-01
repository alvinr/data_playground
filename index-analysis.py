idx_summary = []
idx_ds_summary = []
idx_ri_summary = []
ds_summary = []
ds_counts = []
cluster_summary = []
cluster_ram_summary = []
cluster_ram_summary_by_shard = []


cluster_size_bins = []
cluster_size_labels  = []
cluster_size_summary = []

cluster_ram_size_bins = []
cluster_ram_size_labels  = []
cluster_ram_size_summary = []

shard_dist = []
shard_dist_labels = []
shard_size_dist = []


def process(args):
  import csv
  import sys
  import numpy as np
  import re

  def update_hist(values, bucket_size, hist, countOnly=True):
    max_cols = len(hist)-1
    for i in range(len(values)):
      bucket, rem = divmod(values[i], bucket_size)
      bucket = int(min(bucket, max_cols))
      if (countOnly == True):
        hist[bucket] += 1
      else:
        hist[bucket] += values[i]

  def update_largest(stat, values, labels):
    max_val = max(values, default=0)
    if ( max_val > stat[1]):
      max_index = values.index(max(values, default=0))
      stat[0] = labels[max_index]
      stat[1] = max_val

  # regex for index names for data stream in the followqing pattern
  # [partial-][restored-].ds-<data-stream-name>-YYYY.mm.dd-000001
  ds_pattern = re.compile(
        r'(?P<partial>partial-)?'
        r'(?P<restored>restored-)?'
        r'(?P<prefix>\.ds-)'
        r'(?P<name>.*)-'
        r'(?P<suffix>\d{4}\.\d{2}\.\d{2}-\d{6})'
    ,re.IGNORECASE)
  ver_match = re.compile(args.version, re.IGNORECASE)

  cluster_details = []
  max_index_size_found=['name', 0]
  largest_cluster_found=['name', 0]
  max_ram_found=['name', 0]
  largest_number_of_shards=['name', 0]
  largest_ds=['name', 0]

  # Raw index size file
  clusters_examined = 0
  no_indexes_reported = 0
  no_indexes_over_1mb = 0
  not_version_matched = 0
  tp = [str, int, int]
  linenum = 0

  csv.field_size_limit(sys.maxsize)
  records_read = 0

  with open(args.idxfile, newline='') as f:
    reader = csv.reader(f, delimiter='|', quotechar='"')
    for cluster_id, version, nodes, indexes in reader:

      records_read += 1
      if ( ver_match.match(version) == None ):
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
      if args.exclude_ds == True:
        k = [v for i,v in enumerate(k) if re.match(ds_pattern, v[0]) is None]
      k = [l for l in [[j(k or 0) for j, k in zip(tp, i)] for i in k] if l[2] > 0]

      # This is the above three lines in one line, harder to debug!
      # k = [l for l in [l for l in [[j(k or 0) for j, k in zip(tp, i)] for i in [i.split(',') for i in indexes.split(';')[:-1]]] if len(l) == 3] if l[2] > 0 ]

      if len(k) == 0:
        no_indexes_over_1mb += 1
        continue

      idx_names, idx_shards, idx_sizes =  [list(i) for i in zip(*k)]
      idx_sizes = [i/(1024) for i in idx_sizes]

      clusters_examined += 1
      idx_counts = [0] * len(index_size_bins)
      update_hist(idx_sizes, args.idxbucketsize, idx_counts)
      global idx_summary
      idx_summary = np.add(idx_counts, idx_summary)

      idx_ds_sizes = [idx_sizes[i] for i,v in enumerate(idx_names) if ".ds" in v]
      idx_ri_sizes = [idx_sizes[i] for i,v in enumerate(idx_names) if ".ds" not in v]
      update_hist(idx_ds_sizes, args.idxbucketsize, idx_ds_summary)
      update_hist(idx_ri_sizes, args.idxbucketsize, idx_ri_summary)

      # Compute the size of the Data Streams based on the Data Stream name embeeded into the index names
      ds_idx_sizes = {}
      for i in range(len(idx_names)):
        match = re.match(ds_pattern, idx_names[i])
        if match is None:
          continue
        ds_name = match.groups()[-2]
        if (ds_idx_sizes.get(ds_name) is not None):
          ds_idx_sizes[ds_name] += round(idx_sizes[i])
        else:
          ds_idx_sizes[ds_name] = round(idx_sizes[i])
      update_hist(list(ds_idx_sizes.values()), args.idxbucketsize, ds_summary, countOnly=False)
      update_hist(list(ds_idx_sizes.values()), args.idxbucketsize, ds_counts)

      non_zero = [i for i, idx_size in enumerate(idx_counts) if idx_size != 0 ]
      cluster_summary[non_zero[-1]] += 1
      cluster_ram_summary[non_zero[-1]] += cluster_ram_total
      cluster_total = sum(idx_sizes)
      update_hist([cluster_total], args.clusterbucketsize, cluster_size_summary)
      update_hist([cluster_ram_total], args.idxbucketsize, cluster_ram_size_summary)
      update_hist([cluster_ram_total], args.idxbucketsize, cluster_ram_size_summary)

      for i in range(len(idx_shards)):
        norm_i = min(idx_shards[i], len(shard_dist))-1
        update_hist([idx_sizes[i]], args.idxbucketsize, shard_dist[norm_i])
        update_hist([round(idx_sizes[i])], args.idxbucketsize, shard_size_dist[norm_i], countOnly=False)

      max_shard = min(args.numshardbuckets, max(idx_shards)-1)
      update_hist([round(cluster_ram_total)], args.idxbucketsize, cluster_ram_summary_by_shard[max_shard], countOnly=False)

      update_largest(max_index_size_found, idx_sizes, idx_names)
      update_largest(largest_cluster_found, [cluster_total], [cluster_id])
      update_largest(max_ram_found, [cluster_ram_total], [cluster_id])
      update_largest(largest_number_of_shards, idx_shards, idx_names)
      update_largest(largest_cluster_found, [cluster_total], [cluster_id])
      update_largest(largest_ds, list(ds_idx_sizes.values()), list(ds_idx_sizes.keys()))


  print("=== Index Size Distributions")
  print(*index_size_labels, sep='\t')
  print(*idx_summary, sep='\t')
  print("=== Cluster Count by Max Index Size, Sum of RAM by max index size")
  print(*index_size_labels, sep='\t')
  print(*cluster_summary, sep='\t')
  print(*[round(i) for i in cluster_ram_summary], sep='\t')
  print("=== Cluster Index Total Size Distribution")
  print(*cluster_size_labels, sep='\t')
  print(*cluster_size_summary, sep='\t')
  print("=== Cluster RAM Total Size Distribution")
  print(*cluster_ram_size_labels, sep='\t')
  print(*cluster_ram_size_summary, sep='\t')
  print("=== Cluster Count by Max Index Size by Shard")
  print(*list(["Shard"] + index_size_labels), sep='\t')
  [ print(*[shard_dist_labels[i], *v], sep='\t') for i,v in enumerate(shard_dist) ]
  print("=== Total Index size by Index Bucket by Shard (GB)")
  print(*list(["Shard"] + index_size_labels), sep='\t')
  [ print(*[shard_dist_labels[i], *v], sep='\t') for i,v in enumerate(shard_size_dist) ]
  print("=== Counts and Total Data Stream Size by Index Bucket (GB)")
  print(*index_size_labels, sep='\t')
  print(*ds_counts, sep='\t')
  print(*ds_summary, sep='\t')
  print("=== Cluster Ram by Max Index Size by Shard")
  print(*list(["Shard"] + index_size_labels), sep='\t')
  [ print(*[shard_dist_labels[i], *v], sep='\t') for i,v in enumerate(cluster_ram_summary_by_shard) ]

  print("=== Stats ")
  print("Records examined                    %d" % records_read)
  print("Clusters examined                   %d" % clusters_examined)
  print("Not version matched with '%s'       %d" % (args.version, not_version_matched))
  print("Clusters with no indexes            %d" % no_indexes_reported)
  print("Clusters with no indexes over 1MB   %d" % no_indexes_over_1mb)
  print("Largest Cluster by Total Index Size %d (GB) %s" % (largest_cluster_found[1], largest_cluster_found[0]))
  print("Largest Index                       %d (GB) %s" % (max_index_size_found[1], max_index_size_found[0]))
  print("Largest Cluster by RAM              %d (GB) %s" % (max_ram_found[1], max_ram_found[0]))
  print("Percentage of Data Streams          %d PCT" % (( sum(idx_ds_summary) / ( sum(idx_ds_summary) + sum(idx_ri_summary)))*100) )
  print("Largest number of shards            %d %s" % (largest_number_of_shards[1], largest_number_of_shards[0]))
  print("Largest Data Stream                 %d (GB) %s" % (largest_ds[1], largest_ds[0]))


def plot():
  import pandas as pd
  import matplotlib.pyplot as plt
  import numpy as np

  def plot_pct_pie(labels, values, value_names, title, xlabel, ylabel, color='', colormap='Paired', transpose=False, asOverallPct=False):
    cols = []
    rot = 90
    d = { }
    for i in range(len(values)):
      lbl = value_names[i]
      d[lbl] = values[i]

    df = pd.DataFrame(d, index=labels)
    if transpose == True:
      df = df.transpose()
    # Exclude columns where all values are zero (to avoid div by zero errors)
    non_zero_cols = (df != 0).any()
    cols = non_zero_cols.index[non_zero_cols].tolist()
    if (asOverallPct == False):
      df[cols] = df[cols].div(df[cols].sum(axis=1), axis=0).multiply(100)
    else:
      overall_total = df.sum(numeric_only=True).sum()
      df = (df / overall_total) * 100

    df = df.fillna(0)

    slice_labels = list(df.columns)
    slices = (list(df.index))

    plot_cols = 3
    plot_rows, rem = divmod(len(slices), plot_cols)
    plot_rows+= 1 if rem !=0 else 0

    figure, axis = plt.subplots(nrows=plot_rows, ncols=plot_cols, figsize=( 4*plot_rows, 4*plot_cols))
    for i in range(plot_rows):
      for j in range(plot_cols):
        axis[i, j].axis('off')

    figure.suptitle(title)
    for i in range(len(slices)):
      use_row, use_col = divmod(i, plot_cols)
      df_slice = df.loc[slices[i]]
      if sum(df_slice) < 1:
        continue
      zero_rows = (df_slice < 1)
      rm_rows = zero_rows.index[zero_rows].tolist()
      for j in range(len(rm_rows)):
        df_slice = df_slice.drop(rm_rows[j], axis=0)
      explode = list([0.1 + (i/20) for i in range(len(list(df_slice.index)))])
      ax_slice  = df_slice.plot(ax=axis[use_row, use_col], ylabel="", kind='pie', figsize=(12,12), autopct='%1.0f%%', colormap=colormap,
                                explode=explode, startangle=270, rotatelabels=True)
      ax_slice.set_title(slices[i], loc='left', pad=5)
      ax_slice.margins(x=5, y=5)

    return df


  def plot_stacked(labels, values, value_names, title, xlabel, ylabel, color=['limegreen', 'orange'], colormap='', transpose=False, asPct=True, asOverallPct=False):
    cols = []
    rot = 90
    d = {}
    for i in range(len(values)):
      lbl = value_names[i]
      d[lbl] = values[i]
      cols.append(lbl)

    if len(values) > 5:
      rot = 0

    df = pd.DataFrame(d,  index=labels)
    if transpose == True:
      df = df.transpose()
      cols = list(df.columns)

    fmt='{:,.0f}%'
    df['Total'] = df[cols].sum(axis=1)
    if ( asPct == True):
      if ( asOverallPct == True):
        overall_total = df.sum(numeric_only=True).sum()
        df = (df / overall_total) * 100
        fmt='{:,.1f}%'
      else:
        df[cols] = df[cols].div(df[cols].sum(axis=1), axis=0).multiply(100).round(2)
    if (asOverallPct == True):
      df['Total'] = df[cols].sum(axis=1)
    ax = df.plot(kind='bar', stacked=True, figsize=(12, 10), title=title, y=cols, xlabel=xlabel, ylabel=ylabel, colormap='Paired', edgecolor='white', linewidth=1.75)
    for c in ax.containers:
      xlabels = [f'{w:.1f}%' if (w := v.get_height()) > 1 else '' for v in c ]
      ax.bar_label(c, labels=xlabels, label_type='center', rotation=rot, padding=1)

    if ( asOverallPct == True ):
      tot_labels = [f'{v:.1f}%' for v in df['Total'] ]
    else:
      tot_labels = [f'{v:.0f}' for v in df['Total'] ]
    ax.bar_label(ax.containers[-1], labels=tot_labels, fmt='{:,.3f}$$$', label_type='edge', rotation=45, padding=5)
    ax.legend(bbox_to_anchor=(1.05, 1), loc='best')
    ax.legend(cols);
    ax.margins(y=0.1)

    return df

  def plot_hist(labels, values, title, xlabel, ylabel, asPct=False, runningTot=False, color=['blue', 'orange']):
    cols = ['left']
    plot_vals = values
    secondary = False

    contents = { 'left': plot_vals }

    df = pd.DataFrame(contents, index=labels)
    if ( asPct == True ):
      df['left'] = ((df['left'] / df['left'].sum()) * 100)

    if ( runningTot == True ):
      rt = np.cumsum(df['left'])
      df.insert(1, 'right', rt)
      cols.append('right')
      secondary = True

    ax = df.plot(kind='bar', figsize=(12, 10), title=title, y=cols, xlabel=xlabel, ylabel=ylabel, legend=False, color=color, edgecolor='white', linewidth=1.75)
    rot = 90
    if ( secondary != True ):
      rot = 45
    fmt='{:,.0f}'
    if ( asPct == True ):
      fmt='{:,.1f}%'
    ax.bar_label(ax.containers[0], label_type='edge', rotation=rot, padding=5, fmt=fmt)
    ax.set_xticklabels(labels, rotation=90, ha='right')
    ax.margins(y=0.1)

    if ( secondary == True ):
      fmt='{:,.1f}%'
      if ( runningTot == True ):
        fmt='{:,.0f}%'
      ax.bar_label(ax.containers[1], label_type='edge', rotation=90, padding=5, fmt=fmt)

    return df

  plot_hist(index_size_labels, idx_summary, 'Index Count Distribution by Index Size', 'Index Size (GB)', 'Index Count')
  plot_hist(index_size_labels, cluster_summary, 'Cluster Count by Max Index Size', 'Max Index Size (GB) in cluster', 'Cluster Count')
  plot_hist(cluster_size_labels, cluster_size_summary, 'Cluster Size Distribution', 'Cluster Size (GB)', 'Cluster Count')
  plot_hist(cluster_ram_size_labels, cluster_ram_size_summary, 'Cluster Count by RAM Size Distribution', 'Cluster RAM Size (GB)', 'Cluster Count')
  plot_hist(index_size_labels, cluster_ram_summary, 'Cluster RAM total by Cluster Max Index Size', 'Max Index Size (GB) in cluster', 'RAM (GB)')
  plot_hist(index_size_labels, idx_summary, 'Index Size Distribution (as percentage) across all Clusters', 'Index Size (GB)', 'Percentage', asPct=True, color=['orange'])
  plot_hist(index_size_labels, cluster_ram_summary, 'Cluster RAM Size Distribution (as percentage) by Largest Index in Cluster', 'Index Size (GB)', 'Percentage', asPct=True, runningTot=True)
  plot_hist(cluster_ram_size_labels, cluster_ram_size_summary, 'Cluster Count (as percentage) by RAM Size', 'Index Size (GB)', 'Percentage', asPct=True)
  plot_hist(index_size_labels, cluster_summary, 'Cluster Count Distribution (as percentage) by Cluster MaxIndex Size', 'Max Index Size (GB) in cluster', 'Percentage', asPct=True,
            color=['green'])
  plot_stacked(index_size_labels, [idx_ds_summary, idx_ri_summary], ["Data Streams", "Regular Indexes"],
                   "Data Streams vs. Regular Indexes by Index Size as Percentage (with counts)", 'Index Size (GB)', 'Percentage', color=['limegreen', 'orange'])

  plot_pct_pie(index_size_labels, shard_dist, shard_dist_labels, "Index Size by Shard Distribution", 'Shards', 'Percentage')
  plot_pct_pie(index_size_labels, shard_dist, shard_dist_labels, "Index Size by Shard Distribution", 'Shards', 'Percentage', transpose=True)
  plot_pct_pie(index_size_labels, shard_size_dist, shard_dist_labels, "Accumulated Index Sizes by Index Bucket by Shard", 'Shards', 'Percentage', transpose=True)
  # plot_hist(index_size_labels, ds_summary, 'DataStream Total Sizes by Bucket', 'Index Size (GB)', 'Percentage', asPct=True, color=['limegreen'])
  plot_hist(index_size_labels, ds_counts, 'DataStream Counts by Bucket', 'Index Size (GB)', 'Percentage', asPct=True, color=['limegreen'])
  df = plot_pct_pie(index_size_labels, cluster_ram_summary_by_shard, shard_dist_labels, "Accumulated Index Sizes by Index Bucket by Shard", 'Shards', 'Percentage', asOverallPct=True)
  print(df.round(2))
  plot_stacked(index_size_labels, cluster_ram_summary_by_shard, shard_dist_labels,
                   "ARR percentage by Index Size by Shards", 'Shards', 'Percentage', colormap='Paired', asPct=True, asOverallPct=True, transpose=True)

  plt.show()

def doit():
  import argparse

  class LoadFromFile (argparse.Action):
    def __call__ (self, parser, namespace, values, option_string = None):
        with values as f:
            parser.parse_args(f.read().split(), namespace)

  parser=argparse.ArgumentParser()
  parser.add_argument('--conf', type=open, action=LoadFromFile)
  parser.add_argument("--idxfile", help="Like the file to load", default="cat_indices_output.txt")
  parser.add_argument("--numidxbuckets", help="Number of Index Buckets", default=8, type=int)
  parser.add_argument("--idxbucketsize", help="Size of Index Buckets (in GB)", default=30, type=int)
  parser.add_argument("--numshardbuckets", help="Number of Shard Buckets", default=5, type=int)
  parser.add_argument("--clusterbucketsize", help="Size of Cluster RAM Buckets (in GB)", default=64, type=int)
  parser.add_argument("--numclusterbuckets", help="Number of Cluster RAM Buckets", default=8, type=int)
  parser.add_argument("--version", help="Version to filter on", default="")
  parser.add_argument("--exclude_ds", help="Exclude Data Streams", default=False, type=bool)
  args=parser.parse_args()

  import numpy as np

  global index_size_bins
  global index_size_labels
  global idx_summary
  global idx_ds_summary
  global idx_ri_summary
  global ds_summary
  global ds_counts

  global cluster_summary
  global cluster_ram_summary
  global cluster_ram_summary_by_shard

  global cluster_size_bins
  global cluster_size_labels
  global cluster_size_summary

  global cluster_ram_size_bins
  global cluster_ram_size_labels
  global cluster_ram_size_summary

  global shard_dist
  global shard_dist_labels
  global shard_size_dist

  index_size_bins = [int(x)*args.idxbucketsize for x in range(0,args.numidxbuckets+1)]
  index_size_labels = [">"+ str(x)+"GB" for x in index_size_bins]
  index_size_labels[0] = ">1MB"
  # index_size_labels[-1] = ">" + str(index_size_bins[-2] + args.idxbucketsize) + "GB"
  idx_summary = [0] * len(index_size_bins)
  idx_ds_summary = [0] * len(index_size_bins)
  idx_ri_summary = [0] * len(index_size_bins)
  cluster_summary = [0] * len(index_size_bins)
  cluster_ram_summary = [0] * len(index_size_bins)
  cluster_ram_summary_by_shard = [ [0 for y in range(len(index_size_bins))] for x in range(0, args.numshardbuckets+1) ]

  ds_summary = [0] * len(index_size_bins)
  ds_counts = [0] * len(index_size_bins)

  cluster_size_bins = [int(x)*args.clusterbucketsize for x in range(0, args.numclusterbuckets+1)]
  cluster_size_labels  = [">"+ str(x)+"GB" for x in cluster_size_bins]
  # cluster_size_labels[-1] = ">" + str(cluster_size_bins[-2] + cluster_bucket_size) + "GB"
  cluster_size_labels[0] = ">0GB"
  cluster_size_summary = [0] * len(cluster_size_bins)

  cluster_ram_size_bins = [int(x)*args.clusterbucketsize for x in range(0,args.numclusterbuckets+1)]
  cluster_ram_size_labels  = [">"+ str(x)+"GB" for x in cluster_ram_size_bins]
  # cluster_ram_size_labels[-1] = ">" + str(cluster_ram_size_bins[-2] + args.idxbucketsize) + "GB"
  cluster_ram_size_labels[0] = ">0GB"
  cluster_ram_size_summary = [0] * len(cluster_ram_size_bins)

  shard_dist = [ [0 for y in range(len(index_size_bins))] for x in range(0, args.numshardbuckets+1) ]
  shard_dist_labels = [str(i+1) for i in range(0, args.numshardbuckets+1)]
  shard_dist_labels[-1] = ">" + str(shard_dist_labels[-2])
  shard_size_dist = [ [0 for y in range(len(index_size_bins))] for x in range(0, args.numshardbuckets+1) ]

  process(args)
  plot()

if __name__ == '__main__':
    doit()

