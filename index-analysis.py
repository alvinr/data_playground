import numpy as np

index_size_bins = [0, 1] + [int(x)*30 for x in range(1,36)] + [np.inf]
index_size_labels = ["<"+ str(x)+"GB" for x in index_size_bins]
index_size_labels[0] = ">1MB"
index_size_labels[-2] = ">" + str(index_size_bins[-3]) + "GB"
idx_summary = [0] * len(index_size_bins) 
cluster_summary = [0] * len(index_size_bins)
cluster_ram_summary = [0] * len(index_size_bins)

cluster_size_bins = [0] + [int(x)*50 for x in range(1,20)] + [np.inf]
cluster_size_labels  = ["<"+ str(x)+"GB" for x in cluster_size_bins]
cluster_size_labels[-2] = ">" + str(cluster_size_bins[-3]) + "GB"
cluster_size_labels[0] = ">0GB"
cluster_size_summary = [0] * len(cluster_size_bins)

cluster_ram_size_bins = [0] + [int(x)*30 for x in range(1,10)] + [np.inf]
cluster_ram_size_labels  = ["<"+ str(x)+"GB" for x in cluster_ram_size_bins]
cluster_ram_size_labels[-2] = ">" + str(cluster_ram_size_bins[-3]) + "GB"
cluster_ram_size_labels[0] = ">0GB"
cluster_ram_size_summary = [0] * len(cluster_ram_size_bins)
cluster_ram_size_summary_ids = [ [ ] for j in range(len(cluster_ram_size_bins))] 

def process(idxfile, ramfile):
  import csv

  def lookup(l, k, v, ret, default):
    res = [x[ret] for x in l if x[k] == v]
    if ( len(res) == 0 ):
      return default
    else:
      return res[0]

  def update_hist(values, bins, hist):
    for i in range(len(values)):
      for j in range(len(bins)):
        if values[i] > bins[j]:
          continue
        else:
          hist[j-1] += 1
          break
    return j

  def convert_to_bytes(size_list):
    byte_sizes = {'b': 1, 'kb': 1024, 'mb': 1024 ** 2, 'gb': 1024 ** 3, 'tb': 1024 ** 4}
    def parse_size(size_str):
      num_index = len(size_str) - next((num_index for num_index, char in enumerate(size_str) if not (char.isdigit() or char == '.')), len(size_str))
      num, unit = float(size_str[:-num_index]), size_str[-num_index:].lower()
      return int(num * byte_sizes[unit])
    return [parse_size(i) for i in size_list ]

  cluster_details = []
  max_index_size_found=0
  largest_cluster_found=0

  # File with cluster_ids and RAM
  with open(ramfile, newline='') as f:
    reader = csv.reader(f, delimiter=',', quotechar='"')
    next(reader, None)
    for row in reader:
      ram = 0
      if ( row[9] != '' ):
        ram = int(row[9]) / 1024**3
      cluster_details.append( { 'cluster_id': row[0], "ram_gb": ram } )

  # Raw index size file
  clusters_examined = 0
  with open(idxfile, newline='') as f:
    reader = csv.reader(f, delimiter=',', quotechar='"')
    for row in reader:
      idx_sizes = [x for x in row[1:] if x != '']

      if len(idx_sizes) == 0:
        continue

      idx_sizes = convert_to_bytes(idx_sizes)
      idx_sizes = [i/(1024**3) for i in idx_sizes if i > (1024**2)]

      if ( len(idx_sizes) == 0 ):
        continue

      clusters_examined += 1
      idx_counts = [0] * len(index_size_bins)
      update_hist(idx_sizes, index_size_bins, idx_counts)
#      writer.writerow([row[0], idx_counts[:-1]])
#      idx_summary = [idx_counts[i] + idx_summary[i] for i in range(len(idx_counts))]
      for i in range(len(idx_summary)):
        idx_summary[i] += idx_counts[i]

      non_zero = [i for i, idx_size in enumerate(idx_counts) if idx_size != 0 ]
      cluster_summary[non_zero[-1]] += 1
      cluster_ram_summary[non_zero[-1]] += lookup(cluster_details, "cluster_id", row[0], "ram_gb", 0)
      cluster_total = sum(idx_sizes)
      update_hist([cluster_total], cluster_size_bins, cluster_size_summary)
      bin = update_hist([lookup(cluster_details, "cluster_id", row[0], "ram_gb", 0)], cluster_ram_size_bins, cluster_ram_size_summary)
      cluster_ram_size_summary_ids[bin].append(row[0])

      if max(idx_sizes) > max_index_size_found:
        max_index_size_found = max(idx_sizes)
      if cluster_total > largest_cluster_found:
        largest_cluster_found = cluster_total

  print("=== Index Size Distributions")
  print(*index_size_labels[:-1], sep='\t')
  print(*idx_summary[:-1], sep='\t')
  print("=== Cluster Count by Max Index Size, Sum of RAM by max index size")
  print(*index_size_labels[:-1], sep='\t')
  print(*cluster_summary[:-1], sep='\t')
  print(*cluster_ram_summary[:-1], sep='\t')
  print("=== Cluster Index Total Size Distribution")
  print(*cluster_size_labels[:-1], sep='\t')
  print(*cluster_size_summary[:-1], sep='\t')
  print("=== Cluster RAM Total Size Distribution")
  print(*cluster_ram_size_labels[:-1], sep='\t')
  print(*cluster_ram_size_summary[:-1], sep='\t')
  non_zero = [i for i, cluster_ids in enumerate(cluster_ram_size_summary_ids) if len(cluster_ids) != 0 ]
  print("=== Largest Clusters by RAM %s" % cluster_ram_size_labels[non_zero[-2]])
  print(cluster_ram_size_summary_ids[non_zero[-1]])

  print("=== Stats ")
  print("Clusters examined %d" % clusters_examined)
  print("Largest Cluster by Total Index Size %d (GB)" % largest_cluster_found)
  print("Largest Index %d (GB)" % max_index_size_found)
  print("Largest Cluster by RAM %d (GB)" % max(cluster_details, key=lambda x:x['ram_gb'])["ram_gb"])

def plot():
  import pandas as pd
  import matplotlib.pyplot as plt

  def calc_by_pct(source):
    total=sum(source)
    pct_of_total = [round(i/total*100, 2) for i in source]
    return pct_of_total

  def plot_hist(labels, values, title, xlabel, ylabel, asPct=False, runningTot=False, color=['blue', 'green']):
    cols = ['left']
    plot_vals = values
    secondary = False

    if ( asPct == True ):
      plot_vals = calc_by_pct(values)

    contents = { 'X': labels[:-1], 'left': plot_vals[:-1] }

    if ( runningTot == True ):
      rt = np.cumsum(plot_vals)     
      rt = [round(i) for i in rt]
      contents.update({ 'right': rt[:-1] })
      cols.append('right')
      secondary = True

    df = pd.DataFrame(contents, index=labels[:-1])
    ax = df.plot(kind='bar', figsize=(12, 10), title=title, y=cols, xlabel=xlabel, ylabel=ylabel, legend=False, color=color)
    ax.bar_label(ax.containers[0], label_type='edge', rotation=90, padding=5)
    ax.set_xticklabels(df['X'], rotation=90, ha='right')
    ax.margins(y=0.1)

    if ( secondary == True ):
      ax.bar_label(ax.containers[1], label_type='edge', rotation=90, padding=5)

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

  plt.show()

def doit():
  import argparse

  parser=argparse.ArgumentParser()
  parser.add_argument("--idxfile", help="Like the file to load", default="cat_indices_output.txt")
  parser.add_argument("--ramfile", help="like the RAM sizes for the clusters", default="clusters.csv")
#  parser.add_argument("--numbuckets", help="Number of Buckets", default=8, type=int)
  args=parser.parse_args()

  process(args.idxfile, args.ramfile)
  plot()

if __name__ == '__main__':
    doit()

