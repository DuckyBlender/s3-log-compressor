import matplotlib.pyplot as plt
import pandas as pd
import json
from matplotlib.ticker import ScalarFormatter

# Data from the benchmark logs
rust_data = {
  "files_tested": 10000,
  "optimal_workers": 2048,
  "results": [
    {
      "duration": 22.289212176,
      "files_count": 10000,
      "kbps": 113.63019558805334,
      "total_bytes": 2593513,
      "workers": 64
    },
    {
      "duration": 21.206105782,
      "files_count": 10000,
      "kbps": 119.43388216106653,
      "total_bytes": 2593513,
      "workers": 128
    },
    {
      "duration": 22.630735286,
      "files_count": 10000,
      "kbps": 111.91538883092832,
      "total_bytes": 2593513,
      "workers": 256
    },
    {
      "duration": 23.678944752,
      "files_count": 10000,
      "kbps": 106.96116594674591,
      "total_bytes": 2593513,
      "workers": 512
    },
    {
      "duration": 21.895824911,
      "files_count": 10000,
      "kbps": 105.58809663302665,
      "total_bytes": 2367425,
      "workers": 1024
    },
    {
      "duration": 17.63411656,
      "files_count": 10000,
      "kbps": 96.56402594758056,
      "total_bytes": 1743689,
      "workers": 2048
    }
  ],
  "total_kb": 2532.7275390625
}
# Parse the data
df_rust = pd.DataFrame(rust_data["results"])

# Create the plot
fig, ax1 = plt.subplots(figsize=(12, 8))

# Plot duration
ax1.set_xlabel('Number of Workers')
ax1.set_ylabel('Duration (seconds)')
ax1.plot(df_rust['workers'], df_rust['duration'], color='tab:cyan', marker='o', linestyle='-', label='Rust Duration')
ax1.set_xscale('log')
ax1.set_yscale('log')
ax1.tick_params(axis='y')
for axis in [ax1.xaxis, ax1.yaxis]:
    axis.set_major_formatter(ScalarFormatter())

# Create a second y-axis for KB/s
ax2 = ax1.twinx()
ax2.set_ylabel('Throughput (KB/s)')
ax2.plot(df_rust['workers'], df_rust['kbps'], color='tab:orange', marker='x', linestyle='--', label='Rust Throughput (KB/s)')
ax2.set_yscale('log')
ax2.tick_params(axis='y')
ax2.yaxis.set_major_formatter(ScalarFormatter())

# Add titles and grid
plt.title('Python vs. Rust Benchmark: Download Performance (10240 MB RAM, 10K 3KB Files)')
fig.tight_layout()
fig.legend(loc="upper right", bbox_to_anchor=(0.9, 0.9))
plt.grid(True, which="both", ls="--")

# Save the plot to a file
plt.savefig("benchmark_comparison.png", dpi=300)

# Show plot
plt.show()