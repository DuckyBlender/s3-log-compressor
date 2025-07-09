import matplotlib.pyplot as plt
import pandas as pd
import json
from matplotlib.ticker import ScalarFormatter

# Data from the benchmark logs
rust_data = {
  "files_tested": 1000,
  "optimal_workers": 512,
  "results": [
    { "duration": 19.40474132, "files_count": 1000, "kbps": 7.369131666940459, "total_bytes": 146428, "workers": 1 },
    { "duration": 8.699481258, "files_count": 1000, "kbps": 16.437312698214217, "total_bytes": 146428, "workers": 2 },
    { "duration": 4.066044917, "files_count": 1000, "kbps": 35.168350736151005, "total_bytes": 146428, "workers": 4 },
    { "duration": 1.959822741, "files_count": 1000, "kbps": 72.96378940731967, "total_bytes": 146428, "workers": 8 },
    { "duration": 0.969351441, "files_count": 1000, "kbps": 147.51728599328507, "total_bytes": 146428, "workers": 16 },
    { "duration": 0.495736574, "files_count": 1000, "kbps": 288.45177307817517, "total_bytes": 146428, "workers": 32 },
    { "duration": 0.319751097, "files_count": 1000, "kbps": 447.21064318975584, "total_bytes": 146428, "workers": 64 },
    { "duration": 0.152899897, "files_count": 1000, "kbps": 935.2268808264795, "total_bytes": 146428, "workers": 128 },
    { "duration": 0.117630679, "files_count": 1000, "kbps": 1215.6360480585172, "total_bytes": 146428, "workers": 256 },
    { "duration": 0.107457009, "files_count": 1000, "kbps": 1330.728400880765, "total_bytes": 146428, "workers": 512 }
  ],
  "total_kb": 142.99609375
}

python_data_raw = {
  "statusCode": 200,
  "body": "{\"results\": [{\"workers\": 1, \"duration\": 41.0936963558197, \"total_bytes\": 146428, \"files_count\": 1000, \"kbps\": 3.47975739422012}, {\"workers\": 2, \"duration\": 16.927894592285156, \"total_bytes\": 146428, \"files_count\": 1000, \"kbps\": 8.447364376617166}, {\"workers\": 4, \"duration\": 8.58561110496521, \"total_bytes\": 146428, \"files_count\": 1000, \"kbps\": 16.65531923141765}, {\"workers\": 8, \"duration\": 6.380845069885254, \"total_bytes\": 146428, \"files_count\": 1000, \"kbps\": 22.410212469329156}, {\"workers\": 16, \"duration\": 6.12470817565918, \"total_bytes\": 146428, \"files_count\": 1000, \"kbps\": 23.347413403024685}, {\"workers\": 32, \"duration\": 6.252187728881836, \"total_bytes\": 146428, \"files_count\": 1000, \"kbps\": 22.87136918321132}, {\"workers\": 64, \"duration\": 6.142392873764038, \"total_bytes\": 146428, \"files_count\": 1000, \"kbps\": 23.28019335278573}, {\"workers\": 128, \"duration\": 5.908543109893799, \"total_bytes\": 146428, \"files_count\": 1000, \"kbps\": 24.201582537420165}, {\"workers\": 256, \"duration\": 5.940462589263916, \"total_bytes\": 146428, \"files_count\": 1000, \"kbps\": 24.07154183723572}, {\"workers\": 512, \"duration\": 6.311378240585327, \"total_bytes\": 146428, \"files_count\": 1000, \"kbps\": 22.656872762032137}], \"optimal_workers\": 128, \"files_tested\": 1000, \"total_kb\": 142.99609375}"
}
python_data = json.loads(python_data_raw["body"])

# Parse the data
df_rust = pd.DataFrame(rust_data["results"])
df_python = pd.DataFrame(python_data["results"])

# Create the plot
fig, ax1 = plt.subplots(figsize=(12, 8))

# Plot duration
ax1.set_xlabel('Number of Workers')
ax1.set_ylabel('Duration (seconds)')
ax1.plot(df_python['workers'], df_python['duration'], color='tab:blue', marker='o', linestyle='-', label='Python Duration')
ax1.plot(df_rust['workers'], df_rust['duration'], color='tab:cyan', marker='o', linestyle='-', label='Rust Duration')
ax1.set_xscale('log')
ax1.set_yscale('log')
ax1.tick_params(axis='y')
for axis in [ax1.xaxis, ax1.yaxis]:
    axis.set_major_formatter(ScalarFormatter())

# Create a second y-axis for KB/s
ax2 = ax1.twinx()
ax2.set_ylabel('Throughput (KB/s)')
ax2.plot(df_python['workers'], df_python['kbps'], color='tab:red', marker='x', linestyle='--', label='Python Throughput (KB/s)')
ax2.plot(df_rust['workers'], df_rust['kbps'], color='tab:orange', marker='x', linestyle='--', label='Rust Throughput (KB/s)')
ax2.set_yscale('log')
ax2.tick_params(axis='y')
ax2.yaxis.set_major_formatter(ScalarFormatter())

# Add titles and grid
plt.title('Python vs. Rust Benchmark: Download Performance (10240 MB RAM)')
fig.tight_layout()
fig.legend(loc="upper right", bbox_to_anchor=(0.9, 0.9))
plt.grid(True, which="both", ls="--")

# Save the plot to a file
plt.savefig("benchmark_comparison.png", dpi=300)

# Show plot
plt.show()