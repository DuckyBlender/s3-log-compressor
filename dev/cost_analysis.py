import matplotlib.pyplot as plt
import numpy as np
import math

# --- Configuration ---
# Pricing based on us-east-1 region, subject to change.
# S3 Standard pricing
S3_STANDARD_PRICE_PER_GB_MONTH_TIER1 = 0.023  # First 50 TB
S3_STANDARD_PRICE_PER_GB_MONTH_TIER2 = 0.022  # Next 450 TB

# S3 Glacier Deep Archive pricing
S3_DEEP_ARCHIVE_PRICE_PER_GB_MONTH = 0.00099

# S3 GET request cost
S3_GET_REQUEST_COST_PER_1000 = 0.0004

# S3 PUT request cost (to S3 Standard)
S3_PUT_REQUEST_COST_PER_1000 = 0.005

# Lambda execution cost
LAMBDA_COST_PER_100K_FILES = 0.010019

# --- Project Data ---
TOTAL_FILES = 4_027_798_346
TOTAL_UNCOMPRESSED_TB = 165
FILES_IN_CHUNK = 100_000

# --- Calculations ---
# Calculate average file size in KB. Note: 1 TB = 1024^3 KB
TOTAL_UNCOMPRESSED_KB = TOTAL_UNCOMPRESSED_TB * (1024**3)
AVERAGE_FILE_SIZE_KB = TOTAL_UNCOMPRESSED_KB / TOTAL_FILES

# --- Helper Functions ---
def plot_cost_comparison(
    months,
    cost_standard,
    cost_compressed,
    payback_months,
    title,
    label_standard,
    label_compressed,
    summary_text,
    filename
):
    """Generates and saves a cost comparison plot."""
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 7))

    # Plot costs
    ax.plot(months, cost_standard, 'o-', label=label_standard, color='royalblue')
    ax.plot(months, cost_compressed, 'o-', label=label_compressed, color='forestgreen')

    # Add payback point marker
    if 0 < payback_months <= months[-1]:
        # The cost at index 1 represents the cost for one month.
        monthly_standard_cost = cost_standard[1] if len(cost_standard) > 1 else cost_standard[0]
        payback_cost = payback_months * monthly_standard_cost
        ax.plot(payback_months, payback_cost, 'X', color='red', markersize=12, label=f'Payback Point (~{payback_months*30:.1f} days)')
        ax.axvline(x=payback_months, color='red', linestyle='--', linewidth=1, ymax=(payback_cost/ax.get_ylim()[1]))
        ax.axhline(y=payback_cost, color='red', linestyle='--', linewidth=1, xmax=(payback_months/ax.get_xlim()[1]))

    # Formatting
    ax.set_title(title, fontsize=16, pad=20)
    ax.set_xlabel('Months', fontsize=12)
    ax.set_ylabel('Cumulative Cost ($)\n*Note: Graph excludes KMS costs, assuming S3 Bucket Keys are enabled.', fontsize=12)
    ax.set_xticks(months)
    ax.legend(fontsize=10, loc='lower right')
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    ax.set_ylim(bottom=0)  # Ensure y-axis starts at 0

    # Add text box with summary
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    ax.text(0.05, 0.95, summary_text, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', bbox=props)

    plt.tight_layout()
    plt.savefig(filename, dpi=300)
    plt.close(fig) # Close the figure to free memory
    print(f"Generated plot: {filename}")


# --- Plot 1: Single Batch Calculation & Visualization ---

def analyze_single_batch():
    # 1. Uncompressed size of 100k files is 300MB
    uncompressed_size_gb = 300 / 1024
    # uncompressed_size_gb = (FILES_IN_CHUNK * AVERAGE_FILE_SIZE_KB) / (1024 * 1024)


    # 2. "Compressed" size is the same as uncompressed, as per the requirement
    compressed_size_gb = uncompressed_size_gb

    # 3. Monthly cost for S3 Standard
    monthly_cost_s3_standard = uncompressed_size_gb * S3_STANDARD_PRICE_PER_GB_MONTH_TIER1

    # 4. Monthly cost for S3 Deep Archive
    monthly_cost_s3_deep_archive = compressed_size_gb * S3_DEEP_ARCHIVE_PRICE_PER_GB_MONTH

    # --- Monthly/Yearly Cost Analysis (Storage Only) ---
    print("\n--- Single Batch Monthly & Yearly Costs (Storage Only) ---")
    print(f"S3 Standard:       ${monthly_cost_s3_standard:.4f}/month | ${monthly_cost_s3_standard * 12:.4f}/year")
    print(f"S3 Deep Archive:   ${monthly_cost_s3_deep_archive:.4f}/month | ${monthly_cost_s3_deep_archive * 12:.4f}\n")


    # 5. One-time costs for this chunk (Lambda + S3 GET requests)
    one_time_lambda_cost = LAMBDA_COST_PER_100K_FILES
    s3_get_cost = (FILES_IN_CHUNK / 1000) * S3_GET_REQUEST_COST_PER_1000
    total_one_time_cost = one_time_lambda_cost + s3_get_cost

    # --- Visualization Data ---
    months = np.arange(0, 13)
    cumulative_cost_s3_standard = months * monthly_cost_s3_standard
    cumulative_cost_compressed = total_one_time_cost + (months * monthly_cost_s3_deep_archive)

    # Find payback point
    monthly_savings = monthly_cost_s3_standard - monthly_cost_s3_deep_archive
    payback_months = total_one_time_cost / monthly_savings if monthly_savings > 0 else 0
    payback_days = payback_months * 30

    # --- Plotting ---
    title = 'Cost Comparison: S3 Standard vs. S3 Deep Archive (Single 100k File Batch)'
    label_standard = f'S3 Standard Storage ({uncompressed_size_gb:.2f} GB)'
    label_compressed = f'S3 Deep Archive ({compressed_size_gb:.2f} GB) + One-Time Costs'
    summary_text = '\n'.join((
        f'--- Scenario: Single 100k File Batch (No Size Change) ---',
        f'Data Size: {uncompressed_size_gb:.2f} GB',
        f'One-time Lambda Cost: ${one_time_lambda_cost:.4f}',
        f'One-time S3 GET Cost: ${s3_get_cost:.4f}',
        f'Total One-time Cost: ${total_one_time_cost:.4f}',
        f'Monthly Savings: ${monthly_savings:.4f}',
        f'Payback Period: {payback_days:.1f} days'
    ))
    filename = 's3_cost_visualization_single_batch.png'

    plot_cost_comparison(
        months,
        cumulative_cost_s3_standard,
        cumulative_cost_compressed,
        payback_months,
        title,
        label_standard,
        label_compressed,
        summary_text,
        filename
    )
    print(f"Payback period for a single batch of 100k files is {payback_days:.1f} days.\n")


# --- Plot 2: Total Project Calculation & Visualization ---

def analyze_total_project():
    # 1. Calculate total one-time costs (Lambda + S3 GET/PUT requests)
    total_invocations = math.ceil(TOTAL_FILES / FILES_IN_CHUNK)
    total_lambda_cost = total_invocations * LAMBDA_COST_PER_100K_FILES
    total_s3_get_cost = (TOTAL_FILES / 1000) * S3_GET_REQUEST_COST_PER_1000
    total_s3_put_cost = (total_invocations / 1000) * S3_PUT_REQUEST_COST_PER_1000
    total_one_time_cost = total_lambda_cost + total_s3_get_cost + total_s3_put_cost

    # 2. Total size remains constant after processing
    total_compressed_size_tb = TOTAL_UNCOMPRESSED_TB

    # 3. Calculate monthly cost for S3 Standard (Tiered)
    total_uncompressed_gb = TOTAL_UNCOMPRESSED_TB * 1024
    tier1_gb = 50 * 1024
    tier2_gb = total_uncompressed_gb - tier1_gb

    cost_tier1 = tier1_gb * S3_STANDARD_PRICE_PER_GB_MONTH_TIER1
    cost_tier2 = tier2_gb * S3_STANDARD_PRICE_PER_GB_MONTH_TIER2
    total_monthly_cost_standard = cost_tier1 + cost_tier2

    # 4. Calculate monthly cost for S3 Deep Archive
    total_compressed_gb = total_compressed_size_tb * 1024
    total_monthly_cost_deep_archive = total_compressed_gb * S3_DEEP_ARCHIVE_PRICE_PER_GB_MONTH
    
    # --- Visualization Data ---
    months = np.arange(0, 13)
    cumulative_cost_s3_standard = months * total_monthly_cost_standard
    cumulative_cost_compressed = total_one_time_cost + (months * total_monthly_cost_deep_archive)

    # Find payback point
    monthly_savings = total_monthly_cost_standard - total_monthly_cost_deep_archive
    payback_months = total_one_time_cost / monthly_savings if monthly_savings > 0 else 0
    payback_days = payback_months * 30

    # --- Plotting ---
    title = 'Cost Comparison: S3 Standard vs. S3 Deep Archive (Total Project)'
    label_standard = f'S3 Standard Storage ({TOTAL_UNCOMPRESSED_TB} TB)'
    label_compressed = f'S3 Deep Archive ({total_compressed_size_tb:.2f} TB) + One-Time Costs'
    summary_text = '\n'.join((
        f'--- Scenario: Total Project (No Size Change) ---',
        f'Total Files: {TOTAL_FILES:,}',
        f'Data Size: {TOTAL_UNCOMPRESSED_TB} TB',
        f'Total Lambda Cost: ${total_lambda_cost:,.2f}',
        f'Total S3 GET Cost: ${total_s3_get_cost:,.2f}',
        f'Total S3 PUT Cost: ${total_s3_put_cost:,.2f}',
        f'Total One-time Cost: ${total_one_time_cost:,.2f}',
        f'Monthly Savings: ${monthly_savings:,.2f}',
        f'Payback Period: {payback_days:.1f} days'
    ))
    filename = 's3_cost_visualization_total_project.png'

    plot_cost_comparison(
        months,
        cumulative_cost_s3_standard,
        cumulative_cost_compressed,
        payback_months,
        title,
        label_standard,
        label_compressed,
        summary_text,
        filename
    )
    print(f"Payback period for the entire project is {payback_days:.1f} days.")


# --- Plot 3: Migration Cost Breakdown Bar Chart ---

def analyze_migration_costs():
    """Calculates and visualizes the breakdown of one-time migration costs."""
    # 1. Calculate total one-time costs (Lambda + S3 GET/PUT requests)
    total_invocations = math.ceil(TOTAL_FILES / FILES_IN_CHUNK)
    total_lambda_cost = total_invocations * LAMBDA_COST_PER_100K_FILES
    total_s3_get_cost = (TOTAL_FILES / 1000) * S3_GET_REQUEST_COST_PER_1000
    total_s3_put_cost = (total_invocations / 1000) * S3_PUT_REQUEST_COST_PER_1000

    total_migration_cost = total_lambda_cost + total_s3_get_cost + total_s3_put_cost

    # Data for plotting
    costs = {
        'Lambda Execution': total_lambda_cost,
        'S3 GET Requests': total_s3_get_cost,
        'S3 PUT Requests': total_s3_put_cost
    }
    labels = list(costs.keys())
    values = list(costs.values())

    # --- Plotting ---
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(10, 6))

    bars = ax.bar(labels, values, color=['#FF9999', '#66B2FF', '#99FF99'])

    # Add labels with values and percentages
    for bar in bars:
        height = bar.get_height()
        percentage = (height / total_migration_cost) * 100
        ax.annotate(f'${height:,.2f}\n({percentage:.1f}%)',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')

    # Formatting
    ax.set_ylabel('Cost ($)')
    ax.set_title('Total One-Time Migration Cost Breakdown', fontsize=16, pad=20)
    ax.set_ylim(top=ax.get_ylim()[1] * 1.1) # Add some space at the top
    ax.text(0.95, 0.95, f'Total Cost: ${total_migration_cost:,.2f}',
            transform=ax.transAxes, fontsize=12,
            verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))


    plt.tight_layout()
    filename = 's3_migration_cost_breakdown.png'
    plt.savefig(filename, dpi=300)
    plt.close(fig)
    print(f"Generated plot: {filename}")


# --- Plot 4: Monthly/Yearly Storage Cost Comparison ---

def analyze_storage_costs():
    """Calculates and visualizes the monthly and yearly storage costs."""
    # 1. Calculate monthly cost for S3 Standard (Tiered)
    total_uncompressed_gb = TOTAL_UNCOMPRESSED_TB * 1024
    tier1_gb = 50 * 1024
    tier2_gb = total_uncompressed_gb - tier1_gb

    cost_tier1 = tier1_gb * S3_STANDARD_PRICE_PER_GB_MONTH_TIER1
    cost_tier2 = tier2_gb * S3_STANDARD_PRICE_PER_GB_MONTH_TIER2
    total_monthly_cost_standard = cost_tier1 + cost_tier2

    # 2. Calculate monthly cost for S3 Deep Archive
    total_compressed_gb = TOTAL_UNCOMPRESSED_TB * 1024
    total_monthly_cost_deep_archive = total_compressed_gb * S3_DEEP_ARCHIVE_PRICE_PER_GB_MONTH

    # Data for plotting
    labels = ['Monthly Cost', 'Yearly Cost']
    standard_costs = [total_monthly_cost_standard, total_monthly_cost_standard * 12]
    deep_archive_costs = [total_monthly_cost_deep_archive, total_monthly_cost_deep_archive * 12]

    x = np.arange(len(labels))  # the label locations
    width = 0.35  # the width of the bars

    # --- Plotting ---
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(10, 7))
    rects1 = ax.bar(x - width/2, standard_costs, width, label='S3 Standard', color='royalblue')
    rects2 = ax.bar(x + width/2, deep_archive_costs, width, label='S3 Deep Archive', color='forestgreen')

    # Add some text for labels, title and axes ticks
    ax.set_ylabel('Cost ($)')
    ax.set_title('Ongoing Monthly & Yearly Storage Cost Comparison', fontsize=16, pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    def autolabel(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            ax.annotate(f'${height:,.2f}',
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')

    autolabel(rects1)
    autolabel(rects2)

    fig.tight_layout()

    filename = 's3_storage_cost_comparison.png'
    plt.savefig(filename, dpi=300)
    plt.close(fig)
    print(f"Generated plot: {filename}")


# --- Main Execution ---
if __name__ == "__main__":
    analyze_single_batch()
    analyze_total_project()
    analyze_migration_costs()
    analyze_storage_costs()

