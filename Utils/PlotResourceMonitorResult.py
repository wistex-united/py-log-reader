import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import argparse

def remove_outliers(df, columns, n_std=3):
    """
    Remove outliers from specified columns using z-score method
    
    Args:
        df: DataFrame
        columns: List of column names to clean
        n_std: Number of standard deviations to use as threshold
    Returns:
        DataFrame with outliers removed
    """
    df_clean = df.copy()
    
    for column in columns:
        if column in df.columns:
            # Calculate z-score for the column
            z_scores = np.abs((df[column] - df[column].mean()) / df[column].std())
            # Create mask for rows to keep
            mask = z_scores < n_std
            # Apply mask and interpolate missing values
            df_clean.loc[~mask, column] = np.nan
            df_clean[column] = df_clean[column].interpolate(method='linear')
            
            # Print statistics about removed outliers
            outliers_count = (~mask).sum()
            if outliers_count > 0:
                print(f"\nOutliers removed from {column}: {outliers_count}")
                print(f"Original range: {df[column].min():.2f} - {df[column].max():.2f}")
                print(f"Cleaned range: {df_clean[column].min():.2f} - {df_clean[column].max():.2f}")
    
    return df_clean

def plot_resource_usage(csv_path, output_dir="plots", n_std=3):
    """
    Generate plots from resource monitoring CSV file with outlier removal
    
    Args:
        csv_path: Path to the CSV file
        output_dir: Directory to save plots
        n_std: Number of standard deviations for outlier removal
    """
    # Read CSV file
    df = pd.read_csv(csv_path)
    
    # Columns to clean (excluding timestamps and cumulative metrics)
    columns_to_clean = [
        'cpu_percent', 
        'system_cpu_percent',
        'memory_mb',
        'memory_percent',
        'read_mb_sec',
        'write_mb_sec',
        'read_count_sec',
        'write_count_sec'
    ]
    
    # Remove outliers
    print("\nRemoving outliers...")
    df_clean = remove_outliers(df, columns_to_clean, n_std)
    
    # Create output directory
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Set style
    plt.style.use('ggplot')
    
    # Create subplots
    fig, axes = plt.subplots(4, 1, figsize=(12, 20))
    fig.suptitle('Resource Usage Over Time (Outliers Removed)', fontsize=16, y=0.95)
    
    # Color palette
    colors = ['#2ecc71', '#3498db', '#e74c3c', '#f1c40f']
    
    # 1. CPU Usage
    ax1 = axes[0]
    ax1.plot(df_clean['elapsed_seconds'], df_clean['cpu_percent'], 
             label='Process CPU', color=colors[0], linewidth=2)
    ax1.plot(df_clean['elapsed_seconds'], df_clean['system_cpu_percent'], 
             label='System CPU', color=colors[1], linewidth=2)
    ax1.set_title('CPU Usage', pad=20, fontsize=14)
    ax1.set_ylabel('CPU %')
    ax1.grid(True, alpha=0.3)
    ax1.legend(loc='upper right')
    
    # 2. Memory Usage
    ax2 = axes[1]
    ax2.plot(df_clean['elapsed_seconds'], df_clean['memory_mb'], 
             label='Memory (MB)', color=colors[0], linewidth=2)
    ax2_right = ax2.twinx()
    ax2_right.plot(df_clean['elapsed_seconds'], df_clean['memory_percent'], 
                  label='Memory %', color=colors[1], linewidth=2, linestyle='--')
    ax2.set_title('Memory Usage', pad=20, fontsize=14)
    ax2.set_ylabel('Memory (MB)')
    ax2_right.set_ylabel('Memory %')
    lines1, labels1 = ax2.get_legend_handles_labels()
    lines2, labels2 = ax2_right.get_legend_handles_labels()
    ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
    
    # 3. Disk I/O
    ax3 = axes[2]
    ax3.plot(df_clean['elapsed_seconds'], df_clean['read_mb_sec'], 
             label='Read MB/s', color=colors[0], linewidth=2)
    ax3.plot(df_clean['elapsed_seconds'], df_clean['write_mb_sec'], 
             label='Write MB/s', color=colors[1], linewidth=2)
    ax3.set_title('Disk I/O', pad=20, fontsize=14)
    ax3.set_ylabel('MB/s')
    ax3.grid(True, alpha=0.3)
    ax3.legend(loc='upper right')
    
    # 4. IO Operations
    ax4 = axes[3]
    ax4.plot(df_clean['elapsed_seconds'], df_clean['read_count_sec'], 
             label='Read ops/s', color=colors[0], linewidth=2)
    ax4.plot(df_clean['elapsed_seconds'], df_clean['write_count_sec'], 
             label='Write ops/s', color=colors[1], linewidth=2)
    ax4.set_title('I/O Operations', pad=20, fontsize=14)
    ax4.set_ylabel('Operations/s')
    ax4.set_xlabel('Elapsed Time (seconds)')
    ax4.grid(True, alpha=0.3)
    ax4.legend(loc='upper right')
    
    # Adjust layout
    plt.tight_layout()
    
    # Save plots
    base_name = Path(csv_path).stem
    plt.savefig(output_dir / f'{base_name}_clean_plots.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Generate statistical summary
    summary = pd.DataFrame({
        'Metric': [
            'Average CPU %',
            'Max CPU %',
            'Average Memory (MB)',
            'Max Memory (MB)',
            'Average Read (MB/s)',
            'Average Write (MB/s)',
            'Total Duration (s)',
            'Peak System CPU %',
            'Peak Memory %'
        ],
        'Value': [
            df_clean['cpu_percent'].mean(),
            df_clean['cpu_percent'].max(),
            df_clean['memory_mb'].mean(),
            df_clean['memory_mb'].max(),
            df_clean['read_mb_sec'].mean(),
            df_clean['write_mb_sec'].mean(),
            df_clean['elapsed_seconds'].max(),
            df_clean['system_cpu_percent'].max(),
            df_clean['memory_percent'].max()
        ]
    })
    
    # Save summary
    summary.to_csv(output_dir / f'{base_name}_clean_summary.csv', index=False)
    
    # Print summary
    print("\nResource Usage Summary (After Outlier Removal):")
    print("----------------------------------------")
    for _, row in summary.iterrows():
        print(f"{row['Metric']}: {row['Value']:.2f}")

def main():
    parser = argparse.ArgumentParser(description='Generate resource usage plots from CSV')
    parser.add_argument('csv_file', help='Path to the resource usage CSV file')
    parser.add_argument('--output', '-o', default='plots', help='Output directory for plots')
    parser.add_argument('--std', '-s', type=float, default=3.0, 
                      help='Number of standard deviations for outlier removal')
    args = parser.parse_args()
    
    plot_resource_usage(args.csv_file, args.output, args.std)

if __name__ == '__main__':
    main()