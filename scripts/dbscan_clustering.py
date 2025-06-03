"""
Plotting Utilities for Earthquake Analysis
Standalone plotting functions that save all plots to ./data/plots/
"""

import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from pathlib import Path
import os

# Set matplotlib backend for non-interactive plotting
plt.ioff()
plt.style.use('default')

class EarthquakePlotter:
    """Utility class for creating and saving earthquake analysis plots"""
    
    def __init__(self, plots_dir='./data/plots'):
        """Initialize plotter with output directory"""
        self.plots_dir = Path(plots_dir)
        self.plots_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 Plots will be saved to: {self.plots_dir.absolute()}")
        self.colors = px.colors.qualitative.Set1
        
    def plot_data_overview(self, df, save_name='data_overview'):
        """Create overview plots of the earthquake dataset"""
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        # 1. Magnitude distribution
        axes[0, 0].hist(df['magnitude'].dropna(), bins=30, alpha=0.7, color='skyblue', edgecolor='black')
        axes[0, 0].set_title('Magnitude Distribution', fontweight='bold')
        axes[0, 0].set_xlabel('Magnitude')
        axes[0, 0].set_ylabel('Frequency')
        axes[0, 0].grid(True, alpha=0.3)
        # 2. Depth distribution
        axes[0, 1].hist(df['depth'].dropna(), bins=30, alpha=0.7, color='lightcoral', edgecolor='black')
        axes[0, 1].set_title('Depth Distribution', fontweight='bold')
        axes[0, 1].set_xlabel('Depth (km)')
        axes[0, 1].set_ylabel('Frequency')
        axes[0, 1].grid(True, alpha=0.3)
        # 3. Magnitude vs Depth scatter
        axes[0, 2].scatter(df['magnitude'], df['depth'], alpha=0.6, c='green', s=20)
        axes[0, 2].set_title('Magnitude vs Depth', fontweight='bold')
        axes[0, 2].set_xlabel('Magnitude')
        axes[0, 2].set_ylabel('Depth (km)')
        axes[0, 2].grid(True, alpha=0.3)
        # 4. Regional distribution
        if 'region' in df.columns:
            region_counts = df['region'].value_counts()
            axes[1, 0].bar(region_counts.index, region_counts.values, color='orange', alpha=0.7)
            axes[1, 0].set_title('Earthquakes by Region', fontweight='bold')
            axes[1, 0].set_xlabel('Region')
            axes[1, 0].set_ylabel('Count')
            axes[1, 0].tick_params(axis='x', rotation=45)
        # 5. Time series (if datetime available)
        if 'datetime_local' in df.columns:
            df_time = df.copy()
            df_time['date'] = pd.to_datetime(df_time['datetime_local'], errors='coerce')
            daily_counts = df_time.groupby('date').size()
            axes[1, 1].plot(daily_counts.index, daily_counts.values, color='purple', linewidth=2)
            axes[1, 1].set_title('Daily Earthquake Frequency', fontweight='bold')
            axes[1, 1].set_xlabel('Date')
            axes[1, 1].set_ylabel('Count')
            axes[1, 1].tick_params(axis='x', rotation=45)
            axes[1, 1].grid(True, alpha=0.3)
        # 6. Hazard score distribution (if available)
        if 'hazard_score' in df.columns:
            axes[1, 2].hist(df['hazard_score'].dropna(), bins=25, alpha=0.7, color='red', edgecolor='black')
            axes[1, 2].set_title('Hazard Score Distribution', fontweight='bold')
            axes[1, 2].set_xlabel('Hazard Score')
            axes[1, 2].set_ylabel('Frequency')
            axes[1, 2].grid(True, alpha=0.3)
        plt.tight_layout()
        plot_path = self.plots_dir / f'{save_name}.png'
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"📊 Data overview saved to: {plot_path}")
        return plot_path
    
    def plot_geographic_heatmap(self, df, save_name='geographic_heatmap'):
        """Create geographic heatmap of earthquake density"""
        fig = px.density_mapbox(
            df, 
            lat='latitude', 
            lon='longitude', 
            z='magnitude',
            radius=10,
            center=dict(lat=-2, lon=118),
            zoom=4,
            mapbox_style="open-street-map",
            title="Earthquake Density and Magnitude Heatmap - Indonesia",
            color_continuous_scale="Viridis"
        )
        fig.update_layout(
            mapbox_style="open-street-map",
            height=700,
            margin=dict(r=0, t=50, l=0, b=0)
        )
        plot_path_html = self.plots_dir / f'{save_name}.html'
        fig.write_html(plot_path_html)
        plot_path_png = self.plots_dir / f'{save_name}.png'
        try:
            fig.write_image(plot_path_png, width=1000, height=700)
        except Exception as e:
            print(f"⚠️  PNG export failed (plotly/kaleido not installed?): {e}")
        print(f"🗺️  Geographic heatmap saved to: {plot_path_html} and {plot_path_png}")
        return plot_path_html, plot_path_png
    
    def plot_risk_assessment(self, df, save_name='risk_assessment'):
        """Create risk assessment visualization"""
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        # 1. Alert level distribution
        if 'alert' in df.columns:
            alert_counts = df['alert'].value_counts()
            colors_alert = ['green', 'yellow', 'orange', 'red', 'gray']
            axes[0, 0].pie(alert_counts.values, labels=alert_counts.index, autopct='%1.1f%%', 
                          colors=colors_alert[:len(alert_counts)])
            axes[0, 0].set_title('Alert Level Distribution', fontweight='bold')
        # 2. Tsunami risk
        if 'tsunami' in df.columns:
            tsunami_counts = df['tsunami'].value_counts()
            axes[0, 1].bar(['No Tsunami', 'Tsunami'], tsunami_counts.values, 
                          color=['lightblue', 'darkred'], alpha=0.7)
            axes[0, 1].set_title('Tsunami Risk Distribution', fontweight='bold')
            axes[0, 1].set_ylabel('Count')
        # 3. Depth category vs Magnitude
        if 'depth_category' in df.columns:
            sns.boxplot(data=df, x='depth_category', y='magnitude', ax=axes[1, 0])
            axes[1, 0].set_title('Magnitude by Depth Category', fontweight='bold')
            axes[1, 0].tick_params(axis='x', rotation=45)
        # 4. Hazard score vs Magnitude correlation
        if 'hazard_score' in df.columns:
            axes[1, 1].scatter(df['magnitude'], df['hazard_score'], alpha=0.6, c='red', s=20)
            axes[1, 1].set_title('Hazard Score vs Magnitude', fontweight='bold')
            axes[1, 1].set_xlabel('Magnitude')
            axes[1, 1].set_ylabel('Hazard Score')
            axes[1, 1].grid(True, alpha=0.3)
            # Add trend line
            if df['magnitude'].notnull().any() and df['hazard_score'].notnull().any():
                z = np.polyfit(df['magnitude'].dropna(), df['hazard_score'].dropna(), 1)
                p = np.poly1d(z)
                axes[1, 1].plot(df['magnitude'], p(df['magnitude']), "r--", alpha=0.8)
        plt.tight_layout()
        plot_path = self.plots_dir / f'{save_name}.png'
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"⚠️  Risk assessment plots saved to: {plot_path}")
        return plot_path
    
    def plot_temporal_analysis(self, df, save_name='temporal_analysis'):
        """Create temporal analysis plots"""
        if 'datetime_local' not in df.columns:
            print("⚠️ No datetime_local column found for temporal analysis")
            return None
        # Robust datetime parsing for ISO8601 and mixed formats
        df_temp = df.copy()
        df_temp['datetime_local'] = pd.to_datetime(df_temp['datetime_local'], errors='coerce')
        df_temp['hour'] = df_temp['datetime_local'].dt.hour
        df_temp['day_of_week'] = df_temp['datetime_local'].dt.day_name()
        df_temp['month'] = df_temp['datetime_local'].dt.month_name()
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        # 1. Hourly distribution
        hourly_counts = df_temp['hour'].value_counts().sort_index()
        axes[0, 0].bar(hourly_counts.index, hourly_counts.values, alpha=0.7, color='skyblue')
        axes[0, 0].set_title('Earthquake Frequency by Hour', fontweight='bold')
        axes[0, 0].set_xlabel('Hour of Day')
        axes[0, 0].set_ylabel('Count')
        axes[0, 0].grid(True, alpha=0.3)
        # 2. Day of week distribution
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        day_counts = df_temp['day_of_week'].value_counts().reindex(day_order)
        axes[0, 1].bar(day_counts.index, day_counts.values, alpha=0.7, color='lightcoral')
        axes[0, 1].set_title('Earthquake Frequency by Day of Week', fontweight='bold')
        axes[0, 1].set_xlabel('Day of Week')
        axes[0, 1].set_ylabel('Count')
        axes[0, 1].tick_params(axis='x', rotation=45)
        # 3. Monthly distribution
        month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December']
        month_counts = df_temp['month'].value_counts().reindex(month_order)
        axes[1, 0].bar(month_counts.index, month_counts.values, alpha=0.7, color='lightgreen')
        axes[1, 0].set_title('Earthquake Frequency by Month', fontweight='bold')
        axes[1, 0].set_xlabel('Month')
        axes[1, 0].set_ylabel('Count')
        axes[1, 0].tick_params(axis='x', rotation=45)
        # 4. Magnitude over time
        df_temp_sorted = df_temp.sort_values('datetime_local').dropna(subset=['datetime_local', 'magnitude'])
        df_temp_sorted = df_temp_sorted.reset_index(drop=True)
        axes[1, 1].scatter(df_temp_sorted['datetime_local'], df_temp_sorted['magnitude'],
                        alpha=0.6, s=20, c='purple')
        axes[1, 1].set_title('Magnitude Over Time', fontweight='bold')
        axes[1, 1].set_xlabel('Date')
        axes[1, 1].set_ylabel('Magnitude')
        axes[1, 1].tick_params(axis='x', rotation=45)
        axes[1, 1].grid(True, alpha=0.3)
        plt.tight_layout()
        plot_path = self.plots_dir / f'{save_name}.png'
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"⏰ Temporal analysis plots saved to: {plot_path}")
        return plot_path
    
    def create_all_plots(self, df):
        """Create all standard earthquake analysis plots"""
        print("🎨 Creating comprehensive earthquake analysis plots...")
        print(f"📊 Dataset shape: {df.shape}")
        plot_paths = {}
        # 1. Data overview
        plot_paths['overview'] = self.plot_data_overview(df)
        # 2. Geographic analysis
        if 'latitude' in df.columns and 'longitude' in df.columns:
            plot_paths['geographic'] = self.plot_geographic_heatmap(df)
        # 3. Risk assessment
        plot_paths['risk'] = self.plot_risk_assessment(df)
        # 4. Temporal analysis
        plot_paths['temporal'] = self.plot_temporal_analysis(df)
        print(f"\n✅ All plots created and saved to: {self.plots_dir.absolute()}")
        return plot_paths

def create_earthquake_plots(csv_path='./data/earthquake_enhanced.csv', plots_dir='./data/plots'):
    """
    Standalone function to create all earthquake analysis plots
    Args:
        csv_path: Path to earthquake CSV data
        plots_dir: Directory to save plots
    """
    try:
        # Load data with robust datetime parsing
        print(f"📂 Loading earthquake data from: {csv_path}")
        df = pd.read_csv(csv_path, dtype=str)
        # Convert numeric columns
        for col in ['latitude', 'longitude', 'depth', 'magnitude', 'hazard_score', 'intensity_score']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        # Robust datetime parsing
        if 'datetime_local' in df.columns:
            df['datetime_local'] = pd.to_datetime(df['datetime_local'], errors='coerce')
        # Initialize plotter
        plotter = EarthquakePlotter(plots_dir)
        # Create all plots
        plot_paths = plotter.create_all_plots(df)
        print("\n🎉 Earthquake plotting completed successfully!")
        return plot_paths
    except FileNotFoundError:
        print(f"❌ Error: File {csv_path} not found!")
        print("Please run the ETL pipeline first to generate the data.")
        return None
    except Exception as e:
        print(f"❌ Error creating plots: {e}")
        return None

if __name__ == "__main__":
    # Create all earthquake plots
    create_earthquake_plots()