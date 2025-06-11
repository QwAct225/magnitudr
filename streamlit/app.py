import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import folium
from streamlit_folium import st_folium
import requests
import json
from datetime import datetime, timedelta
import time

# Page configuration
st.set_page_config(
    page_title="🌍 Magnitudr - Earthquake Hazard Detection",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #FF6B35, #F7931E);
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
        color: white;
        margin-bottom: 2rem;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #2C3E50, #34495E);
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #FF6B35;
        color: white;
    }
    
    .risk-extreme { border-left-color: #E74C3C !important; }
    .risk-high { border-left-color: #FF6B35 !important; }
    .risk-moderate { border-left-color: #F39C12 !important; }
    .risk-low { border-left-color: #27AE60 !important; }
</style>
""", unsafe_allow_html=True)

# API Configuration
API_BASE_URL = "http://api:8000"

@st.cache_data(ttl=60)
def check_api_health():
    """Check API health with timeout"""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        if response.status_code == 200:
            return True, response.json()
        else:
            return False, None
    except:
        return False, None

@st.cache_data(ttl=300)
def fetch_api_data(endpoint, retries=3):
    """Fetch data from FastAPI with retry logic"""
    for attempt in range(retries):
        try:
            response = requests.get(f"{API_BASE_URL}{endpoint}", timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 500:
                st.error(f"API Internal Error (500): {endpoint}")
                return None
            else:
                st.warning(f"API Error {response.status_code}: {endpoint}")
                return None
        except requests.exceptions.ConnectionError:
            if attempt == 0:
                st.warning("Connecting to API...")
            time.sleep(2)
        except Exception as e:
            st.error(f"API Request Error: {e}")
            return None
    
    st.error("⚠️ API unavailable")
    return None

def safe_get_column(df, column_name, default_value=None):
    """Safely get column from dataframe"""
    if column_name in df.columns:
        return df[column_name]
    else:
        st.warning(f"⚠️ Column '{column_name}' not found, using default values")
        if default_value is None:
            if column_name == 'risk_zone':
                return 'Unknown'
            elif column_name == 'region':
                return 'Unknown'
            elif column_name == 'hazard_score':
                return df.get('magnitude', 0) * 2
            else:
                return 'Unknown'
        return default_value

# Sidebar Navigation
st.sidebar.markdown("""
<div style='text-align: center; padding: 1rem; background: linear-gradient(135deg, #FF6B35, #F7931E); border-radius: 10px; margin-bottom: 1rem;'>
    <h2 style='color: white; margin: 0;'>Magnitudr</h2>
    <p style='color: white; margin: 0;'>Earthquake Hazard Detection</p>
</div>
""", unsafe_allow_html=True)

# Page selection
page = st.sidebar.selectbox(
    "Navigate:",
    ["🗺️ Hazard Zone Map", "📊 Data Distribution", "📈 Temporal Analysis", "🔧 System Status"],
    index=0
)

# API Health Check
api_healthy, health_data = check_api_health()
if api_healthy and health_data:
    st.sidebar.success(f"✅ API Status: {health_data.get('status', 'unknown')}")
    if 'earthquake_records' in health_data:
        st.sidebar.info(f"📊 DB Records: {health_data['earthquake_records']:,}")
else:
    st.sidebar.warning("⚠️ API Offline")

# Load data with complete error handling
df_earthquakes = None

if api_healthy:
    stats_data = fetch_api_data("/stats")
    earthquake_data = fetch_api_data("/earthquakes?limit=2000")
    cluster_data = fetch_api_data("/clusters")
    
    if earthquake_data and stats_data:
        try:
            df_earthquakes = pd.DataFrame(earthquake_data)
            
            # Ensure required columns exist
            required_columns = {
                'risk_zone': 'Unknown',
                'region': 'Unknown', 
                'hazard_score': 0
            }
            
            for col, default in required_columns.items():
                if col not in df_earthquakes.columns:
                    if col == 'risk_zone':
                        # Generate risk zone based on magnitude and depth
                        def calculate_risk_zone(row):
                            mag = row.get('magnitude', 0)
                            depth = row.get('depth', 999)
                            if mag >= 7.0 or (mag >= 6.0 and depth < 50):
                                return 'Extreme'
                            elif mag >= 5.5 or (mag >= 5.0 and depth < 70):
                                return 'High'
                            elif mag >= 4.0:
                                return 'Moderate'
                            else:
                                return 'Low'
                        
                        df_earthquakes['risk_zone'] = df_earthquakes.apply(calculate_risk_zone, axis=1)
                    else:
                        df_earthquakes[col] = default
            
            st.sidebar.metric("📊 Live Data", "Active")
            st.sidebar.metric("Total Earthquakes", f"{stats_data['total_earthquakes']:,}")
            st.sidebar.metric("Clusters", stats_data['total_clusters'])
            st.sidebar.metric("High-Risk Zones", stats_data['high_risk_zones'])
            
        except Exception as e:
            st.error(f"Data processing error: {e}")
            df_earthquakes = None

if df_earthquakes is None:
    st.error("❌ Unable to load earthquake data")
    st.info("Please check if the pipeline has completed successfully")
    st.code("""
    # Check pipeline status:
    docker-compose logs airflow | grep "Pipeline completed"
    
    # Check API health:
    curl http://localhost:8000/health
    """)
    st.stop()

# PAGE 1: HAZARD ZONE MAP
if page == "🗺️ Hazard Zone Map":
    st.markdown("""
    <div class='main-header'>
        <h1>🌍 Indonesia Earthquake Hazard Zone Map</h1>
        <p>Interactive visualization of seismic risk zones based on DBSCAN clustering analysis</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Safe filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        magnitude_filter = st.slider(
            "🔸 Minimum Magnitude", 
            min_value=float(df_earthquakes['magnitude'].min()), 
            max_value=float(df_earthquakes['magnitude'].max()), 
            value=float(df_earthquakes['magnitude'].min()),
            step=0.1
        )
    
    with col2:
        # Safe risk zone filter
        available_risk_zones = df_earthquakes['risk_zone'].unique().tolist()
        risk_filter = st.multiselect(
            "🚨 Risk Zones",
            options=available_risk_zones,
            default=available_risk_zones
        )
    
    with col3:
        # Safe region filter
        available_regions = df_earthquakes['region'].unique().tolist()
        region_filter = st.multiselect(
            "🗺️ Regions",
            options=available_regions,
            default=available_regions
        )
    
    # Safe filtering
    filtered_df = df_earthquakes.copy()
    
    try:
        # Apply magnitude filter
        filtered_df = filtered_df[filtered_df['magnitude'] >= magnitude_filter]
        
        # Apply risk zone filter safely
        if risk_filter:
            filtered_df = filtered_df[filtered_df['risk_zone'].isin(risk_filter)]
        
        # Apply region filter safely
        if region_filter:
            filtered_df = filtered_df[filtered_df['region'].isin(region_filter)]
            
    except Exception as e:
        st.error(f"Filtering error: {e}")
        filtered_df = df_earthquakes[df_earthquakes['magnitude'] >= magnitude_filter]
    
    # Main map and metrics
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.markdown("### Interactive Hazard Zone Map")
        
        if not filtered_df.empty:
            # Create map
            center_lat = filtered_df['latitude'].mean()
            center_lon = filtered_df['longitude'].mean()
            
            m = folium.Map(
                location=[center_lat, center_lon],
                zoom_start=5,
                tiles='cartodbdark_matter'
            )
            
            # Color mapping
            risk_colors = {
                'Low': '#27AE60',
                'Moderate': '#F39C12',
                'High': '#FF6B35',
                'Extreme': '#E74C3C'
            }
            
            # Add points (sample for performance)
            sample_df = filtered_df.sample(n=min(500, len(filtered_df)), random_state=42)
            
            for idx, row in sample_df.iterrows():
                try:
                    folium.CircleMarker(
                        location=[row['latitude'], row['longitude']],
                        radius=max(row['magnitude'], 2),
                        popup=f"""
                        <b>Magnitude:</b> {row['magnitude']}<br>
                        <b>Risk Zone:</b> {row.get('risk_zone', 'Unknown')}<br>
                        <b>Region:</b> {row.get('region', 'Unknown')}<br>
                        <b>Depth:</b> {row['depth']:.1f} km
                        """,
                        color=risk_colors.get(row.get('risk_zone', 'Unknown'), '#BDC3C7'),
                        fill=True,
                        fillOpacity=0.7,
                        weight=2
                    ).add_to(m)
                except Exception as e:
                    continue  # Skip problematic points
            
            st_folium(m, width=700, height=500)
        else:
            st.warning("No data matches current filters")
    
    with col2:
        st.markdown("### 📊 Risk Zone Statistics")
        
        if not filtered_df.empty:
            try:
                risk_counts = filtered_df['risk_zone'].value_counts()
                risk_colors = {
                    'Extreme': '#E74C3C',
                    'High': '#FF6B35', 
                    'Moderate': '#F39C12',
                    'Low': '#27AE60'
                }
                
                for risk_zone in ['Extreme', 'High', 'Moderate', 'Low']:
                    if risk_zone in risk_counts.index:
                        count = risk_counts[risk_zone]
                        percentage = (count / len(filtered_df)) * 100
                        
                        st.markdown(f"""
                        <div class='metric-card risk-{risk_zone.lower()}'>
                            <h4>{risk_zone} Risk</h4>
                            <h2>{count:,}</h2>
                            <p>{percentage:.1f}% of events</p>
                        </div>
                        """, unsafe_allow_html=True)
                        st.markdown("<br>", unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Statistics error: {e}")

# PAGE 2: DATA DISTRIBUTION  
elif page == "📊 Data Distribution":
    st.markdown("""
    <div class='main-header'>
        <h1>📊 Earthquake Data Distribution Analysis</h1>
        <p>Comprehensive analysis of processed earthquake data and ETL insights</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Data quality metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Total Records", f"{len(df_earthquakes):,}")
    with col2:
        st.metric("🌍 Regions Covered", df_earthquakes['region'].nunique())
    with col3:
        st.metric("⚡ Avg Magnitude", f"{df_earthquakes['magnitude'].mean():.2f}")
    with col4:
        avg_hazard = df_earthquakes['hazard_score'].mean() if 'hazard_score' in df_earthquakes.columns else 0
        st.metric("🎯 Avg Hazard Score", f"{avg_hazard:.2f}")
    
    # Distribution charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Magnitude distribution
        try:
            fig_hist = px.histogram(
                df_earthquakes,
                x='magnitude',
                nbins=30,
                title='Magnitude Distribution',
                color_discrete_sequence=['#FF6B35']
            )
            fig_hist.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white')
            )
            st.plotly_chart(fig_hist, use_container_width=True)
        except Exception as e:
            st.error(f"Magnitude plot error: {e}")
    
    with col2:
        # Depth distribution by region
        try:
            if 'region' in df_earthquakes.columns:
                fig_depth = px.box(
                    df_earthquakes,
                    x='region',
                    y='depth',
                    title='Depth Distribution by Region',
                    color='region'
                )
                fig_depth.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white')
                )
                st.plotly_chart(fig_depth, use_container_width=True)
            else:
                # Fallback: Simple depth histogram
                fig_depth = px.histogram(
                    df_earthquakes,
                    x='depth',
                    title='Depth Distribution',
                    color_discrete_sequence=['#3498DB']
                )
                fig_depth.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white')
                )
                st.plotly_chart(fig_depth, use_container_width=True)
        except Exception as e:
            st.error(f"Depth plot error: {e}")
    
    # Risk zone distribution
    col1, col2 = st.columns(2)
    
    with col1:
        try:
            if 'risk_zone' in df_earthquakes.columns:
                risk_counts = df_earthquakes['risk_zone'].value_counts()
                fig_risk = px.pie(
                    values=risk_counts.values,
                    names=risk_counts.index,
                    title='Risk Zone Distribution',
                    color_discrete_map={
                        'Extreme': '#E74C3C',
                        'High': '#FF6B35',
                        'Moderate': '#F39C12',
                        'Low': '#27AE60',
                        'Unknown': '#BDC3C7'
                    }
                )
                fig_risk.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white')
                )
                st.plotly_chart(fig_risk, use_container_width=True)
        except Exception as e:
            st.error(f"Risk zone plot error: {e}")
    
    with col2:
        try:
            if 'magnitude_category' in df_earthquakes.columns:
                mag_counts = df_earthquakes['magnitude_category'].value_counts()
                fig_mag_cat = px.bar(
                    x=mag_counts.index,
                    y=mag_counts.values,
                    title='Magnitude Categories',
                    color=mag_counts.values,
                    color_continuous_scale='Reds'
                )
                fig_mag_cat.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white')
                )
                st.plotly_chart(fig_mag_cat, use_container_width=True)
        except Exception as e:
            st.error(f"Magnitude category plot error: {e}")
    
    # Correlation heatmap
    st.markdown("### 🔥 Feature Correlation Analysis")
    
    try:
        numeric_cols = ['magnitude', 'depth', 'hazard_score', 'latitude', 'longitude', 'spatial_density']
        available_cols = [col for col in numeric_cols if col in df_earthquakes.columns]
        
        if len(available_cols) > 1:
            # Select numeric data only
            numeric_df = df_earthquakes[available_cols].select_dtypes(include=[np.number])
            
            if not numeric_df.empty and len(numeric_df.columns) > 1:
                corr_matrix = numeric_df.corr()
                
                fig_heatmap = px.imshow(
                    corr_matrix,
                    text_auto=True,
                    color_continuous_scale='RdYlBu_r',
                    title='Feature Correlation Matrix',
                    aspect='auto'
                )
                fig_heatmap.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white')
                )
                st.plotly_chart(fig_heatmap, use_container_width=True)
            else:
                st.info("📊 Insufficient numeric data for correlation analysis")
        else:
            st.info("📊 Insufficient columns for correlation analysis")
    except Exception as e:
        st.error(f"Correlation analysis error: {e}")

# PAGE 3: TEMPORAL ANALYSIS
elif page == "📈 Temporal Analysis":
    st.markdown("""
    <div class='main-header'>
        <h1>📈 Temporal Earthquake Analysis</h1>
        <p>Time-series analysis and earthquake frequency patterns</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Check if time data is available
    has_time_data = 'time' in df_earthquakes.columns and df_earthquakes['time'].notna().any()
    
    if has_time_data:
        try:
            # Process real time data
            df_earthquakes['time_dt'] = pd.to_datetime(df_earthquakes['time'], errors='coerce')
            df_earthquakes = df_earthquakes.dropna(subset=['time_dt'])
            
            if not df_earthquakes.empty:
                # Daily aggregation
                df_earthquakes['date'] = df_earthquakes['time_dt'].dt.date
                daily_stats = df_earthquakes.groupby('date').agg({
                    'magnitude': ['count', 'mean', 'max'],
                    'depth': 'mean'
                }).reset_index()
                
                daily_stats.columns = ['date', 'earthquake_count', 'avg_magnitude', 'max_magnitude', 'avg_depth']
                
                # Time series plots
                col1, col2 = st.columns(2)
                
                with col1:
                    fig_time = px.line(
                        daily_stats,
                        x='date',
                        y='earthquake_count',
                        title='Daily Earthquake Frequency (Real Data)',
                        color_discrete_sequence=['#FF6B35']
                    )
                    fig_time.update_layout(
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font=dict(color='white')
                    )
                    st.plotly_chart(fig_time, use_container_width=True)
                
                with col2:
                    fig_mag_time = px.line(
                        daily_stats,
                        x='date',
                        y='avg_magnitude',
                        title='Average Daily Magnitude (Real Data)',
                        color_discrete_sequence=['#F39C12']
                    )
                    fig_mag_time.update_layout(
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font=dict(color='white')
                    )
                    st.plotly_chart(fig_mag_time, use_container_width=True)
                
                # Seasonal patterns
                df_earthquakes['month'] = df_earthquakes['time_dt'].dt.month
                df_earthquakes['hour'] = df_earthquakes['time_dt'].dt.hour
                df_earthquakes['year'] = df_earthquakes['time_dt'].dt.year
                
                col1, col2 = st.columns(2)
                
                with col1:
                    monthly_pattern = df_earthquakes.groupby('month').size()
                    fig_monthly = px.bar(
                        x=monthly_pattern.index,
                        y=monthly_pattern.values,
                        title='Seasonal Pattern (Monthly) - Real Data',
                        labels={'x': 'Month', 'y': 'Earthquake Count'},
                        color_discrete_sequence=['#27AE60']
                    )
                    fig_monthly.update_layout(
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font=dict(color='white')
                    )
                    st.plotly_chart(fig_monthly, use_container_width=True)
                
                with col2:
                    yearly_pattern = df_earthquakes.groupby('year').size()
                    fig_yearly = px.bar(
                        x=yearly_pattern.index,
                        y=yearly_pattern.values,
                        title='Yearly Pattern - Real Data',
                        labels={'x': 'Year', 'y': 'Earthquake Count'},
                        color_discrete_sequence=['#9B59B6']
                    )
                    fig_yearly.update_layout(
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font=dict(color='white')
                    )
                    st.plotly_chart(fig_yearly, use_container_width=True)
                
                # Additional temporal insights
                st.markdown("### 📅 Temporal Insights")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("📊 Data Span", f"{df_earthquakes['year'].min()}-{df_earthquakes['year'].max()}")
                with col2:
                    st.metric("📈 Peak Year", f"{yearly_pattern.idxmax()}")
                with col3:
                    st.metric("🌙 Peak Month", f"{monthly_pattern.idxmax()}")
                with col4:
                    avg_daily = daily_stats['earthquake_count'].mean()
                    st.metric("📊 Avg Daily", f"{avg_daily:.1f}")
            
            else:
                st.warning("⚠️ No valid time data found after processing")
                
        except Exception as e:
            st.error(f"Temporal analysis error: {e}")
            has_time_data = False
    
    if not has_time_data:
        st.info("📊 Time data not available - showing sample patterns")
        
        # Fallback: Generate sample time series
        date_range = pd.date_range(start='2024-01-01', end='2025-06-11', freq='D')
        daily_counts = np.random.poisson(5, len(date_range))
        
        time_series_df = pd.DataFrame({
            'date': date_range,
            'earthquake_count': daily_counts,
            'avg_magnitude': np.random.normal(4.2, 0.8, len(date_range))
        })
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_time = px.line(
                time_series_df,
                x='date',
                y='earthquake_count',
                title='Daily Earthquake Frequency',
                color_discrete_sequence=['#FF6B35']
            )
            fig_time.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white')
            )
            st.plotly_chart(fig_time, use_container_width=True)
        
        with col2:
            fig_mag_time = px.line(
                time_series_df,
                x='date',
                y='avg_magnitude',
                title='Average Daily Magnitude',
                color_discrete_sequence=['#F39C12']
            )
            fig_mag_time.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white')
            )
            st.plotly_chart(fig_mag_time, use_container_width=True)

# PAGE 4: SYSTEM STATUS
elif page == "🔧 System Status":
    st.markdown("""
    <div class='main-header'>
        <h1>🔧 System Status & Pipeline Health</h1>
        <p>Monitor system status and pipeline health</p>
    </div>
    """, unsafe_allow_html=True)
    
    if api_healthy and health_data:
        st.success(f"✅ API Status: Connected")
        st.info(f"📊 Database Records: {health_data.get('earthquake_records', 0):,}")
    else:
        st.error("❌ API Status: Offline")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #7F8C8D; padding: 1rem;'>
    <p>🌍 <b>Magnitudr</b> - Earthquake Hazard Detection System</p>
</div>
""", unsafe_allow_html=True)
