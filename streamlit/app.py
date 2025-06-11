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
import seaborn as sns
import matplotlib.pyplot as plt
import time

# Page configuration
st.set_page_config(
    page_title="🌍 Magnitudr - Earthquake Hazard Detection",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS with Segment analytics blocking
st.markdown("""
<style>
    /* Block Segment Analytics */
    script[src*="segment.com"] { display: none !important; }
    script[src*="analytics.js"] { display: none !important; }
    
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
    
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #2C3E50, #34495E);
    }
</style>
""", unsafe_allow_html=True)

# API Configuration with health check
API_BASE_URL = "http://api:8000"

@st.cache_data(ttl=60)  # Cache for 1 minute
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

@st.cache_data(ttl=300)  # Cache for 5 minutes
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
                st.warning("🔄 Connecting to API...")
            time.sleep(2)  # Wait before retry
        except Exception as e:
            st.error(f"API Request Error: {e}")
            return None
    
    st.error("⚠️ API unavailable. Using fallback data.")
    return None

@st.cache_data(ttl=600)  # Cache for 10 minutes
def generate_demo_data():
    """Generate comprehensive demo data for testing"""
    np.random.seed(42)
    n_points = 1500  # Increased sample size
    
    # Indonesia coordinates with regional focus
    regions = {
        'Java': {'lat_range': (-8.5, -5.5), 'lon_range': (106, 115), 'weight': 0.3},
        'Sumatra': {'lat_range': (-6, 6), 'lon_range': (95, 106), 'weight': 0.25},
        'Sulawesi': {'lat_range': (-6, 2), 'lon_range': (118, 125), 'weight': 0.2},
        'Eastern_Indonesia': {'lat_range': (-11, 2), 'lon_range': (125, 141), 'weight': 0.15},
        'Kalimantan': {'lat_range': (-4, 5), 'lon_range': (108, 117), 'weight': 0.1}
    }
    
    data = []
    for i in range(n_points):
        # Select region based on weights
        region = np.random.choice(list(regions.keys()), p=list(r['weight'] for r in regions.values()))
        reg_info = regions[region]
        
        # Generate coordinates within region
        lat = np.random.uniform(reg_info['lat_range'][0], reg_info['lat_range'][1])
        lon = np.random.uniform(reg_info['lon_range'][0], reg_info['lon_range'][1])
        
        # Generate realistic earthquake parameters
        magnitude = np.random.exponential(1.8) + 2.5  # More realistic magnitude distribution
        depth = np.random.exponential(40) + 5  # Depth distribution
        
        # Risk zone based on magnitude and depth
        if magnitude >= 6.5 or (magnitude >= 5.5 and depth < 30):
            risk_zone = 'Extreme'
        elif magnitude >= 5.0 or (magnitude >= 4.5 and depth < 50):
            risk_zone = 'High'
        elif magnitude >= 4.0:
            risk_zone = 'Moderate'
        else:
            risk_zone = 'Low'
        
        # Hazard score calculation
        hazard_score = min(magnitude + (70 - depth) / 20, 10)
        
        earthquake = {
            'id': f'demo_{i:04d}',
            'latitude': lat,
            'longitude': lon,
            'magnitude': round(magnitude, 1),
            'depth': round(depth, 1),
            'hazard_score': round(hazard_score, 1),
            'region': region,
            'cluster_id': np.random.randint(-1, 12),
            'risk_zone': risk_zone,
            'time': (datetime.now() - timedelta(days=np.random.randint(0, 180))).isoformat(),
            'place': f'{region} Region Demo Event'
        }
        data.append(earthquake)
    
    return pd.DataFrame(data)

# Sidebar Navigation
st.sidebar.markdown("""
<div style='text-align: center; padding: 1rem; background: linear-gradient(135deg, #FF6B35, #F7931E); border-radius: 10px; margin-bottom: 1rem;'>
    <h2 style='color: white; margin: 0;'>🌍 Magnitudr</h2>
    <p style='color: white; margin: 0;'>Earthquake Hazard Detection</p>
</div>
""", unsafe_allow_html=True)

# API Health Check
api_healthy, health_data = check_api_health()
if api_healthy and health_data:
    st.sidebar.success(f"✅ API Status: {health_data.get('status', 'unknown')}")
    if 'earthquake_records' in health_data:
        st.sidebar.info(f"📊 DB Records: {health_data['earthquake_records']:,}")
else:
    st.sidebar.warning("⚠️ API Offline - Demo Mode")

# Page selection
page = st.sidebar.selectbox(
    "📍 Navigate to:",
    ["🗺️ Hazard Zone Map", "📊 Data Distribution", "📈 Temporal Analysis", "🔧 System Status"],
    index=0
)

# Load data based on API availability
if api_healthy:
    # Try to fetch real data
    stats_data = fetch_api_data("/stats")
    earthquake_data = fetch_api_data("/earthquakes?limit=2000")
    cluster_data = fetch_api_data("/clusters")
    
    if earthquake_data and stats_data:
        df_earthquakes = pd.DataFrame(earthquake_data)
        st.sidebar.metric("📊 Live Data", "Active")
        st.sidebar.metric("Total Earthquakes", f"{stats_data['total_earthquakes']:,}")
        st.sidebar.metric("Clusters", stats_data['total_clusters'])
        st.sidebar.metric("High-Risk Zones", stats_data['high_risk_zones'])
    else:
        # Fallback to demo data
        df_earthquakes = generate_demo_data()
        st.sidebar.metric("📊 Demo Data", "Active")
        st.sidebar.metric("Total Earthquakes", f"{len(df_earthquakes):,}")
        st.sidebar.metric("Clusters", "12")
        st.sidebar.metric("High-Risk Zones", "4")
else:
    # Demo mode
    df_earthquakes = generate_demo_data()
    st.sidebar.metric("📊 Demo Mode", "Active")
    st.sidebar.metric("Total Earthquakes", f"{len(df_earthquakes):,}")
    st.sidebar.metric("Clusters", "12")
    st.sidebar.metric("High-Risk Zones", "4")

# PAGE 1: HAZARD ZONE MAP
if page == "🗺️ Hazard Zone Map":
    st.markdown("""
    <div class='main-header'>
        <h1>🌍 Indonesia Earthquake Hazard Zone Map</h1>
        <p>Interactive visualization of seismic risk zones based on DBSCAN clustering analysis</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Filters
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
        risk_filter = st.multiselect(
            "🚨 Risk Zones",
            options=['Low', 'Moderate', 'High', 'Extreme'],
            default=['Low', 'Moderate', 'High', 'Extreme']
        )
    
    with col3:
        region_filter = st.multiselect(
            "🗺️ Regions",
            options=df_earthquakes['region'].unique(),
            default=df_earthquakes['region'].unique()
        )
    
    # Filter data
    filtered_df = df_earthquakes[
        (df_earthquakes['magnitude'] >= magnitude_filter) &
        (df_earthquakes['risk_zone'].isin(risk_filter)) &
        (df_earthquakes['region'].isin(region_filter))
    ]
    
    # Main map and metrics
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.markdown("### 🗺️ Interactive Hazard Zone Map")
        
        if not filtered_df.empty:
            # Create folium map
            center_lat = filtered_df['latitude'].mean()
            center_lon = filtered_df['longitude'].mean()
            
            m = folium.Map(
                location=[center_lat, center_lon],
                zoom_start=5,
                tiles='cartodbdark_matter'
            )
            
            # Color mapping for risk zones
            risk_colors = {
                'Low': '#27AE60',      # Green
                'Moderate': '#F39C12',  # Orange
                'High': '#FF6B35',      # Red-Orange
                'Extreme': '#E74C3C'    # Red
            }
            
            # Add earthquake points (sample for performance)
            sample_df = filtered_df.sample(n=min(500, len(filtered_df)), random_state=42)
            
            for idx, row in sample_df.iterrows():
                folium.CircleMarker(
                    location=[row['latitude'], row['longitude']],
                    radius=max(row['magnitude'], 2),
                    popup=f"""
                    <b>Magnitude:</b> {row['magnitude']}<br>
                    <b>Risk Zone:</b> {row['risk_zone']}<br>
                    <b>Region:</b> {row['region']}<br>
                    <b>Depth:</b> {row['depth']:.1f} km<br>
                    <b>Hazard Score:</b> {row['hazard_score']:.1f}
                    """,
                    color=risk_colors.get(row['risk_zone'], '#BDC3C7'),
                    fill=True,
                    fillOpacity=0.7,
                    weight=2
                ).add_to(m)
            
            # Display map
            map_data = st_folium(m, width=700, height=500)
        else:
            st.warning("No data matches current filters")
    
    with col2:
        st.markdown("### 📊 Risk Zone Statistics")
        
        if not filtered_df.empty:
            # Risk zone distribution
            risk_counts = filtered_df['risk_zone'].value_counts()
            
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
    
    # Bottom charts
    if not filtered_df.empty:
        st.markdown("### 📈 Risk Analysis Charts")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Magnitude vs Risk Zone
            fig_scatter = px.scatter(
                filtered_df,
                x='magnitude',
                y='depth',
                color='risk_zone',
                color_discrete_map=risk_colors,
                size='hazard_score',
                hover_data=['region'],
                title='Magnitude vs Depth by Risk Zone',
                labels={'magnitude': 'Magnitude', 'depth': 'Depth (km)'}
            )
            fig_scatter.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white')
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
        
        with col2:
            # Regional risk distribution
            risk_region = filtered_df.groupby(['region', 'risk_zone']).size().reset_index(name='count')
            
            fig_bar = px.bar(
                risk_region,
                x='region',
                y='count',
                color='risk_zone',
                color_discrete_map=risk_colors,
                title='Risk Distribution by Region',
                labels={'count': 'Number of Events', 'region': 'Region'}
            )
            fig_bar.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white')
            )
            st.plotly_chart(fig_bar, use_container_width=True)

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
        st.metric("🎯 Avg Hazard Score", f"{df_earthquakes['hazard_score'].mean():.2f}")
    
    # Distribution charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Magnitude distribution
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
    
    with col2:
        # Depth distribution by region
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
    
    # Correlation heatmap
    st.markdown("### 🔥 Feature Correlation Analysis")
    
    numeric_cols = ['magnitude', 'depth', 'hazard_score', 'latitude', 'longitude']
    available_cols = [col for col in numeric_cols if col in df_earthquakes.columns]
    
    if len(available_cols) > 1:
        corr_matrix = df_earthquakes[available_cols].corr()
        
        fig_heatmap = px.imshow(
            corr_matrix,
            text_auto=True,
            color_continuous_scale='RdYlBu_r',
            title='Feature Correlation Matrix'
        )
        fig_heatmap.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white')
        )
        st.plotly_chart(fig_heatmap, use_container_width=True)

# PAGE 3: TEMPORAL ANALYSIS
elif page == "📈 Temporal Analysis":
    st.markdown("""
    <div class='main-header'>
        <h1>📈 Temporal Earthquake Analysis</h1>
        <p>Time-series analysis and earthquake frequency patterns</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Generate time series data from actual earthquake data
    if 'time' in df_earthquakes.columns:
        # Convert time column to datetime
        df_earthquakes['time_dt'] = pd.to_datetime(df_earthquakes['time'], errors='coerce')
        df_earthquakes['date'] = df_earthquakes['time_dt'].dt.date
        
        # Daily aggregation
        daily_counts = df_earthquakes.groupby('date').agg({
            'magnitude': ['count', 'mean', 'max']
        }).reset_index()
        daily_counts.columns = ['date', 'earthquake_count', 'avg_magnitude', 'max_magnitude']
        
    else:
        # Generate sample time series
        date_range = pd.date_range(start='2024-01-01', end='2025-06-11', freq='D')
        daily_counts = pd.DataFrame({
            'date': date_range,
            'earthquake_count': np.random.poisson(3, len(date_range)),
            'avg_magnitude': np.random.normal(4.2, 0.8, len(date_range))
        })
    
    # Time series plots
    col1, col2 = st.columns(2)
    
    with col1:
        fig_time = px.line(
            daily_counts,
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
            daily_counts,
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
        <p>Monitor Airflow pipeline status and data quality metrics</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 🔄 Pipeline Status")
        
        # API Health
        if api_healthy and health_data:
            st.success(f"✅ FastAPI: {health_data.get('status', 'unknown')}")
            st.info(f"📊 Database Records: {health_data.get('earthquake_records', 0):,}")
        else:
            st.warning("⚠️ FastAPI: Offline")
        
        # Check data volume
        data_volume = fetch_api_data("/data-volume")
        if data_volume:
            st.markdown("### 📈 Data Volume Status")
            
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                st.metric("📊 Records", f"{data_volume['record_count']:,}")
            with col_b:
                st.metric("💾 Size (MB)", f"{data_volume['estimated_size_mb']:.1f}")
            with col_c:
                meets_req = data_volume['meets_64mb_requirement']
                st.metric("✅ 64MB Req", "✅ Met" if meets_req else "❌ Not Met")
            
            if not meets_req:
                st.warning(data_volume.get('recommendation', 'Consider increasing data collection period'))
    
    with col2:
        st.markdown("### 🎯 Quick Actions")
        
        # Manual data refresh
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
        
        # API endpoints
        st.markdown("### 🔗 API Endpoints")
        st.markdown("- [API Docs](http://localhost:8000/docs)")
        st.markdown("- [Health Check](http://localhost:8000/health)")
        st.markdown("- [Statistics](http://localhost:8000/stats)")
        
        # Airflow links
        st.markdown("### ⚙️ Airflow")
        st.markdown("- [Airflow UI](http://localhost:8080)")
        st.markdown("- [Master Pipeline](http://localhost:8080/dags/earthquake_master_pipeline)")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #7F8C8D; padding: 1rem;'>
    <p>🌍 <b>Magnitudr</b> - Earthquake Hazard Detection System | 
    Data Engineering Capstone Project | 
    Powered by USGS API, Apache Airflow, PostgreSQL & Streamlit</p>
</div>
""", unsafe_allow_html=True)
