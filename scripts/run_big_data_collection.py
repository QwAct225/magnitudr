"""
STEP 2: Big Data Collection for Earthquake Analysis
Goal: Collect 64MB+ earthquake data for Spark processing
"""

import requests
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BigDataCollector:
    """Collect large earthquake dataset for big data processing"""
    
    def __init__(self, target_size_mb=64, output_dir='./data/bigdata'):
        self.target_size_mb = target_size_mb
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # USGS API endpoints
        self.base_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
        self.catalog_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php"
        
        # Data collection strategy
        self.regions = {
            'indonesia': {'minlat': -11, 'maxlat': 6, 'minlon': 95, 'maxlon': 141},
            'pacific_ring': {'minlat': -60, 'maxlat': 60, 'minlon': 120, 'maxlon': -60},
            'japan': {'minlat': 24, 'maxlat': 46, 'minlon': 129, 'maxlon': 146},
            'philippines': {'minlat': 5, 'maxlat': 19, 'minlon': 116, 'maxlon': 127},
            'chile': {'minlat': -56, 'maxlat': -17, 'minlon': -76, 'maxlon': -66}
        }
        
        self.collected_data = []
        self.total_size_mb = 0
        
    def calculate_file_size_mb(self, data):
        """Calculate size of data in MB"""
        json_str = json.dumps(data)
        size_bytes = len(json_str.encode('utf-8'))
        return size_bytes / (1024 * 1024)
    
    def fetch_earthquake_data(self, params, region_name="unknown"):
        """Fetch earthquake data from USGS API with parameters"""
        try:
            logger.info(f"Fetching data for {region_name} with params: {params}")
            
            response = requests.get(self.base_url, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            if 'features' in data:
                logger.info(f"Retrieved {len(data['features'])} earthquakes for {region_name}")
                return data['features']
            else:
                logger.warning(f"No features found in response for {region_name}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {region_name}: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON for {region_name}: {e}")
            return []
    
    def generate_date_ranges(self, start_year=2020, end_year=2024):
        """Generate date ranges for historical data collection"""
        date_ranges = []
        
        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)
        
        current_date = start_date
        while current_date < end_date:
            next_date = current_date + timedelta(days=90)  # 3-month chunks
            if next_date > end_date:
                next_date = end_date
                
            date_ranges.append({
                'starttime': current_date.strftime('%Y-%m-%d'),
                'endtime': next_date.strftime('%Y-%m-%d')
            })
            
            current_date = next_date + timedelta(days=1)
        
        return date_ranges
    
    def collect_regional_data(self, region_name, region_bounds, date_ranges):
        """Collect earthquake data for a specific region"""
        region_data = []
        
        for date_range in date_ranges:
            # Check if we've reached target size
            if self.total_size_mb >= self.target_size_mb:
                logger.info(f"Target size reached, stopping collection for {region_name}")
                break
            
            params = {
                'format': 'geojson',
                'starttime': date_range['starttime'],
                'endtime': date_range['endtime'],
                'minmagnitude': 2.5,  # Lower threshold for more data
                'minlatitude': region_bounds['minlat'],
                'maxlatitude': region_bounds['maxlat'],
                'minlongitude': region_bounds['minlon'],
                'maxlongitude': region_bounds['maxlon'],
                'limit': 20000  # Maximum per request
            }
            
            features = self.fetch_earthquake_data(params, f"{region_name}_{date_range['starttime']}")
            region_data.extend(features)
            
            # Calculate current size
            current_size = self.calculate_file_size_mb(region_data)
            logger.info(f"Current size for {region_name}: {current_size:.2f} MB")
            
            # Rate limiting
            time.sleep(1)
        
        return region_data
    
    def collect_parallel_data(self):
        """Collect data from multiple regions in parallel"""
        logger.info("Starting parallel data collection...")
        
        date_ranges = self.generate_date_ranges(2020, 2024)
        logger.info(f"Generated {len(date_ranges)} date ranges")
        
        all_data = []
        
        # Use ThreadPoolExecutor for parallel collection
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_region = {}
            
            for region_name, region_bounds in self.regions.items():
                future = executor.submit(
                    self.collect_regional_data, 
                    region_name, 
                    region_bounds, 
                    date_ranges[:10]  # Limit to first 10 date ranges per region
                )
                future_to_region[future] = region_name
            
            for future in as_completed(future_to_region):
                region_name = future_to_region[future]
                try:
                    region_data = future.result()
                    all_data.extend(region_data)
                    
                    current_size = self.calculate_file_size_mb(all_data)
                    self.total_size_mb = current_size
                    
                    logger.info(f"Completed {region_name}: {len(region_data)} records")
                    logger.info(f"Total size so far: {current_size:.2f} MB")
                    
                    # Check if target reached
                    if current_size >= self.target_size_mb:
                        logger.info("Target size reached! Stopping collection.")
                        break
                        
                except Exception as e:
                    logger.error(f"Error collecting data for {region_name}: {e}")
        
        self.collected_data = all_data
        return all_data
    
    def enhance_earthquake_data(self, earthquake_features):
        """Enhance earthquake data with additional computed features"""
        enhanced_data = []
        logger.info("Enhancing earthquake data with computed features...")

        for feature in earthquake_features:
            try:
                properties = feature.get('properties', {})
                geometry = feature.get('geometry', {})
                coordinates = geometry.get('coordinates', [0, 0, 0])

                # Convert time from ms timestamp to ISO string
                raw_time = properties.get('time')
                if raw_time is not None:
                    try:
                        # USGS time is in ms since epoch
                        iso_time = datetime.utcfromtimestamp(raw_time / 1000).strftime('%Y-%m-%dT%H:%M:%SZ')
                    except Exception:
                        iso_time = ''
                else:
                    iso_time = ''
                
                # Basic earthquake data
                enhanced_record = {
                    # Original fields
                    'id': feature.get('id', ''),
                    'magnitude': properties.get('mag'),
                    'place': properties.get('place', ''),
                    'time': iso_time,
                    'updated': properties.get('updated'),
                    'longitude': coordinates[0] if len(coordinates) > 0 else None,
                    'latitude': coordinates[1] if len(coordinates) > 1 else None,
                    'depth': coordinates[2] if len(coordinates) > 2 else None,
                    
                    # USGS detailed fields
                    'felt': properties.get('felt'),
                    'cdi': properties.get('cdi'),
                    'mmi': properties.get('mmi'),
                    'alert': properties.get('alert', ''),
                    'status': properties.get('status', ''),
                    'tsunami': properties.get('tsunami', 0),
                    'sig': properties.get('sig'),
                    'net': properties.get('net', ''),
                    'code': properties.get('code', ''),
                    'ids': properties.get('ids', ''),
                    'sources': properties.get('sources', ''),
                    'types': properties.get('types', ''),
                    'nst': properties.get('nst'),
                    'dmin': properties.get('dmin'),
                    'rms': properties.get('rms'),
                    'gap': properties.get('gap'),
                    'magType': properties.get('magType', ''),
                    'type': properties.get('type', ''),
                    'url': properties.get('url', ''),
                    'detail': properties.get('detail', ''),
                    
                    # Computed features for ML
                    'energy_magnitude': self.calculate_energy_magnitude(properties.get('mag')),
                    'distance_from_surface': coordinates[2] if len(coordinates) > 2 else 0,
                    'regional_risk_score': self.calculate_regional_risk(coordinates),
                    'temporal_features': self.extract_temporal_features(properties.get('time')),
                    'intensity_composite': self.calculate_intensity_composite(properties),
                    'data_quality_score': self.calculate_data_quality(properties),
                    'tectonic_setting': self.determine_tectonic_setting(coordinates),
                    'population_exposure': self.estimate_population_exposure(coordinates, properties.get('mag')),
                    'seismic_moment': self.calculate_seismic_moment(properties.get('mag')),
                    'stress_drop_estimate': self.estimate_stress_drop(properties.get('mag'), coordinates[2] if len(coordinates) > 2 else 0)
                }
                
                enhanced_data.append(enhanced_record)
            except Exception as e:
                logger.warning(f"Error enhancing earthquake record {feature.get('id', 'unknown')}: {e}")
                continue

        logger.info(f"Enhanced {len(enhanced_data)} earthquake records")
        return enhanced_data
    
    def calculate_energy_magnitude(self, magnitude):
        """Calculate energy release from magnitude (log10 scale)"""
        if magnitude is None:
            return None
        return 1.5 * magnitude + 4.8  # Energy magnitude formula
    
    def calculate_regional_risk(self, coordinates):
        """Calculate regional risk score based on location"""
        if len(coordinates) < 2:
            return 0
        
        lon, lat = coordinates[0], coordinates[1]
        
        # High-risk regions (Ring of Fire)
        pacific_ring_zones = [
            {'bounds': [120, 160, -10, 50], 'risk': 0.9},  # Western Pacific
            {'bounds': [-180, -120, -60, 60], 'risk': 0.8},  # Eastern Pacific
            {'bounds': [95, 141, -11, 6], 'risk': 0.95}     # Indonesia (highest)
        ]
        
        for zone in pacific_ring_zones:
            if (zone['bounds'][0] <= lon <= zone['bounds'][1] and 
                zone['bounds'][2] <= lat <= zone['bounds'][3]):
                return zone['risk']
        
        return 0.3  # Default low risk
    
    def extract_temporal_features(self, timestamp):
        """Extract temporal features from earthquake timestamp"""
        if timestamp is None:
            return {}
        
        try:
            dt = datetime.fromtimestamp(timestamp / 1000)  # Convert from milliseconds
            return {
                'year': dt.year,
                'month': dt.month,
                'day': dt.day,
                'hour': dt.hour,
                'day_of_year': dt.timetuple().tm_yday,
                'day_of_week': dt.weekday(),
                'is_weekend': dt.weekday() >= 5,
                'season': (dt.month % 12 + 3) // 3  # 1: Spring, 2: Summer, 3: Fall, 4: Winter
            }
        except:
            return {}
    
    def calculate_intensity_composite(self, properties):
        """Calculate composite intensity score"""
        score = 0
        
        # Magnitude component (weight: 40%)
        mag = properties.get('mag', 0)
        if mag:
            score += min(mag / 9.0, 1.0) * 0.4
        
        # Felt reports (weight: 20%)
        felt = properties.get('felt', 0)
        if felt:
            score += min(np.log10(felt + 1) / 5.0, 1.0) * 0.2
        
        # Significance (weight: 20%)
        sig = properties.get('sig', 0)
        if sig:
            score += min(sig / 2000.0, 1.0) * 0.2
        
        # Alert level (weight: 20%)
        alert = properties.get('alert', '')
        alert_scores = {'green': 0.2, 'yellow': 0.5, 'orange': 0.8, 'red': 1.0}
        score += alert_scores.get(alert, 0) * 0.2
        
        return score
    
    def calculate_data_quality(self, properties):
        """Calculate data quality score"""
        score = 1.0
        
        # Azimuthal gap penalty
        gap = properties.get('gap')
        if gap and gap > 180:
            score -= 0.3
        
        # RMS error penalty
        rms = properties.get('rms')
        if rms and rms > 1.0:
            score -= 0.2
        
        # Station count bonus
        nst = properties.get('nst')
        if nst and nst >= 10:
            score += 0.1
        
        return max(score, 0.0)
    
    def determine_tectonic_setting(self, coordinates):
        """Determine tectonic setting based on location"""
        if len(coordinates) < 2:
            return 'unknown'
        
        lon, lat = coordinates[0], coordinates[1]
        
        # Simplified tectonic classification
        if 95 <= lon <= 141 and -11 <= lat <= 6:
            return 'subduction_zone'
        elif -180 <= lon <= -60 and -60 <= lat <= 60:
            return 'transform_fault'
        elif 120 <= lon <= 160 and -10 <= lat <= 50:
            return 'volcanic_arc'
        else:
            return 'intraplate'
    
    def estimate_population_exposure(self, coordinates, magnitude):
        """Estimate population exposure based on location and magnitude"""
        if len(coordinates) < 2 or not magnitude:
            return 0
        
        # Simplified population density mapping
        high_density_regions = [
            {'bounds': [106, 115, -9, -5], 'density': 1000},  # Java
            {'bounds': [139, 141, 35, 36], 'density': 6000},  # Tokyo
            {'bounds': [-118, -117, 33, 34], 'density': 3000}  # LA
        ]
        
        lon, lat = coordinates[0], coordinates[1]
        population_density = 100  # Default
        
        for region in high_density_regions:
            if (region['bounds'][0] <= lon <= region['bounds'][1] and 
                region['bounds'][2] <= lat <= region['bounds'][3]):
                population_density = region['density']
                break
        
        # Exposure radius based on magnitude (simplified)
        exposure_radius = 10 ** (magnitude - 3)  # km
        exposure_area = np.pi * (exposure_radius ** 2)
        
        return population_density * exposure_area / 1000  # Scaled down
    
    def calculate_seismic_moment(self, magnitude):
        """Calculate seismic moment from magnitude"""
        if magnitude is None:
            return None
        return 10 ** (1.5 * magnitude + 9.1)  # N⋅m
    
    def estimate_stress_drop(self, magnitude, depth):
        """Estimate stress drop from magnitude and depth"""
        if magnitude is None or depth is None:
            return None
        
        # Simplified stress drop estimation (MPa)
        base_stress = 1.0  # MPa
        magnitude_factor = 10 ** (magnitude - 5)
        depth_factor = 1 + depth / 100
        
        return base_stress * magnitude_factor * depth_factor
    
    def save_big_data(self, data, filename_prefix='earthquake_bigdata'):
        """Save collected data in multiple formats"""
        logger.info("Saving big data to files...")
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Save as CSV (main format)
        csv_path = self.output_dir / f'{filename_prefix}.csv'
        df.to_csv(csv_path, index=False)
        
        # Save as Parquet (for Spark optimization)
        parquet_path = self.output_dir / f'{filename_prefix}.parquet'
        df.to_parquet(parquet_path, index=False)
        
        # Save as JSON (for flexibility)
        json_path = self.output_dir / f'{filename_prefix}.json'
        with open(json_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Calculate and log file sizes
        csv_size = os.path.getsize(csv_path) / (1024 * 1024)
        parquet_size = os.path.getsize(parquet_path) / (1024 * 1024)
        json_size = os.path.getsize(json_path) / (1024 * 1024)
        
        logger.info(f"Files saved:")
        logger.info(f"  CSV: {csv_path} ({csv_size:.2f} MB)")
        logger.info(f"  Parquet: {parquet_path} ({parquet_size:.2f} MB)")
        logger.info(f"  JSON: {json_path} ({json_size:.2f} MB)")
        
        # Create metadata file
        metadata = {
            'collection_date': datetime.now().isoformat(),
            'total_records': len(data),
            'file_sizes_mb': {
                'csv': csv_size,
                'parquet': parquet_size,
                'json': json_size
            },
            'data_sources': list(self.regions.keys()),
            'date_range_collected': self.generate_date_ranges(2020, 2024),
            'features_included': list(data[0].keys()) if data else [],
            'target_size_mb': self.target_size_mb,
            'actual_size_mb': max(csv_size, parquet_size, json_size)
        }
        
        metadata_path = self.output_dir / f'{filename_prefix}_metadata.json'
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Metadata saved: {metadata_path}")
        
        return {
            'csv_path': csv_path,
            'parquet_path': parquet_path,
            'json_path': json_path,
            'metadata_path': metadata_path,
            'total_size_mb': max(csv_size, parquet_size, json_size),
            'record_count': len(data)
        }
    
    def collect_big_data(self):
        """Main method to collect big earthquake dataset with accurate file size check"""
        logger.info(f"Starting big data collection (target: {self.target_size_mb} MB)")

        # Hapus semua file lama di folder output_dir sebelum mulai koleksi baru
        for f in self.output_dir.glob('*'):
            try:
                f.unlink()
            except Exception as e:
                logger.warning(f"Failed to delete old file {f}: {e}")

        all_data = []
        target_reached = False

        # Loop terus ke semua region dan semua date_range sampai target size tercapai
        while not target_reached:
            for region_name, region_bounds in self.regions.items():
                logger.info(f"Collecting data for region: {region_name}")
                date_ranges = self.generate_date_ranges(2020, 2024)
                for date_range in date_ranges:
                    params = {
                        'format': 'geojson',
                        'starttime': date_range['starttime'],
                        'endtime': date_range['endtime'],
                        'minmagnitude': 2.5,
                        'minlatitude': region_bounds['minlat'],
                        'maxlatitude': region_bounds['maxlat'],
                        'minlongitude': region_bounds['minlon'],
                        'maxlongitude': region_bounds['maxlon'],
                        'limit': 20000
                    }
                    features = self.fetch_earthquake_data(params, f"{region_name}_{date_range['starttime']}")
                    all_data.extend(features)
                    logger.info(f"Current raw data count: {len(all_data)}")

                    # Enhance and save temporarily to check file size
                    if len(all_data) % 5000 == 0 or len(all_data) > 20000:
                        enhanced_data = self.enhance_earthquake_data(all_data)
                        temp_result = self.save_big_data(enhanced_data, filename_prefix='temp_earthquake_bigdata')
                        logger.info(f"Current max file size: {temp_result['total_size_mb']:.2f} MB")
                        if temp_result['total_size_mb'] >= self.target_size_mb:
                            logger.info("Target file size reached after enhancement, stopping collection.")
                            target_reached = True
                            break
                if target_reached:
                    break
            if not target_reached:
                logger.info("All regions and date ranges processed, but target size not reached. Repeating collection with more data...")

        # Step 2: Enhance data with ML features (final)
        enhanced_data = self.enhance_earthquake_data(all_data)

        # Step 3: Save data (final)
        result = self.save_big_data(enhanced_data)

        logger.info("Big data collection completed!")
        logger.info(f"Total records: {result['record_count']}")
        logger.info(f"Total size: {result['total_size_mb']:.2f} MB")

        # Cleanup temp files
        temp_files = list(self.output_dir.glob('temp_earthquake_bigdata*'))
        for f in temp_files:
            try:
                f.unlink()
            except Exception:
                pass

        return result

    def _is_target_size_reached(self, data):
        """Check if any output file (CSV/JSON/Parquet) has reached the target size"""
        if not data:
            return False
        try:
            enhanced = self.enhance_earthquake_data(data)
            temp_result = self.save_big_data(enhanced, filename_prefix='temp_earthquake_bigdata_check')
            size = temp_result['total_size_mb']
            # Cleanup temp files
            for f in self.output_dir.glob('temp_earthquake_bigdata_check*'):
                try:
                    f.unlink()
                except Exception:
                    pass
            logger.info(f"Actual temp file size: {size:.2f} MB")
            return size >= self.target_size_mb
        except Exception as e:
            logger.warning(f"Size check failed: {e}")
            return False

def main():
    """Main function to run big data collection"""
    print("🌍 BIG DATA COLLECTION FOR EARTHQUAKE ANALYSIS")
    print("🎯 Target: 64MB+ dataset for Spark processing")
    print("=" * 60)
    
    try:
        # Initialize collector
        collector = BigDataCollector(target_size_mb=64)
        
        # Collect big data
        result = collector.collect_big_data()
        
        if result:
            print("\n🎉 Big Data Collection Completed Successfully!")
            print(f"📊 Dataset size: {result['total_size_mb']:.2f} MB")
            print(f"📊 Total records: {result['record_count']:,}")
            print(f"📁 Files saved to: {collector.output_dir}")
            print("\n📋 Generated files:")
            for key, path in result.items():
                if 'path' in key:
                    print(f"  • {path}")
            
            print("\n🚀 Ready for Spark processing!")
            return result
        else:
            print("❌ Data collection failed!")
            return None
            
    except Exception as e:
        logger.error(f"Error in big data collection: {e}")
        return None

if __name__ == "__main__":
    result = main()