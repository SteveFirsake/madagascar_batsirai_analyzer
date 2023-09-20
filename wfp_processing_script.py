import os
import geopandas as gpd
import pandas as pd
from rasterstats import zonal_stats

#Dataset Inputs
input_folder = os.getcwd() + "/datasets/"
mdg_adm2_shp = input_folder + "mdg_adm_bngrc_ocha_20181031_shp/mdg_admbnda_adm2_BNGRC_OCHA_20181031.shp"
mdg_ppln_1km = input_folder + "mdg_pd_2020_1km.tif"
mdg_api_data = input_folder + "getgeometry.json"

#EXCEL Output File Path
csv_file_path = os.getcwd() + "/outputs/mdg_processed_data.csv"

#Load and Read Input data 
#shapefile
mdg_adm2_gdf = gpd.read_file(mdg_adm2_shp)
#api - get attribute using "polygonlabel" where 120 km/h, 90 km/h and 60 km/h
with open(mdg_api_data) as jsondata:
    mdg_api_gdf = gpd.read_file(jsondata).to_crs(mdg_adm2_gdf.crs)
mdg_api_gdf_120kmh = mdg_api_gdf[mdg_api_gdf["polygonlabel"] == "120 km/h"]
mdg_api_gdf_60kmh = mdg_api_gdf[mdg_api_gdf["polygonlabel"] == "60 km/h"]
mdg_api_gdf_90kmh = mdg_api_gdf[mdg_api_gdf["polygonlabel"] == "90 km/h"]

def GetZonalStats(gdf,data,col_name):
    """
    Function to get Zonal Statistics per GeoDataFrame
    """
    totals = gpd.GeoDataFrame(zonal_stats(gdf, data, stats = "sum"))
    gdf = gdf.join(totals)
    gdf.rename(columns={'sum':col_name}, inplace=True)
    return gdf

def ClipWindZones(totals_adm2_gdf, wind_gdf, col_name):
    """
    Function to clip Adm2_totals layer with windspeed layer 
    """
    gdf = totals_adm2_gdf.clip(wind_gdf)
    gdf.rename(columns={'Total_population_by_adm2':col_name}, inplace=True)
    return gdf

# Zonal statistics of Population per Adm2
mdg_adm2_gdf_totals = GetZonalStats(mdg_adm2_gdf, mdg_ppln_1km, 'Total_population_by_adm2')

# Extract Total Population per WindSpeedZones
mdg_api_gdf_120kmh_totals = ClipWindZones(mdg_adm2_gdf_totals,mdg_api_gdf_120kmh, 'People_at_120kmph')
mdg_api_gdf_90kmh_totals = ClipWindZones(mdg_adm2_gdf_totals, mdg_api_gdf_90kmh, 'People_at_90kmph')
mdg_api_gdf_60kmh_totals = ClipWindZones(mdg_adm2_gdf_totals,mdg_api_gdf_60kmh, 'People_at_60kmph')

# Intersect mdg_adm2_gdf_totals and WindSpeedZones Totals
combined_gdf_60kmh = mdg_adm2_gdf_totals.merge(mdg_api_gdf_60kmh_totals, on=list(mdg_adm2_gdf.columns), how='left').fillna({'People_at_60kmph':0})
combined_gdf_90kmh = mdg_adm2_gdf_totals.merge(mdg_api_gdf_90kmh_totals, on=list(mdg_adm2_gdf.columns), how='left').fillna({'People_at_90kmph':0})
combined_gdf_120kmh = mdg_adm2_gdf_totals.merge(mdg_api_gdf_120kmh_totals, on=list(mdg_adm2_gdf.columns), how='left').fillna({'People_at_120kmph':0})

combined_gdf_windspeed = combined_gdf_60kmh.merge(combined_gdf_90kmh, on=list(mdg_adm2_gdf_totals.columns), how='left')
combined_gdf_windspeed = combined_gdf_windspeed.merge(combined_gdf_120kmh, on=list(mdg_adm2_gdf_totals.columns), how='left')

# Calculate percentage of afflicted people

def GetPercentageAffected(gdf, calc_column_name, percent_column_name):
    "Calculate percentages of the afflicted"
    total = sum(gdf[calc_column_name].values)
    gdf[percent_column_name] = gdf[calc_column_name].apply(lambda x: (x/total)*100)
    return gdf

combined_gdf_windspeed = GetPercentageAffected(combined_gdf_windspeed, 'People_at_60kmph', '%_people_at_60_kmph')
combined_gdf_windspeed = GetPercentageAffected(combined_gdf_windspeed, 'People_at_90kmph', '%_people_at_90_kmph')
combined_gdf_windspeed = GetPercentageAffected(combined_gdf_windspeed, 'People_at_120kmph', '%_people_at_120_kmph')

# Save to file (skip geometry column)
df_output = pd.DataFrame(combined_gdf_windspeed.drop(columns='geometry'))
df_output.to_csv(csv_file_path, columns=['ADM0_EN','ADM1_EN', 'ADM2_PCODE', 'ADM2_EN', 'Total_population_by_adm2', 'People_at_60kmph', 'People_at_90kmph', 'People_at_120kmph', '%_people_at_60_kmph', '%_people_at_90_kmph', '%_people_at_120_kmph'], index=False)