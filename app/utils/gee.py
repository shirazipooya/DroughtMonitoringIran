import os
import glob
import ee
import geemap
import geopandas as gpd
import pandas as pd
import xee
import xarray as xr
from pathlib import Path

def extract_points_to_csv(image_collection_id: str, 
                          start_date: str, 
                          end_date: str, 
                          parameter: str, 
                          multiply: float = 1.0, 
                          add: float = 0.0, 
                          scale: float = None,
                          unit: str = None,
                          Cadence: str = None,
                          name: str = None,
                          points_shapefile: str = None, 
                          points_geojson: str = None, 
                          output_path: str = "output.csv") -> None:
  
    if points_shapefile:
        points_fc = geemap.shp_to_ee(points_shapefile)
    elif points_geojson:
        points_fc = geemap.geojson_to_ee(points_geojson)
    else:
        raise ValueError("A points_shapefile or points_geojson must be provided.")
    
    collection = ee.ImageCollection(image_collection_id).filterDate(start_date, end_date).select(parameter)
    
    def daily_to_monthly_sum(col):
        def add_month(img):
            date = ee.Date(img.get("system:time_start"))
            return img.set("month", date.format("YYYY-MM"))
        
        col_with_month = col.map(add_month)

        months = ee.List(col_with_month.aggregate_array("month")).distinct()

        band_names = ee.Image(col.first()).bandNames()

        def make_monthly_image(m):
            m = ee.String(m)
            month_coll = col_with_month.filter(ee.Filter.eq("month", m))
            month_sum = month_coll.sum()
            month_sum = month_sum.rename(band_names)

            date = ee.Date.parse("YYYY-MM", m)
            return month_sum.set("system:time_start", date.millis())

        monthly_ic = ee.ImageCollection(months.map(make_monthly_image))
        return monthly_ic
    
    if Cadence == "1 Day":
        collection = daily_to_monthly_sum(collection)
    
           
    if (multiply != 1.0) or (add != 0.0):
        def apply_scaling(img):
            scaled = ee.Image(img).multiply(multiply).add(add)
            return scaled.copyProperties(img, img.propertyNames())
        collection = collection.map(apply_scaling)
    
    def image_to_points(image):
        img_scale = scale if scale is not None else image.projection().nominalScale()
        img = ee.Image(image).select(parameter)
        values = img.reduceRegions(
            collection=points_fc,
            reducer=ee.Reducer.first().setOutputs(["value"]),
            scale=img_scale,
        )
        date_str = ee.Date(image.get("system:time_start")).format("YYYY-MM-dd")
        return values.map(lambda f: f.set("date", date_str))
    
    def accumulate_points(image, fc_list):
        fc_list = ee.List(fc_list)
        values_with_date = image_to_points(image)
        return fc_list.add(values_with_date)
    
    fc_list = ee.List(collection.iterate(accumulate_points, ee.List([])))
    result_fc = ee.FeatureCollection(fc_list).flatten()
    df = geemap.ee_to_df(result_fc)
    

    
    if unit == "mm/month":
        df["value"] = df["value"]
    elif unit == "mm/day":
        df["value"] = df["value"] * df["date"].apply(lambda x: pd.Period(x, freq='M').days_in_month)
    elif unit == "mm/hr":
        df["value"] = df["value"] * 24 * df["date"].apply(lambda x: pd.Period(x, freq='M').days_in_month)
    
    if name:
        df.rename(columns = {"value": f"{name}"}, inplace=True)

    if output_path:
        if output_path.lower().endswith(".xlsx"):
            df.to_excel(output_path, index=False)
        else:
            df.to_csv(output_path, index=False)        
        print(f"Point extraction results saved to {output_path}")

    return df



def run_with_adaptive_scale(
    config: dict,
    base_points_geojson: str,
    scale_factors=None
) -> pd.DataFrame:
    if scale_factors is None:
        scale_factors = [2, 3, 4, 5, 7, 8, 9, 10, 15, 20]
    col = ee.ImageCollection(config["image_collection_id"]) \
        .filterDate(config["start_date"], config["end_date"]) \
            .select(config["parameter"])

    nominal_scale = col.first().projection().nominalScale().getInfo()
    print("Nominal Scale:", nominal_scale)

    scales = [None] + [nominal_scale * f for f in scale_factors]
    print("Scales to test:", scales)
    
    ID_COL = "St_ID"
    gdf_all = gpd.read_file(base_points_geojson)
    station_ids = gdf_all[ID_COL].unique()

    value_col = config.get("name") or config["parameter"]

    df_final: pd.DataFrame | None = None
    remaining_ids = set(station_ids)

    for i, sc in enumerate(scales):
        print(f"\n=== Try {i+1} with scale = {sc} ===")

        gdf_sub = gdf_all[gdf_all[ID_COL].isin(remaining_ids)].copy()
        if gdf_sub.empty:
            print("No remaining stations with all-NaN. Done.")
            break

        tmp_geojson = f"tmp_stations_scale_{sc or 'default'}.geojson"
        gdf_sub.to_file(tmp_geojson, driver="GeoJSON")

        df_sub = extract_points_to_csv(
            image_collection_id=config["image_collection_id"],
            start_date=config["start_date"],
            end_date=config["end_date"],
            parameter=config["parameter"],
            multiply=config.get("multiply", 1.0),
            add=config.get("add", 0.0),
            scale=sc,
            unit=config.get("unit"),
            Cadence=config.get("Cadence"),
            name=config.get("name"),
            points_geojson=tmp_geojson,
            points_shapefile=None,
            output_path=None,
        )
        
        try:
            os.remove(tmp_geojson)
        except FileNotFoundError as e:
            print("Could not delete temp file:", tmp_geojson, e)

        df_sub["date"] = pd.to_datetime(df_sub["date"])

        if df_final is None:
            df_final = df_sub.copy()
        else:
            df_final = (
                df_final
                .set_index(["date", ID_COL])
                .combine_first(
                    df_sub.set_index(["date", ID_COL])
                )
                .reset_index()
            )

        remaining_ids = set()
        for sid in station_ids:
            mask = df_final[ID_COL] == sid
            if not mask.any():
                remaining_ids.add(sid)
                continue
            if df_final.loc[mask, value_col].isna().all():
                remaining_ids.add(sid)

        print(f"Stations still all-NaN after scale {sc}: {remaining_ids}")

        if not remaining_ids:
            print("All stations have at least some non-NaN values. Stopping.")
            break

    if df_final is None:
        raise RuntimeError("run_with_adaptive_scale: no data extracted.")

    return df_final