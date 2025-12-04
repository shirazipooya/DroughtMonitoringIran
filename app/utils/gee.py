import os
import glob
import tempfile
import ee
import geemap
import geopandas as gpd
import pandas as pd
import xee
import xarray as xr
from pathlib import Path

def extract_points_to_csv(
    image_collection_id: str,
    start_date: str,
    end_date: str,
    parameter: str,
    multiply: float = 1.0,
    add: float = 0.0,
    scale: float | None = None,
    unit: str | None = None,
    Cadence: str | None = None,
    name: str | None = None,
    points_shapefile: str | None = None,
    points_geojson: str | None = None,
    output_path: str | None = "output.csv",
    buffer_m: float | None = None,        
) -> pd.DataFrame:
  
    if points_shapefile:
        points_fc = geemap.shp_to_ee(points_shapefile)
    elif points_geojson:
        points_fc = geemap.geojson_to_ee(points_geojson)
    else:
        raise ValueError("A points_shapefile or points_geojson must be provided.")

    if buffer_m is not None and buffer_m > 0:
        points_fc = points_fc.map(lambda f: f.buffer(buffer_m))
        reducer = ee.Reducer.mean().setOutputs(["value"])
    else:
        reducer = ee.Reducer.first().setOutputs(["value"])


    raw_collection = ee.ImageCollection(image_collection_id).select(parameter)
    collection = raw_collection.filterDate(start_date, end_date)
        
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

        return ee.ImageCollection(months.map(make_monthly_image))
    
    if Cadence == "1 Day":
        collection = daily_to_monthly_sum(collection)
    
           
    if (multiply != 1.0) or (add != 0.0):
        def apply_scaling(img):
            scaled = ee.Image(img).multiply(multiply).add(add)
            return scaled.copyProperties(img, img.propertyNames())
        collection = collection.map(apply_scaling)


    if scale is not None:
        base_scale = scale
    else:
        base_scale = (
            ee.Image(raw_collection.first())
            .projection()
            .nominalScale()
            .getInfo()
        )
    print(f"Using scale: {base_scale}, buffer_m: {buffer_m}")


    def image_to_points(image):
        img = ee.Image(image).select(parameter)
        values = img.reduceRegions(
            collection=points_fc,
            reducer=reducer,
            scale=base_scale,
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
    
    if "value" in df.columns:
        if unit == "mm/month":
            pass
        elif unit == "mm/day":
            df["value"] = df["value"] * df["date"].apply(
                lambda x: pd.Period(x, freq="M").days_in_month
            )
        elif unit == "mm/hr":
            df["value"] = df["value"] * 24 * df["date"].apply(
                lambda x: pd.Period(x, freq="M").days_in_month
            )
        if name:
            df.rename(columns={"value": f"{name}"}, inplace=True)
    else:
        if name and name not in df.columns:
            df[name] = pd.NA

    if output_path:
        if output_path.lower().endswith(".xlsx"):
            df.to_excel(output_path, index=False)
        else:
            df.to_csv(output_path, index=False)        
        print(f"Point extraction results saved to {output_path}")

    return df



def run_with_adaptive_buffer(
    config: dict,
    base_points_geojson: str,
    buffer_list_m: list[float] | None = None,
) -> pd.DataFrame:
    
    if buffer_list_m is None:
        buffer_list_m = [0, 5000, 10000, 20000] 

    raw_collection = ee.ImageCollection(config["image_collection_id"]).select(
        config["parameter"]
    )
    nominal_scale = (
        ee.Image(raw_collection.first())
        .projection()
        .nominalScale()
        .getInfo()
    )
    print(f"Nominal scale for {config.get('name', config['parameter'])}: {nominal_scale}")

    
    ID_COL = "St_ID"
    gdf_all = gpd.read_file(base_points_geojson)
    station_ids = gdf_all[ID_COL].unique()

    value_col = config.get("name") or config["parameter"]

    df_final: pd.DataFrame | None = None
    remaining_ids = set(station_ids)

    for i, buf in enumerate(buffer_list_m):
        print(f"\n=== Try {i+1} with buffer_m = {buf} m ===")

        gdf_sub = gdf_all[gdf_all[ID_COL].isin(remaining_ids)].copy()
        if gdf_sub.empty:
            print("No remaining stations with all-NaN. Done.")
            break

        with tempfile.NamedTemporaryFile(suffix=".geojson", delete=False) as tmp:
            tmp_geojson = tmp.name
        gdf_sub.to_file(tmp_geojson, driver="GeoJSON")

        try:
            df_sub = extract_points_to_csv(
                image_collection_id=config["image_collection_id"],
                start_date=config["start_date"],
                end_date=config["end_date"],
                parameter=config["parameter"],
                multiply=config.get("multiply", 1.0),
                add=config.get("add", 0.0),
                scale=config.get("scale", None),
                unit=config.get("unit"),
                Cadence=config.get("Cadence"),
                name=config.get("name"),
                points_geojson=tmp_geojson,
                points_shapefile=None,
                output_path=None,
                buffer_m=buf,
            )
        finally:
            try:
                os.remove(tmp_geojson)
            except OSError:
                pass

        if "date" in df_sub.columns:
            df_sub["date"] = pd.to_datetime(df_sub["date"])

        if df_final is None:
            df_final = df_sub.copy()
        else:
            if "date" in df_final.columns and "date" in df_sub.columns:
                df_final = (
                    df_final
                    .set_index(["date", ID_COL])
                    .combine_first(
                        df_sub.set_index(["date", ID_COL])
                    )
                    .reset_index()
                )
            else:
                df_final = pd.concat([df_final, df_sub], ignore_index=True)

        remaining_ids = set()
        if df_final is not None and value_col in df_final.columns:
            for sid in station_ids:
                mask = df_final[ID_COL] == sid
                if not mask.any():
                    remaining_ids.add(sid)
                    continue
                if df_final.loc[mask, value_col].isna().all():
                    remaining_ids.add(sid)
        else:
            remaining_ids = set(station_ids)

        print(f"Stations still all-NaN after buffer {buf} m: {remaining_ids}")

        if not remaining_ids:
            print("All stations have at least some non-NaN values. Stopping.")
            break

    if df_final is None:
        raise RuntimeError("run_with_adaptive_buffer: no data extracted.")

    return df_final