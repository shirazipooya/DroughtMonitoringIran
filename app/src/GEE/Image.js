var ostan = ee.FeatureCollection("projects/drought-monitoring-iran/assets/Ostan");
var dataset = ee.ImageCollection("MODIS/061/MOD13A3");

var datasetName = "MOD13A3";
var startDate = '2000-01-01';
var endDate = '2024-12-30';
var parameter = 'NDVI';
var modification = 0.0001;
var description = datasetName + "_Monthly_" + parameter;
var scale = 1000;


var datasetFiltered = dataset
  .select(parameter)
  .filterDate(startDate, endDate);


var datasetFilteredClipped = datasetFiltered.map(function(image) {
  var tmp = image.multiply(modification);
  // var tmp = image.multiply(
  //   ee.Date(image.get('system:time_start'))
  //   .advance(1, 'month')
  //   .difference(ee.Date(image.get('system:time_start')), 'days')
  //   .multiply(24)
  // );
  return tmp.clip(ostan).set('system:time_start', image.get('system:time_start'));
});


var result = datasetFilteredClipped.toBands();
print(result, "Stacked Bands");


Export.image.toDrive({
  image: result,
  description: description,
  folder: 'GEE_exports',
  scale: scale,
  region: ostan.geometry().bounds(),
  crs: 'EPSG:4326',
  maxPixels: 1e13,
  formatOptions: {
    cloudOptimized: true
  }
});
