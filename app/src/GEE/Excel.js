var stations = ee.FeatureCollection("projects/drought-monitoring-iran/assets/StationsIRIMO");

// // Terra Vegetation Indices Monthly L3 Global 1 km SIN Grid - Name: NDVI, EVI - Scale: 0.0001 - Resolution: 1000 m 
// var EES = "MODIS/061/MOD13A3"; 

// Aqua Vegetation Indices Monthly L3 Global 1 km SIN Grid - Name: NDVI, EVI - Scale: 0.0001 - Resolution: 1000 m
var EES = "MODIS/061/MYD13A3";

// // Terra Land Surface Temperature and 3-Band Emissivity Monthly L3 Global 0.05 Deg CMG - Name: LST_Day, LST_Night - Scale: 0.02 - Resolution: 1000 m
// var EES = "MODIS/061/MOD21C3";

// // Aqua Land Surface Temperature and 3-Band Emissivity Monthly L3 Global 0.05 Deg CMG - Name: LST_Day, LST_Night - Scale: 0.02 - Resolution: 1000 m
// var EES = "MODIS/061/MYD21C3";


var dataset = ee.ImageCollection(EES);
var datasetName = EES.split("/")[2];
var startDate = '2000-01-01';
var endDate = '2011-12-30';
// var startDate = '2012-01-01';
// var endDate = '2024-12-30';
var parameter = 'NDVI';
var n_multiply = 0.0001;
var n_add = 0; // K to c: -273.15
var description = datasetName + "_Monthly_" + parameter + "_" + startDate.split("-")[0] + "_" + endDate.split("-")[0];
var scale = 1000;


var datasetFiltered = dataset
  .select(parameter)
  .filterDate(startDate, endDate);
print(datasetFiltered);


var extractPoints = function(image) {
  var tmp = image.multiply(n_multiply).add(n_add).unmask(-999);
  // var tmp = image.multiply(
  //   ee.Date(image.get('system:time_start'))
  //   .advance(1, 'month')
  //   .difference(ee.Date(image.get('system:time_start')), 'days')
  //   .multiply(24)
  // );
  return tmp.reduceRegions({
    collection: stations,
    reducer: ee.Reducer.mean(),
    scale: scale
  }).map(function(feature) {
    return feature.set({
      'date': image.date().format('YYYY-MM'),
      // 'date': image.date().format('YYYY-MM-dd'),
    });
  });
};



var result = datasetFiltered.map(extractPoints).flatten();
print(result);


Export.table.toDrive({
  collection: result,
  description: description,
  fileFormat: 'CSV'
});