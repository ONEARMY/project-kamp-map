const { GeoPackageAPI } = require('@ngageoint/geopackage')
const { getDistance } = require('geolib')

const { cloneFeature, deleteFeature, createFeatureAsync } = require('./utils/geopkg_utils.js')
const { dedupeConsecutivePoints } = require('./utils/points.js')

const INPUT_FILE = './ProjectKamp.gpkg';
const INPUT_FEATURE = 'Raw';
const OUTPUT_FEATURE = 'Sanitised';

const outputWriter = async (feature, values, status) => {
  for (value of values) {
    await createFeatureAsync(feature, value);
    status.rows++;
  }
  status.done = true
  return values.length
}

(async () => {
  /*
   * Open file, open input table, delete/create output table
   */
  const pkg = await GeoPackageAPI.open(INPUT_FILE);
  const inputFeature = pkg.getFeatureDao(INPUT_FEATURE)
  if (pkg.hasFeatureTable(OUTPUT_FEATURE)) {
    console.log(`Deleting feature ${OUTPUT_FEATURE}`)
    deleteFeature(pkg, OUTPUT_FEATURE)
    console.log(`${OUTPUT_FEATURE} deleted`)
  }
  const outputFeature = await cloneFeature(pkg, inputFeature, OUTPUT_FEATURE)
  //console.log(outputFeature.columns)

  /*
   * Begin processing. Note, the pipeline will process each function one by one, taking the output of one function into the next
   *   1. Dedupe the consecutive points (duplicate readings, assumed from different GPS protocols)
   */

  const inputPoints = inputFeature.queryForEach(null, null, null, null, 'created_at ASC')

  const processingPipeline = [
    (points) => { return dedupeConsecutivePoints(points, { offset: 0.0001 })}
  ]

  const outputPoints = processingPipeline.reduce((a, c) => c(a), inputPoints)

  /*
   * Write all the data, and close
   */

  console.log('Beginning output write')
  const status = { row: 0, done: false }
  const statusReporter = setInterval(() => {
    if (status.done) {
      clearInterval(statusReporter)
      return;
    }
    console.log(`Writing... ${status.row} records written so far`)
  }, 500)

  const outputPromise = await outputWriter(outputFeature, outputPoints, status)
  pkg.close()

  console.log('Done')
})()
