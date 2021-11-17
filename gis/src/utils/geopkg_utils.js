const { meanBy } = require('lodash')
const { FeatureColumn } = require('@ngageoint/geopackage')

const featureColumnFromExistingTable = (column) => {
  const { index, name, dataType, max, notNull, defaultValue, primaryKey, geometryType } = column
  if (column.primaryKey) {
    return FeatureColumn.createPrimaryKeyColumnWithIndexAndName(index, name);
  }
  if (column.geometryType) {
    return FeatureColumn.createGeometryColumn(index, name, 'GEOMETRY', notNull, defaultValue);
  }
  return FeatureColumn.createColumn(index, name, dataType, notNull, defaultValue, max)
}

exports.cloneFeature = async (pkg, inputFeature, outputFeatureName) => {
  const { table, geometryColumns, srs, dataColumnsDao } = inputFeature
  const { srs_id } = srs

  const columns = table.columns.map(t => featureColumnFromExistingTable(t))
  await pkg.createFeatureTable(
    outputFeatureName,
    { ...geometryColumns, table_name: outputFeatureName },
    columns,
    inputFeature.getBoundingBox(),
    srs_id,
    dataColumnsDao.getDataColumns()
  )

  return pkg.getFeatureDao(outputFeatureName)
}

exports.deleteFeature = (pkg, tableName) => {
  if (!pkg.hasFeatureTable(tableName)) {
    throw new Error('No such feature table');
  }

  const tables = pkg.connection.all(`SELECT name from sqlite_master where type = 'table' and name LIKE 'rtree_${tableName}_geom%'`);

  [tableName, ...tables].map((t) => { pkg.connection.dropTable(t) });

  [
    'gpkg_ogr_contents',
    'gpkg_geometry_columns',
    'gpkg_contents',
  ].map((t) => { pkg.connection.delete(t, 'table_name = :tableName', { tableName })})
}

exports.mergePoints = (points, options) => {
  const { fieldsToMean } = { fieldsToMean: [], ...options }

  return Object.keys(fieldsToMean).reduce((acc, field) => {
    acc[field] = meanBy(points, field)
    return acc
  }, points[0])
}

exports.createFeatureAsync = (feature, object) => {
  return new Promise(resolve => {
    const recordId = feature.create(object);
    resolve(recordId);
  })
}
