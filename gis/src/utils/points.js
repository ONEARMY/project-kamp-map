const { getDistance } = require('geolib')

const { mergePoints } = require('./geopkg_utils')

const DEFAULT_CONSECUTIVE_OPTIONS = {
  offset: 0.0001,
  fieldsToMean: ['lat', 'lon', 'alt', 'velocity', 'error_lon', 'error_lat', 'error_alt', 'error_velocity', 'error_total'],
}

exports.dedupeConsecutivePoints = (points, options) => {
  const { offset, fieldsToMean } = { ...DEFAULT_CONSECUTIVE_OPTIONS, ...options }

  const output = []
  let current = points.next()
  while (!current.done) {
    let next = points.next()
    if (next.done) { break }

    const from = current.value
    const to = next.value

    const distance = getDistance(
      { latitude: from.lat, longitude: from.lon },
      { latitude: to.lat, longitude: to.lon },
      offset
    )

    if (distance < offset) {
      output.push(mergePoints([from, to], fieldsToMean))
    }

    current = next
  }

  return output
}
