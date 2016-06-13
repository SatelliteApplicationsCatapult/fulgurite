# Reasonable Excuse

Reasonable Excuse is a project to experiment with using spark to process GeoTIF images.

## Overview

This project mainly consists of utility functions in the `GeoSparkUtils`, `SparkUtils` and `SparkApplication` classes.

GeoTif files can be read using the `GeoSparkUtils.GeoTiffRDD()` function, this will return an RDD containing index of
pixels and colour values.

Writing geotiff files is done with `GeoSparkUtils.saveGeoTiff()` the results of this then need to be joined together
with the join output methods in `SparkUtils`

