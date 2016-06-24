# Reasonable Excuse

Reasonable Excuse is a project to experiment with using spark to process GeoTIF images.

## Overview

This project mainly consists of utility functions in the `GeoSparkUtils`, `SparkUtils` and `SparkApplication` classes.

GeoTif files can be read using the `GeoSparkUtils.GeoTiffRDD()` function, this will return an RDD containing index of
pixels and colour values.

Writing geotiff files is done with `GeoSparkUtils.saveGeoTiff()` the results of this then need to be joined together
with the join output methods in `SparkUtils` The header will be in header.tif and the body in the part* files. A line
like the following should join things up

```SparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), "part-", opts("output") + "/data.tif")```

The Class `MangleColours` gives a good example of how to use these.

## License
This project is licensed under the LGPL3

## Thanks
Thanks to the spark project and JAI for building code this is built on top of.
