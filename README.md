# Fulgurite

Fulgurite is a project to use spark to process GeoTIF images.

## Overview

This project mainly consists of utility functions in the `GeoSparkUtils`, `SparkUtils` and `SparkApplication` classes.

GeoTif files can be read using the `GeoSparkUtils.GeoTiffRDD()` function, this will return an RDD containing index of
pixels and colour values.

Writing geotiff files is done with `GeoSparkUtils.saveGeoTiff()` the results of this then need to be joined together
with the join output methods in `SparkUtils` The header will be in header.tif and the body in the part* files. A line
like the following should join things up

```SparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), "part-", opts("output") + "/data.tif")```

The Class `MangleColours` gives a good example of how to use these. The `org.catapult.sa.example` package contains
several examples.

## Deployment and Usage

Currently this project is not in Maven. We will be working on that soon. Build a Jar from this project with the
`mvn package` target or use the `mvn install` and then include

```
  <dependency>
    <groupId>org.catapult.sa</groupId>
    <artifactId>fulgurite</artifactId>
    <version>1.0-SNAPSHOT</version>
  </dependency>
```

Any problems with this process please raise a bug.

## Contributing

All contributions to this project are warmly welcomed. If you have found bugs please raise them. We probably won't find
 if you don't tell us about it.

## License
This project is licensed under the LGPL3

## Thanks
Thanks to the spark project and JAI for building code this is built on top of.
