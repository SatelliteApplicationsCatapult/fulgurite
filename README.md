# Fulgurite

Fulgurite is a way to use Apache Spark to process GeoTIF images in a distributed way

[Fulgurite](https://en.wikipedia.org/wiki/Fulgurite) is also the name for a rock formed by a lightning strike with the
ground.

## Overview

This project mainly consists of utility functions in the `GeoSparkUtils` and `SparkUtils` classes.

GeoTif files can be read using the `GeoSparkUtils.GeoTiffRDD()` function, this will return an RDD containing index of
pixels and band values.

Writing GeoTiff files is done with `GeoSparkUtils.saveGeoTiff()` the results of this then need to be joined together
with the join output methods in `SparkUtils` The header will be in header.tif and the body in the part-* files. A line
like the following should join things up

```SparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), opts("output") + "/data.tif")```

The Class `MangleColours` gives a good example of how to use these. The `org.catapult.sa.fulgurite.examples` package
contains several other examples.

## Deployment and Usage

Currently this project is not in Maven. We will be working on that soon. Build a Jar from this project with the
`mvn package` target or use the `mvn install` and then include the following for your dependency

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
if you don't tell us about it. Pull requests with new features or bug fixes are extremely welcome. Please make sure
there are test cases (We don't want the test debt to get worse) and that the code is formatted similar to existing code.
New use cases or feature requests are also welcome.

Above all else keep things civil please.

## License
This project is licensed under the LGPL3

## Thanks
Thanks to the spark project and JAI for building code this is built on top of. Thanks to Mike Nidel for the GeoTiff
metadata adapter which we converted to Scala and updated a bit.
