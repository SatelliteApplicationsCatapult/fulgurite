# Fulgurite

[![Build Status](https://travis-ci.org/SatelliteApplicationsCatapult/fulgurite.svg?branch=master)](https://travis-ci.org/SatelliteApplicationsCatapult/fulgurite)
[![License](https://img.shields.io/badge/licence-LGPLv3-green.svg?style=flat)](https://github.com/SatelliteApplicationsCatapult/fulgurite/blob/master/LICENSE.txt)

Fulgurite is a way to use Apache Spark to process GeoTIF images in a distributed way.

[Fulgurite](https://en.wikipedia.org/wiki/Fulgurite) is also the name for a rock formed by a lightning strike with the
ground.

## Overview

This project mainly consists of functions in the `GeoSparkUtils` class which allow the reading and writing of GeoTiff
files.

GeoTif files can be read using the `GeoSparkUtils.GeoTiffRDD()` function, this will return an RDD containing index of
pixels and band values.

Writing GeoTiff files is done with `GeoSparkUtils.saveGeoTiff()`. The results of this then need to be joined together
with the join output methods in `GeoSparkUtils`. The header will be in header.tiff and the body in the part-* files. A 
line like the following should join things up:

```GeoSparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), opts("output") + "/data.tif")```

The Class `MangleColours` gives a good example of how to use these. The `org.catapult.sa.fulgurite.examples` package
contains several other examples.

## Deployment and Usage

Currently this project is not in Maven. We will be working on that soon. Build a Jar from this project with the
`mvn package` target or use the `mvn install`, and then include the following for your dependency:

Maven:
```
  <dependency>
    <groupId>org.catapult.sa</groupId>
    <artifactId>fulgurite</artifactId>
    <version>0.1-SNAPSHOT</version>
  </dependency>
```
Gradle:
```
compile 'org.catapult.sa:fulgurite:0.1-SNAPSHOT'
```

**Please note:** building on windows requires the use of the hadoop `winutils.exe` and the `HADOOP_HOME` environment 
variable being set so it can be found.

If there are any problems with this process, please raise a bug.

## Contributing
All contributions to this project are warmly welcomed. If you have found bugs please raise them. We probably won't find 
it if you don't tell us about it. Pull requests with new features or bug fixes are extremely welcome. Please make sure
there are test cases (We don't want the test debt to get worse) and that the code is formatted similar to existing code.
New use cases or feature requests are also welcome.

Above all else, keep things civil.

Please see issues list (filtered by enhancement) for a current list of features we want to implement. The issues list is
 a good place to find things that need fixing. If there is some thing that we think is easy to fix but we don't have 
 time to deal with we will tag it.

If you are trying to fix something and have any questions please get in contact we will be happy to try and explain 
things if we can.

## License
This project is licensed under the [LGPL3](https://github.com/SatelliteApplicationsCatapult/fulgurite/blob/master/LICENSE.txt).

## Thanks
Thanks to the [Apache Spark](https://github.com/apache/spark) project and [JAI](https://github.com/jai-imageio/jai-imageio-core) 
for building the code this is built on top of. Thanks to Mike Nidel for the GeoTiff IIO Metadata Adapter which we used 
early in this project and taught us a lot about the GeoTiff format.
