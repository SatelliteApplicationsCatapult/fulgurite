package org.catapult.sa.geotiff

/*
 * (c) 2004 Mike Nidel
 *
 * Take, Modify, Distribute freely
 * Buy, Sell, Pass it off as your own
 *

This code is now made available under the MIT License:

The MIT License (MIT)

Copyright (c) 2004 by Mike Nidel

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

 * If you feel like it, send any suggestions for improvement or
 * bug fixes, or modified source code to mike 'at' gelbin.org
 *
 * Do not taunt Happy Fun Ball.
 *
 */

import com.github.jaiimageio.plugins.tiff.GeoTIFFTagSet
import javax.imageio.metadata.IIOMetadata
import javax.imageio.metadata.IIOMetadataNode
import org.w3c.dom.Node
import org.w3c.dom.NodeList

/**
  * Modified and converted to scala Wil Selwood 2016
  */
object GeoTiffIIOMetadataAdapter {
  // The following values are taken from the GeoTIFF specification


  // GeoTIFF Configuration GeoKeys

  val ImageWidth : Int = 256
  val ImageLength : Int = 257

  val BitsPerSample : Int = 258
  val SamplesPerPixel: Int = 277

  val StripOffsets : Int = 273
  val StripByteCounts : Int = 279

  /**
    * GTModelTypeGeoKey
    * Key ID = 1024
    * Type: SHORT (code)
    * Values:  Section 6.3.1.1 Codes
    * This GeoKey defines the general type of model Coordinate system
    * used, and to which the raster space will be transformed:
    * unknown, Geocentric (rarely used), Geographic, Projected
    * Coordinate System, or user-defined. If the coordinate system
    * is a PCS, then only the PCS code need be specified. If the
    * coordinate system does not fit into one of the standard
    * registered PCS'S, but it uses one of the standard projections
    * and datums, then its should be documented as a PCS model with
    * "user-defined" type, requiring the specification of projection
    * parameters, etc.
    *
    * GeoKey requirements for User-Defined Model Type (not advisable):
    *     GTCitationGeoKey
    *
    */
  val GTModelTypeGeoKey: Int = 1024

  /**
    * GTRasterTypeGeoKey
    * Key ID = 1025
    * Type =  Section 6.3.1.2 codes
    *
    * This establishes the Raster Space coordinate system used;
    * there are currently only two, namely RasterPixelIsPoint and
    * RasterPixelIsArea. No user-defined raster spaces are currently
    * supported. For variance in imaging display parameters, such as
    * pixel aspect-ratios, use the standard TIFF 6.0 device-space tags
    * instead.
    *
    */
  val GTRasterTypeGeoKey: Int = 1025

  /**
    * GTCitationGeoKey
    * Key ID = 1026
    * Type = ASCII
    *
    * As with all the "Citation" GeoKeys, this is provided to give
    * an ASCII reference to published documentation on the overall
    * configuration of this GeoTIFF file.
    *
    */
  val GTCitationGeoKey: Int = 1026

  // Geographic Coordinate System Parameter GeoKeys

  /**
    * GeographicTypeGeoKey
    * Key ID = 2048
    * Type = SHORT (code)
    * Values =  Section 6.3.2.1 Codes
    * http://www.remotesensing.org/geotiff/spec/geotiff6.html#6.3.2.1
    *
    * This key may be used to specify the code for the geographic
    * coordinate system used to map lat-long to a specific ellipsoid
    * over the earth.
    *
    * GeoKey Requirements for User-Defined geographic CS:
    *
    *  GeogCitationGeoKey
    *  GeogGeodeticDatumGeoKey
    *  GeogAngularUnitsGeoKey (if not degrees)
    *  GeogPrimeMeridianGeoKey (if not Greenwich)
    *
    */
  val GeographicTypeGeoKey: Int = 2048

  /**
    * GeogCitationGeoKey
    * Key ID = 2049
    * Type = ASCII
    * Values = text
    *
    * General citation and reference for all Geographic CS parameters.
    *
    */
  val GeogCitationGeoKey: Int = 2049

  /**
    * GeogGeodeticDatumGeoKey
    * Key ID = 2050
    * Type = SHORT (code)
    * Values =  Section 6.3.2.2 Codes
    * http://www.remotesensing.org/geotiff/spec/geotiff6.html#6.3.2.2
    *
    * This key may be used to specify the horizontal datum,
    * defining the size, position and orientation of the reference
    * ellipsoid used in user-defined geographic coordinate systems.
    *
    * GeoKey Requirements for User-Defined Horizontal Datum:
    *        GeogCitationGeoKey
    *        GeogEllipsoidGeoKey
    *
    */
  val GeogGeodeticDatumGeoKey: Int = 2050

  /**
    * GeogPrimeMeridianGeoKey
    * Key ID = 2051
    * Type = SHORT (code)
    * Units:  Section 6.3.2.4 code
    * http://www.remotesensing.org/geotiff/spec/geotiff6.html#6.3.2.4
    *
    * Allows specification of the location of the Prime meridian
    * for user-defined geographic coordinate systems. The default
    * standard is Greenwich, England.
    *
    */
  val GeogPrimeMeridianGeoKey: Int = 2051

  /**
    * GeogPrimeMeridianLongGeoKey
    * Key ID = 2061
    * Type = DOUBLE
    * Units =  GeogAngularUnits
    *
    * This key allows definition of user-defined Prime Meridians,
    * the location of which is defined by its longitude relative to
    * Greenwich.
    *
    */
  val GeogPrimeMeridianLongGeoKey: Int = 2061

  /**
    * GeogLinearUnitsGeoKey
    * Key ID = 2052
    * Type = SHORT
    * Values:  Section 6.3.1.3 Codes
    * http://www.remotesensing.org/geotiff/spec/geotiff6.html#6.3.1.3
    *
    * Allows the definition of geocentric CS linear units for
    * user-defined GCS.
    *
    */
  val GeogLinearUnitsGeoKey: Int = 2052

  /**
    * GeogLinearUnitSizeGeoKey
    * Key ID = 2053
    * Type = DOUBLE
    * Units: meters
    *
    * Allows the definition of user-defined linear geocentric units,
    * as measured in meters.
    *
    */
  val GeogLinearUnitSizeGeoKey: Int = 2053

  /**
    * GeogAngularUnitsGeoKey
    * Key ID = 2054
    * Type = SHORT (code)
    * Values =   Section 6.3.1.4  Codes
    *
    * Allows the definition of geocentric CS Linear units for user-defined GCS and for ellipsoids.
    *
    * GeoKey Requirements for "user-defined" units:
    *     GeogCitationGeoKey
    *     GeogAngularUnitSizeGeoKey
    *
    */
  val GeogAngularUnitsGeoKey: Int = 2054


  /**
    * GeogAngularUnitSizeGeoKey
    * Key ID = 2055
    * Type = DOUBLE
    * Units: radians
    *
    * Allows the definition of user-defined angular geographic units,
    * as measured in radians.
    *
    */
  val GeogAngularUnitSizeGeoKey: Int = 2055

  /**
    * GeogEllipsoidGeoKey
    * Key ID = 2056
    * Type = SHORT (code)
    * Values =  Section 6.3.2.3 Codes
    * http://www.remotesensing.org/geotiff/spec/geotiff6.html#6.3.2.3
    *
    * This key may be used to specify the coded ellipsoid used in the
    * geodetic datum of the Geographic Coordinate System.
    *
    * GeoKey Requirements for User-Defined Ellipsoid:
    *    GeogCitationGeoKey
    *    [GeogSemiMajorAxisGeoKey,
    *            [GeogSemiMinorAxisGeoKey | GeogInvFlatteningGeoKey] ]
    *
    */
  val GeogEllipsoidGeoKey: Int = 2056

  /**
    * GeogSemiMajorAxisGeoKey
    * Key ID = 2057
    * Type = DOUBLE
    * Units: Geocentric CS Linear Units
    *
    * Allows the specification of user-defined Ellipsoid
    * Semi-Major Axis (a).
    *
    */
  val GeogSemiMajorAxisGeoKey: Int = 2057

  /**
    * GeogSemiMinorAxisGeoKey
    * Key ID = 2058
    * Type = DOUBLE
    * Units: Geocentric CS Linear Units
    *
    * Allows the specification of user-defined Ellipsoid
    * Semi-Minor Axis (b).
    *
    */
  val GeogSemiMinorAxisGeoKey: Int = 2058

  /**
    * GeogInvFlatteningGeoKey
    * Key ID = 2059
    * Type = DOUBLE
    * Units: none.
    *
    * Allows the specification of the inverse of user-defined
    * Ellipsoid's flattening parameter (f). The eccentricity-squared
    * e^2 of the ellipsoid is related to the non-inverted f by:
    *      e^2  = 2*f  - f^2
    *
    *   Note: if the ellipsoid is spherical the inverse-flattening
    *   becomes infinite; use the GeogSemiMinorAxisGeoKey instead, and
    *   set it equal to the semi-major axis length.
    *
    */
  val GeogInvFlatteningGeoKey: Int = 2059

  /**
    * GeogAzimuthUnitsGeoKey
    * Key ID = 2060
    * Type = SHORT (code)
    * Values =   Section 6.3.1.4 Codes
    *
    * This key may be used to specify the angular units of
    * measurement used to defining azimuths, in geographic
    * coordinate systems. These may be used for defining azimuthal
    * parameters for some projection algorithms, and may not
    * necessarily be the same angular units used for lat-long.
    *
    */
  val GeogAzimuthUnitsGeoKey: Int = 2060

  // Projected Coordinate System Parameter GeoKeys

  /**
    * ProjectedCSTypeGeoKey
    * Key ID = 3072
    * Type = SHORT (codes)
    * Values:  Section 6.3.3.1 codes
    * This code is provided to specify the projected coordinate system.
    *
    * GeoKey requirements for "user-defined" PCS families:
    *    PCSCitationGeoKey
    *    ProjectionGeoKey
    *
    */
  val ProjectedCSTypeGeoKey: Int = 3072

  /**
    * PCSCitationGeoKey
    * Key ID = 3073
    * Type = ASCII
    *
    * As with all the "Citation" GeoKeys, this is provided to give
    * an ASCII reference to published documentation on the Projected
    * Coordinate System particularly if this is a "user-defined" PCS.
    *
    */
  val PCSCitationGeoKey: Int = 3073

  // Projection Definition GeoKeys
  //
  // With the exception of the first two keys, these are mostly
  // projection-specific parameters, and only a few will be required
  // for any particular projection type. Projected coordinate systems
  // automatically imply a specific projection type, as well as
  // specific parameters for that projection, and so the keys below
  // will only be necessary for user-defined projected coordinate
  // systems.

  /**
    * ProjectionGeoKey
    * Key ID = 3074
    * Type = SHORT (code)
    * Values:   Section 6.3.3.2 codes
    * http://www.remotesensing.org/geotiff/spec/geotiff6.html#6.3.3.2
    *
    * Allows specification of the coordinate transformation method and projection zone parameters. Note : when associated with an appropriate Geographic Coordinate System, this forms a Projected Coordinate System.
    *
    * GeoKeys Required for "user-defined" Projections:
    *   PCSCitationGeoKey
    *   ProjCoordTransGeoKey
    *   ProjLinearUnitsGeoKey
    *   (additional parameters depending on ProjCoordTransGeoKey).
    *
    */
  val ProjectionGeoKey: Int = 3074

  /**
    * ProjCoordTransGeoKey
    * Key ID = 3075
    * Type = SHORT (code)
    * Values:   Section 6.3.3.3 codes
    * http://www.remotesensing.org/geotiff/spec/geotiff6.html#6.3.3.3
    *
    * Allows specification of the coordinate transformation method used.
    * Note: this does not include the definition of the corresponding
    * Geographic Coordinate System to which the projected CS is related;
    * only the transformation method is defined here.
    *
    *GeoKeys Required for "user-defined" Coordinate Transformations:
    *   PCSCitationGeoKey
    *   (additional parameter geokeys depending on the Coord. Trans.
    *    specified).
    *
    */
  val ProjCoordTransGeoKey: Int = 3075

  /**
    * ProjLinearUnitsGeoKey
    * Key ID = 3076
    * Type = SHORT (code)
    * Values:  Section 6.3.1.3 codes
    *
    * Defines linear units used by this projection.
    * http://www.remotesensing.org/geotiff/spec/geotiff6.html#6.3.1.3
    *
    */
  val ProjLinearUnitsGeoKey: Int = 3076

  /**
    * ProjLinearUnitSizeGeoKey
    * Key ID = 3077
    * Type = DOUBLE
    * Units: meters
    *
    * Defines size of user-defined linear units in meters.
    *
    */
  val ProjLinearUnitSizeGeoKey: Int = 3077

  /**
    * ProjStdParallel1GeoKey
    * Key ID = 3078
    * Type = DOUBLE
    * Units: GeogAngularUnit
    * Alias: ProjStdParallelGeoKey (from Rev 0.2)
    *
    * Latitude of primary Standard Parallel.
    *
    */
  val ProjStdParallel1GeoKey: Int = 3078

  /**
    * ProjStdParallel2GeoKey
    * Key ID = 3079
    * Type = DOUBLE
    * Units: GeogAngularUnit
    *
    * Latitude of second Standard Parallel.
    *
    */
  val ProjStdParallel2GeoKey: Int = 3079

  /**
    * ProjNatOriginLongGeoKey
    * Key ID = 3080
    * Type = DOUBLE
    * Units: GeogAngularUnit
    * Alias: ProjOriginLongGeoKey
    *
    * Longitude of map-projection Natural origin.
    *
    */
  val ProjNatOriginLongGeoKey: Int = 3080

  /**
    * ProjNatOriginLatGeoKey
    * Key ID = 3081
    * Type = DOUBLE
    * Units: GeogAngularUnit
    * Alias: ProjOriginLatGeoKey
    *
    * Latitude of map-projection Natural origin.
    *
    */
  val ProjNatOriginLatGeoKey: Int = 3081

  /**
    * ProjFalseEastingGeoKey
    * Key ID = 3082
    * Type = DOUBLE
    * Units: ProjLinearUnit
    * Gives the easting coordinate of the map projection Natural origin.
    *
    */
  val ProjFalseEastingGeoKey: Int = 3082

  /**
    * ProjFalseNorthingGeoKey
    * Key ID = 3083
    * Type = DOUBLE
    * Units: ProjLinearUnit
    * Gives the northing coordinate of the map projection Natural origin.
    *
    */
  val ProjFalseNorthingGeoKey: Int = 3083

  /**
    * ProjFalseOriginLongGeoKey
    * Key ID = 3084
    * Type = DOUBLE
    * Units: GeogAngularUnit
    * Gives the longitude of the False origin.
    *
    */
  val ProjFalseOriginLongGeoKey: Int = 3084

  /**
    * ProjFalseOriginLatGeoKey
    * Key ID = 3085
    * Type = DOUBLE
    * Units: GeogAngularUnit
    * Gives the latitude of the False origin.
    *
    */
  val ProjFalseOriginLatGeoKey: Int = 3085
  val ProjFalseOriginEastingGeoKey: Int = 3086
  val ProjFalseOriginNorthingGeoKey: Int = 3087
  val ProjCenterLongGeoKey: Int = 3088
  val ProjCenterLatGeoKey: Int = 3089
  val ProjCenterEastingGeoKey: Int = 3090
  val ProjCenterNorthingGeoKey: Int = 3091
  val ProjScaleAtNatOriginGeoKey: Int = 3092
  val ProjScaleAtCenterGeoKey: Int = 3093
  val ProjAzimuthAngleGeoKey: Int = 3094
  val ProjStraightVertPoleLongGeoKey: Int = 3095
  val VerticalCSTypeGeoKey: Int = 4096
  val VerticalCitationGeoKey: Int = 4097
  val VerticalDatumGeoKey: Int = 4098
  val VerticalUnitsGeoKey: Int = 4099
  val ModelTypeProjected: Int = 1
  val ModelTypeGeographic: Int = 2
  val ModelTypeGeocentric: Int = 3
  val RasterPixelIsArea: Int = 1
  val RasterPixelIsPoint: Int = 2
  val Linear_Meter: Int = 9001
  val Linear_Foot: Int = 9002
  val Linear_Foot_US_Survey: Int = 9003
  val Linear_Foot_Modified_American: Int = 9004
  val Linear_Foot_Clarke: Int = 9005
  val Linear_Foot_Indian: Int = 9006
  val Linear_Link: Int = 9007
  val Linear_Link_Benoit: Int = 9008
  val Linear_Link_Sears: Int = 9009
  val Linear_Chain_Benoit: Int = 9010
  val Linear_Chain_Sears: Int = 9011
  val Linear_Yard_Sears: Int = 9012
  val Linear_Yard_Indian: Int = 9013
  val Linear_Fathom: Int = 9014
  val Linear_Mile_International_Nautical: Int = 9015
  val Angular_Radian: Int = 9101
  val Angular_Degree: Int = 9102
  val Angular_Arc_Minute: Int = 9103
  val Angular_Arc_Second: Int = 9104
  val Angular_Grad: Int = 9105
  val Angular_Gon: Int = 9106
  val Angular_DMS: Int = 9107
  val Angular_DMS_Hemisphere: Int = 9108
  val GCS_NAD27: Int = 4267
  val GCS_NAD83: Int = 4269
  val GCS_WGS_72: Int = 4322
  val GCS_WGS_72BE: Int = 4324
  val GCS_WGS_84: Int = 4326
  val GCSE_WGS84: Int = 4030
  val PCS_WGS72_UTM_zone_1N: Int = 32201
  val PCS_WGS72_UTM_zone_60N: Int = 32260
  val PCS_WGS72_UTM_zone_1S: Int = 32301
  val PCS_WGS72_UTM_zone_60S: Int = 32360
  val PCS_WGS72BE_UTM_zone_1N: Int = 32401
  val PCS_WGS72BE_UTM_zone_1S: Int = 32501
  val PCS_WGS72BE_UTM_zone_60S: Int = 32560
  val PCS_WGS84_UTM_zone_1N: Int = 32601
  val PCS_WGS84_UTM_zone_60N: Int = 32660
  val PCS_WGS84_UTM_zone_1S: Int = 32701
  val PCS_WGS84_UTM_zone_60S: Int = 32760
  val GEO_KEY_DIRECTORY_VERSION_INDEX: Int = 0
  val GEO_KEY_REVISION_INDEX: Int = 1
  val GEO_KEY_MINOR_REVISION_INDEX: Int = 2
  val GEO_KEY_NUM_KEYS_INDEX: Int = 3
  val TIFF_IFD_TAG: String = "TIFFIFD"
  val TIFF_FIELD_TAG: String = "TIFFField"
  val TIFF_DOUBLES_TAG: String = "TIFFDoubles"
  val TIFF_DOUBLE_TAG: String = "TIFFDouble"
  val TIFF_SHORTS_TAG: String = "TIFFShorts"
  val TIFF_SHORT_TAG: String = "TIFFShort"
  val TIFF_LONG_TAG: String = "TIFFLong"
  val TIFF_ASCIIS_TAG: String = "TIFFAsciis"
  val TIFF_ASCII_TAG: String = "TIFFAscii"
  val NUMBER_ATTR: String = "number"
  val VALUE_ATTR: String = "value"
}

class GeoTiffIIOMetadataAdapter(val imageMetadata: IIOMetadata) {
  val formatName: String = imageMetadata.getNativeMetadataFormatName
  private val myRootNode = imageMetadata.getAsTree(formatName).asInstanceOf[IIOMetadataNode]

  class GeoKeyRecord(var myKeyID: Int, var myTiffTagLocation: Int, var myCount: Int, var myValueOffset: Int) {
    def getKeyID: Int = {
      myKeyID
    }

    def getTiffTagLocation: Int = {
      myTiffTagLocation
    }

    def getCount: Int = {
      myCount
    }

    def getValueOffset: Int = {
      myValueOffset
    }
  }



  def getGeoKeyDirectoryVersion: Int = {
    val geoKeyDir: IIOMetadataNode = getTiffField(GeoTIFFTagSet.TAG_GEO_KEY_DIRECTORY)
    if (geoKeyDir == null) {
      throw new UnsupportedOperationException("GeoKey directory does not exist")
    }
    val result: Int = getTiffShort(geoKeyDir, GeoTiffIIOMetadataAdapter.GEO_KEY_DIRECTORY_VERSION_INDEX)
    result
  }

  def getGeoKeyRevision: Int = {
    val geoKeyDir: IIOMetadataNode = getTiffField(GeoTIFFTagSet.TAG_GEO_KEY_DIRECTORY)
    if (geoKeyDir == null) {
      throw new UnsupportedOperationException("GeoKey directory does not exist")
    }
    val result: Int = getTiffShort(geoKeyDir, GeoTiffIIOMetadataAdapter.GEO_KEY_REVISION_INDEX)
    result
  }

  def getGeoKeyMinorRevision: Int = {
    val geoKeyDir: IIOMetadataNode = getTiffField(GeoTIFFTagSet.TAG_GEO_KEY_DIRECTORY)
    if (geoKeyDir == null) {
      throw new UnsupportedOperationException("GeoKey directory does not exist")
    }
    val result: Int = getTiffShort(geoKeyDir, GeoTiffIIOMetadataAdapter.GEO_KEY_MINOR_REVISION_INDEX)
    result
  }

  def getNumGeoKeys: Int = {
    val geoKeyDir: IIOMetadataNode = getTiffField(GeoTIFFTagSet.TAG_GEO_KEY_DIRECTORY)
    if (geoKeyDir == null) {
      throw new UnsupportedOperationException("GeoKey directory does not exist")
    }
    val result: Int = getTiffShort(geoKeyDir, GeoTiffIIOMetadataAdapter.GEO_KEY_NUM_KEYS_INDEX)
    result
  }

  def getGeoKey(keyID: Int): String = {
    var result: String = null
    val rec: GeoKeyRecord = getGeoKeyRecord(keyID)
    if (rec != null) {
      if (rec.getTiffTagLocation == 0) {
        result = String.valueOf(rec.getValueOffset)
      }
      else {
        val field: IIOMetadataNode = getTiffField(rec.getTiffTagLocation)
        if (field != null) {
          val sequence: Node = field.getFirstChild
          if (sequence != null) {
            if (sequence.getNodeName == GeoTiffIIOMetadataAdapter.TIFF_ASCIIS_TAG) {
              result = getTiffAscii(sequence.asInstanceOf[IIOMetadataNode], rec.getValueOffset, rec.getCount)
            }
            else {
              val valueNodes: NodeList = sequence.getChildNodes
              val node: Node = valueNodes.item(rec.getValueOffset)
              result = getValueAttribute(node)
            }
          }
        }
      }
    }
    result
  }

  def getWidth : Long = {
    val rec = getTiffField(GeoTiffIIOMetadataAdapter.ImageWidth)
    if (rec != null) {
      return getTiffLong(rec, 0)
    }
    -1L
  }

  def getHeight : Long = {
    val rec = getTiffField(GeoTiffIIOMetadataAdapter.ImageLength)
    if (rec != null) {
      return getTiffLong(rec, 0)
    }
    -1L
  }

  def getBitsPerSample : Array[Int] = {
    val rec = getTiffField(GeoTiffIIOMetadataAdapter.BitsPerSample)
    if (rec != null) {
      return getTiffShorts(rec)
    }
    Array.empty[Int]
  }

  def getSamplesPerPixel : Int = {
    val rec = getTiffField(GeoTiffIIOMetadataAdapter.SamplesPerPixel)
    if (rec != null) {
      return getTiffShort(rec, 0)
    }
    -1
  }

  def getFirstStripOffset : Long = {
    val rec = getTiffField(GeoTiffIIOMetadataAdapter.StripOffsets)
    if (rec != null) {
      return getTiffLong(rec, 0)
    }

    -1L
  }

  def getEndOffset : Long = {
    val recOffsets = getTiffField(GeoTiffIIOMetadataAdapter.StripOffsets)
    val recBytes = getTiffField(GeoTiffIIOMetadataAdapter.StripByteCounts)
    if (recOffsets != null && recBytes != null) {
      return getTiffLongs(recOffsets).last + getTiffLongs(recBytes).last
    }
    -1L
  }

  def getGeoKeyRecord(keyID: Int): GeoKeyRecord = {
    val geoKeyDir: IIOMetadataNode = getTiffField(GeoTIFFTagSet.TAG_GEO_KEY_DIRECTORY)
    if (geoKeyDir == null) {
      throw new UnsupportedOperationException("GeoKey directory does not exist")
    }
    var result: GeoKeyRecord = null
    val tiffShortsNode: IIOMetadataNode = geoKeyDir.getFirstChild.asInstanceOf[IIOMetadataNode]
    val keys: NodeList = tiffShortsNode.getElementsByTagName(GeoTiffIIOMetadataAdapter.TIFF_SHORT_TAG)
    var i: Int = 4
    while (i < keys.getLength && result == null) {
      val n: Node = keys.item(i)
      val thisKeyID: Int = getIntValueAttribute(n)
      if (thisKeyID == keyID) {
        val locNode: Node = keys.item(i + 1)
        val countNode: Node = keys.item(i + 2)
        val offsetNode: Node = keys.item(i + 3)
        val loc: Int = getIntValueAttribute(locNode)
        val count: Int = getIntValueAttribute(countNode)
        val offset: Int = getIntValueAttribute(offsetNode)
        result = new GeoKeyRecord(thisKeyID, loc, count, offset)
      }

      i += 4
    }
    result
  }

  def getModelPixelScales: Array[Double] = {
    val modelTiePointNode: IIOMetadataNode = getTiffField(GeoTIFFTagSet.TAG_MODEL_PIXEL_SCALE)
    if (modelTiePointNode != null) {
      getTiffDoubles(modelTiePointNode)
    } else {
      Array.emptyDoubleArray
    }
  }

  def getModelTiePoints: Array[Double] = {
    var result: Array[Double] = null
    val modelTiePointNode: IIOMetadataNode = getTiffField(GeoTIFFTagSet.TAG_MODEL_TIE_POINT)
    if (modelTiePointNode != null) {
      result = getTiffDoubles(modelTiePointNode)
    }
    result
  }

  def getModelTransformation: Array[Double] = {
    var result: Array[Double] = null
    val modelTransNode: IIOMetadataNode = getTiffField(GeoTIFFTagSet.TAG_MODEL_TRANSFORMATION)
    if (modelTransNode != null) {
      result = getTiffDoubles(modelTransNode)
    }
    result
  }

  private def getValueAttribute(node: Node): String = {
    node.getAttributes.getNamedItem(GeoTiffIIOMetadataAdapter.VALUE_ATTR).getNodeValue
  }

  private def getIntValueAttribute(node: Node): Int = {
    getValueAttribute(node).toInt
  }

  // Tiff "Long" is 32 bits so is actually an Int
  private def getLongValueAttribute(node: Node): Long = {
    getValueAttribute(node).toLong
  }

  private def getTiffField(tag: Int): IIOMetadataNode = {
    var result: IIOMetadataNode = null
    val tiffDirectory: IIOMetadataNode = getTiffDirectory
    val children: NodeList = tiffDirectory.getElementsByTagName(GeoTiffIIOMetadataAdapter.TIFF_FIELD_TAG)
    var i: Int = 0
    while (i < children.getLength && result == null) {
      val child: Node = children.item(i)
      val number: Node = child.getAttributes.getNamedItem(GeoTiffIIOMetadataAdapter.NUMBER_ATTR)
      if (number != null) {
        val num: Int = number.getNodeValue.toInt
        if (num == tag) {
          result = child.asInstanceOf[IIOMetadataNode]
        }
      }

      i += 1
    }
    result
  }

  private def getTiffDirectory: IIOMetadataNode = {
    myRootNode.getFirstChild.asInstanceOf[IIOMetadataNode]
  }

  private def getTiffShorts(tiffField: IIOMetadataNode): Array[Int] = {
    val shortsElement: IIOMetadataNode = tiffField.getFirstChild.asInstanceOf[IIOMetadataNode]
    val shorts: NodeList = shortsElement.getElementsByTagName(GeoTiffIIOMetadataAdapter.TIFF_SHORT_TAG)
    val result: Array[Int] = new Array[Int](shorts.getLength)
    var i: Int = 0
    while (i < shorts.getLength) {
      val node: Node = shorts.item(i)
      result(i) = getIntValueAttribute(node)
      i += 1
    }
    result
  }

  // Tiff "Long" is 32 bits so is actually an Int
  private def getTiffLongs(tiffField: IIOMetadataNode): Array[Long] = {
    val shortsElement: IIOMetadataNode = tiffField.getFirstChild.asInstanceOf[IIOMetadataNode]
    val longs: NodeList = shortsElement.getElementsByTagName(GeoTiffIIOMetadataAdapter.TIFF_LONG_TAG)
    val result: Array[Long] = new Array[Long](longs.getLength)
    var i: Int = 0
    while (i < longs.getLength) {
      val node: Node = longs.item(i)
      result(i) = getLongValueAttribute(node)
      i += 1
    }
    result
  }

  // Tiff "Long" is 32 bits so is actually an Int
  private def getTiffLong(tiffField: IIOMetadataNode, index: Int): Long = {
    val longsElement: IIOMetadataNode = tiffField.getFirstChild.asInstanceOf[IIOMetadataNode]
    val longs: NodeList = longsElement.getElementsByTagName(GeoTiffIIOMetadataAdapter.TIFF_LONG_TAG)
    if (longs != null && longs.getLength >= index + 1) {
      val node: Node = longs.item(index)
      getLongValueAttribute(node)
    } else {
      val shorts: NodeList = longsElement.getElementsByTagName(GeoTiffIIOMetadataAdapter.TIFF_SHORT_TAG)
      val node: Node = shorts.item(index)
      getLongValueAttribute(node)
    }
  }

  private def getTiffShort(tiffField: IIOMetadataNode, index: Int): Int = {
    val shortsElement: IIOMetadataNode = tiffField.getFirstChild.asInstanceOf[IIOMetadataNode]
    val shorts: NodeList = shortsElement.getElementsByTagName(GeoTiffIIOMetadataAdapter.TIFF_SHORT_TAG)
    val node: Node = shorts.item(index)
    val result: Int = getIntValueAttribute(node)
    result
  }

  private def getTiffDoubles(tiffField: IIOMetadataNode): Array[Double] = {
    val doublesElement: IIOMetadataNode = tiffField.getFirstChild.asInstanceOf[IIOMetadataNode]
    val doubles: NodeList = doublesElement.getElementsByTagName(GeoTiffIIOMetadataAdapter.TIFF_DOUBLE_TAG)
    val result: Array[Double] = new Array[Double](doubles.getLength)
    var i: Int = 0
    while (i < doubles.getLength) {
        val node: Node = doubles.item(i)
        result(i) = getValueAttribute(node).toDouble
        i += 1
    }
    result
  }

  private def getTiffDouble(tiffField: IIOMetadataNode, index: Int): Double = {
    val doublesElement: IIOMetadataNode = tiffField.getFirstChild.asInstanceOf[IIOMetadataNode]
    val doubles: NodeList = doublesElement.getElementsByTagName(GeoTiffIIOMetadataAdapter.TIFF_DOUBLE_TAG)
    val node: Node = doubles.item(index)
    val result: Double = getValueAttribute(node).toDouble
    result
  }

  private def getTiffAscii(tiffField: IIOMetadataNode, start: Int, length: Int): String = {
    val asciisElement: IIOMetadataNode = tiffField.getFirstChild.asInstanceOf[IIOMetadataNode]
    val asciis: NodeList = asciisElement.getElementsByTagName(GeoTiffIIOMetadataAdapter.TIFF_ASCII_TAG)
    val node: Node = asciis.item(0)
    val result: String = getValueAttribute(node).substring(start, length - 1)
    result
  }
}