package org.catapult.sa.fulgurite.geotiff

import javax.imageio.metadata.IIOMetadata

import com.github.jaiimageio.impl.plugins.tiff.{TIFFIFD, TIFFImageMetadata}
import com.github.jaiimageio.plugins.tiff.{BaselineTIFFTagSet, GeoTIFFTagSet, TIFFField, TIFFTag}

import collection.JavaConversions._

/**
  * Helper class to extract fields from GeoTIFF metadata
  *
  * Wraps up an instance of IOMetadata and provides readonly accessors to the required fields
  *
  * Implementation of metadata helper using jaiimageio rather than the GeoTiffIOMetadataAdapter
  */
class GeoTiffMetaHelper(baseMeta : IIOMetadata) {

  private val meta = baseMeta.asInstanceOf[TIFFImageMetadata].getRootIFD

  def width = getIntField(BaselineTIFFTagSet.TAG_IMAGE_WIDTH)
  def height = getIntField(BaselineTIFFTagSet.TAG_IMAGE_LENGTH)
  def samplesPerPixel = getIntField(BaselineTIFFTagSet.TAG_SAMPLES_PER_PIXEL)
  def bitsPerSample = getIntsField(BaselineTIFFTagSet.TAG_BITS_PER_SAMPLE)
  def firstOffset = getLongField(BaselineTIFFTagSet.TAG_STRIP_OFFSETS)
  def endOffset = getLongsField(BaselineTIFFTagSet.TAG_STRIP_OFFSETS).last + getLongsField(BaselineTIFFTagSet.TAG_STRIP_BYTE_COUNTS).last
  def modelTiePoints = getDoublesField(GeoTIFFTagSet.TAG_MODEL_TIE_POINT)
  def pixelScales = getDoublesField(GeoTIFFTagSet.TAG_MODEL_PIXEL_SCALE)
  def photometricInterpretation = getIntField(BaselineTIFFTagSet.TAG_PHOTOMETRIC_INTERPRETATION)
  def planarConfiguration = getIntField(BaselineTIFFTagSet.TAG_PLANAR_CONFIGURATION)
  def extraSamples =  baseGet(BaselineTIFFTagSet.TAG_EXTRA_SAMPLES, Array.empty[Char], _.getAsChars())
  def sampleFormats = getIntsField(BaselineTIFFTagSet.TAG_SAMPLE_FORMAT)
  def geoAsciiParams = getStringField(GeoTIFFTagSet.TAG_GEO_ASCII_PARAMS)
  def xResolution = getRationalField(BaselineTIFFTagSet.TAG_X_RESOLUTION)
  def yResolution = getRationalField(BaselineTIFFTagSet.TAG_Y_RESOLUTION)
  def compression = getIntField(BaselineTIFFTagSet.TAG_COMPRESSION)
  def geoKeyDirectory = getIntsField(GeoTIFFTagSet.TAG_GEO_KEY_DIRECTORY)
  def rowsPerStrip = getIntField(BaselineTIFFTagSet.TAG_ROWS_PER_STRIP)
  def stripByteCounts = getLongsField(BaselineTIFFTagSet.TAG_STRIP_BYTE_COUNTS)
  def stripOffsets = getLongsField(BaselineTIFFTagSet.TAG_STRIP_OFFSETS)
  def geoDoubleParams = getDoublesField(GeoTIFFTagSet.TAG_GEO_DOUBLE_PARAMS)

  private def getIntField(field : Int, offset : Int = 0) = baseGet(field, -1, _.getAsInt(offset))
  private def getIntsField(field : Int) = baseGet(field, Array.empty[Int], _.getAsInts())
  private def getLongField(field : Int, offset : Int = 0) = baseGet(field, -1L, _.getAsLong(offset))
  private def getLongsField(field : Int) = baseGet(field, Array.empty[Long], _.getAsLongs())
  private def getDoublesField(field : Int) = baseGet(field, Array.empty[Double], _.getAsDoubles())
  private def getStringField(field : Int) = baseGet(field, "", _.getAsString(0))
  private def getRationalField(field : Int, offset : Int = 0) = baseGet(field, Array(0L, 0L), _.getAsRational(offset))

  private def baseGet[T](field : Int, nullValue : T, extractor : TIFFField => T) : T = {
    meta.getTIFFField(field) match {
      case null => nullValue
      case f => extractor(f)
    }
  }
}

object GeoTiffMetaHelper {

  def createImageMetaData(meta : GeoTiffMeta) : TIFFImageMetadata = {

    val baseMeta = new TIFFImageMetadata(List(geoTiffBase, base))
    val rootIFD = baseMeta.getRootIFD

    // update the ifd and make sure it matches the meta object.
    setInt(rootIFD, BaselineTIFFTagSet.TAG_IMAGE_WIDTH, meta.width.toInt)
    setInt(rootIFD, BaselineTIFFTagSet.TAG_IMAGE_LENGTH, meta.height.toInt)
    setInt(rootIFD, BaselineTIFFTagSet.TAG_SAMPLES_PER_PIXEL, meta.samplesPerPixel)
    setShorts(rootIFD, BaselineTIFFTagSet.TAG_BITS_PER_SAMPLE, meta.bitsPerSample.map(_.toChar))
    setInt(rootIFD, BaselineTIFFTagSet.TAG_PLANAR_CONFIGURATION, meta.planarConfiguration)
    setInt(rootIFD, BaselineTIFFTagSet.TAG_PHOTOMETRIC_INTERPRETATION, meta.photometricInterpretation)
    setInt(rootIFD, BaselineTIFFTagSet.TAG_COMPRESSION, meta.compression)
    setGeoShorts(rootIFD, GeoTIFFTagSet.TAG_GEO_KEY_DIRECTORY, meta.geoKeyDirectory.map(_.toChar))

    // given we have just updated the size we should also update the number of offset and byte count places to be filled in later
    rootIFD.removeTIFFField(BaselineTIFFTagSet.TAG_STRIP_OFFSETS)
    rootIFD.removeTIFFField(BaselineTIFFTagSet.TAG_STRIP_BYTE_COUNTS)
    meta.planarConfiguration match {
      case BaselineTIFFTagSet.PLANAR_CONFIGURATION_CHUNKY => {
        setEmptyLongs(rootIFD, BaselineTIFFTagSet.TAG_STRIP_OFFSETS, meta.height.asInstanceOf[Int])
        setEmptyLongs(rootIFD, BaselineTIFFTagSet.TAG_STRIP_BYTE_COUNTS, meta.height.asInstanceOf[Int] * meta.samplesPerPixel)
      }
      case BaselineTIFFTagSet.PLANAR_CONFIGURATION_PLANAR => {
        setEmptyLongs(rootIFD, BaselineTIFFTagSet.TAG_STRIP_OFFSETS, meta.height.asInstanceOf[Int])
        setEmptyLongs(rootIFD, BaselineTIFFTagSet.TAG_STRIP_BYTE_COUNTS, meta.height.asInstanceOf[Int])
      }
      case _ => throw new IllegalArgumentException("Unknown planar configuration")
    }

    rootIFD.addTIFFField(new TIFFField(geoTiffBase.getTag(GeoTIFFTagSet.TAG_GEO_ASCII_PARAMS), TIFFTag.TIFF_ASCII, 1, Array[String](meta.geoAsciiParams)))

    setGeoDoubles(rootIFD, GeoTIFFTagSet.TAG_GEO_DOUBLE_PARAMS, meta.geoDoubleParams)

    setGeoDoubles(rootIFD, GeoTIFFTagSet.TAG_MODEL_TIE_POINT, meta.modelTiePoints)

    setGeoDoubles(rootIFD, GeoTIFFTagSet.TAG_MODEL_PIXEL_SCALE, meta.pixelScales)

    // Optional fields, when we don't have any data they should not be provided.
    if (meta.extraSamples.isEmpty) {
      rootIFD.removeTIFFField(BaselineTIFFTagSet.TAG_EXTRA_SAMPLES)
    } else {
      setShorts(rootIFD, BaselineTIFFTagSet.TAG_EXTRA_SAMPLES, meta.extraSamples)
    }

    if (meta.sampleFormat.isEmpty) {
      rootIFD.removeTIFFField(BaselineTIFFTagSet.TAG_SAMPLE_FORMAT)
    } else {
      setShorts(rootIFD, BaselineTIFFTagSet.TAG_SAMPLE_FORMAT, meta.sampleFormat.map(_.toChar))
    }

    baseMeta
  }

  private def setInt(rootIFD : TIFFIFD, field : Int, value : Int) =
    rootIFD.addTIFFField(new TIFFField(base.getTag(field), value))

  private def setRational(rootIFD : TIFFIFD, field : Int, value : Array[Long]) =
    rootIFD.addTIFFField(new TIFFField(base.getTag(field), TIFFTag.TIFF_RATIONAL, 1, Array(value)))

  private def setEmptyLongs(rootIFD : TIFFIFD, field : Int, numRows : Int) =
    rootIFD.addTIFFField(new TIFFField(base.getTag(field), TIFFTag.TIFF_LONG, numRows))

  private def setShorts(rootIFD : TIFFIFD, field : Int, value : Array[Char]) =
    rootIFD.addTIFFField(new TIFFField(base.getTag(field), TIFFTag.TIFF_SHORT, value.length, value))

  private def setBytes(rootIFD : TIFFIFD, field : Int, value : Array[Char]) =
    rootIFD.addTIFFField(new TIFFField(base.getTag(field), TIFFTag.TIFF_ASCII, value.length, value))

  private def setGeoShorts(rootIFD : TIFFIFD, field : Int, value : Array[Char]) =
    rootIFD.addTIFFField(new TIFFField(geoTiffBase.getTag(field), TIFFTag.TIFF_SHORT, value.length, value))

  private def setGeoDoubles(rootIFD : TIFFIFD, field : Int, value : Array[Double]) =
    rootIFD.addTIFFField(new TIFFField(geoTiffBase.getTag(field), TIFFTag.TIFF_DOUBLE, value.length, value))

  private val base = BaselineTIFFTagSet.getInstance
  private val geoTiffBase = GeoTIFFTagSet.getInstance
}