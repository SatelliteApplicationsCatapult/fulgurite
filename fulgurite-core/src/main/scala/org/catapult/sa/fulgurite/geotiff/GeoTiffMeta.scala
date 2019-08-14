package org.catapult.sa.fulgurite.geotiff

import java.io.{File, IOException}
import javax.imageio.ImageIO
import javax.imageio.metadata.IIOMetadata

import com.github.jaiimageio.plugins.tiff.BaselineTIFFTagSet

/**
  * Metadata for geo tiff files
  */
case class GeoTiffMeta(var width : Long, var height : Long,
                       var samplesPerPixel : Int, var bitsPerSample: Array[Int],
                       var startOffset : Long, var endOffset : Long,
                       var modelTiePoints : Array[Double],
                       var pixelScales : Array[Double],
                       var photometricInterpretation : Int,
                       var planarConfiguration : Int,
                       var extraSamples : Array[Char],
                       var sampleFormat : Array[Int],
                       var geoAsciiParams : String,
                       var xResolution : Array[Long], var yResolution : Array[Long],
                       var compression: Int,
                       var geoKeyDirectory : Array[Int],
                       var rowsPerStrip: Int,
                       var geoDoubleParams: Array[Double],
                       var stripByteCounts: Array[Long],
                       var stripOffsets: Array[Long]) {

  def bytesPerSample = bitsPerSample.map(b => (b + 7) / 8)

  def isCompressed = this.compression != BaselineTIFFTagSet.COMPRESSION_NONE

  // TODO: Rebuild toString method
}

object GeoTiffMeta {

  def apply(meta: IIOMetadata) : GeoTiffMeta = {
    val geoMeta = new GeoTiffMetaHelper(meta)

    new GeoTiffMeta(
      geoMeta.width, geoMeta.height,
      geoMeta.samplesPerPixel, geoMeta.bitsPerSample,
      geoMeta.firstOffset, geoMeta.endOffset,
      geoMeta.modelTiePoints, geoMeta.pixelScales,
      geoMeta.photometricInterpretation, geoMeta.planarConfiguration,
      geoMeta.extraSamples, geoMeta.sampleFormats,
      geoMeta.geoAsciiParams,
      geoMeta.xResolution, geoMeta.yResolution,
      geoMeta.compression,
      geoMeta.geoKeyDirectory,
      geoMeta.rowsPerStrip,
      geoMeta.geoDoubleParams,
      geoMeta.stripByteCounts,
      geoMeta.stripOffsets
    )

  }

  def apply(old : GeoTiffMeta) : GeoTiffMeta = GeoTiffMeta(
    old.width, old.height,
    old.samplesPerPixel, old.bitsPerSample,
    old.startOffset, old.endOffset,
    old.modelTiePoints, old.pixelScales,
    old.photometricInterpretation, old.planarConfiguration,
    old.extraSamples, old.sampleFormat,
    old.geoAsciiParams,
    old.xResolution, old.yResolution,
    old.compression,
    old.geoKeyDirectory,
    old.rowsPerStrip,
    old.geoDoubleParams,
    old.stripByteCounts,
    old.stripOffsets
  )

  def apply(file : File) : GeoTiffMeta = {
    if (file == null || !file.canRead || !file.isFile) {
      throw new IOException("Can not read " + file.getAbsolutePath)
    }

    val iis = ImageIO.createImageInputStream(file)
    val readers = ImageIO.getImageReaders(iis)

    if (readers.hasNext) {
      val reader = readers.next()
      reader.setInput(iis, true, false)
      apply(reader.getImageMetadata(0))
    } else {
      throw new IOException("could not read metadata")
    }
  }

  def apply(fileName : String) : GeoTiffMeta = {
    apply(new File(fileName))
  }
}
