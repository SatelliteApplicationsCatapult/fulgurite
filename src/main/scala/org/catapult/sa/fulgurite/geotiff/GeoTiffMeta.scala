package org.catapult.sa.fulgurite.geotiff

import java.io.{File, IOException}
import javax.imageio.ImageIO
import javax.imageio.metadata.IIOMetadata

/**
  * Metadata for geo tiff files
  */
case class GeoTiffMeta(var width : Long, var height : Long,
                       var samplesPerPixel : Int, var bitsPerSample: Array[Int],
                       var startOffset : Long, var endOffset : Long,
                       var tiePoints : Array[Double],
                       var pixelScales : Array[Double],
                       var photometricInterpretation : Int,
                       var planarConfiguration : Int,
                       var extraSamples : Array[Int],
                       var sampleFormat : Array[Int],
                       var geoAsciiParams : String,
                       var xResolution : Array[Long], var yResolution : Array[Long],
                       var compression: Int,
                       var geoKeyDirectory : Array[Int]) {

  def bytesPerSample = bitsPerSample.map(b => (b + 7) / 8)

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
      geoMeta.geoKeyDirectory.map(_.toInt)
    )

  }

  def apply(old : GeoTiffMeta) : GeoTiffMeta = GeoTiffMeta(
    old.width, old.height,
    old.samplesPerPixel, old.bitsPerSample,
    old.startOffset, old.endOffset,
    old.tiePoints, old.pixelScales,
    old.photometricInterpretation, old.planarConfiguration,
    old.extraSamples, old.sampleFormat,
    old.geoAsciiParams,
    old.xResolution, old.yResolution,
    old.compression,
    old.geoKeyDirectory
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
