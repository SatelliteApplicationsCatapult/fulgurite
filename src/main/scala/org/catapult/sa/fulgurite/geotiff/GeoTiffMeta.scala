package org.catapult.sa.fulgurite.geotiff

import java.io.{File, IOException}
import javax.imageio.ImageIO
import javax.imageio.metadata.IIOMetadata

/**
  * Metadata for geo tiff files
  *
  * TODO: Create more stuff in here.
  */
case class GeoTiffMeta(var width : Long, var height : Long,
                       var samplesPerPixel : Int, var bitsPerSample: Array[Int],
                       var startOffset : Long, var endOffset : Long,
                       var tiePoints : Array[Double],
                       var pixelScales : Array[Double],
                       var colourMode : String,
                       var planarConfiguration : Int,
                       var extraSamples : Array[Int],
                       var sampleFormat : Array[Int]) {

  def bytesPerSample = bitsPerSample.map(b => (b + 7) / 8)

  override def toString : String = {
    "GeoTiffMeta(width=" + width + " height=" + height +
      " samplesPerPixel=" + samplesPerPixel + " bitsPerSample=[" + bitsPerSample.mkString(", ") +
    "] startOffset=" + startOffset + " endOffset=" + endOffset +
      " tiePoints=[" + (if (tiePoints == null || tiePoints.isEmpty) { "" } else { tiePoints.mkString(", ") }) +
      "] pixelScales=[" + (if (pixelScales == null || pixelScales.isEmpty) { "" } else { pixelScales.mkString(", ") }) +
      "] colourMode=" + colourMode + " planarConfiguration=" + planarConfiguration +
      " extraSamples=[" + extraSamples.mkString(", ") + "] sampleFormat=[" + sampleFormat.mkString(", ") + "])"
  }
}

object GeoTiffMeta {

  def apply(meta: IIOMetadata) : GeoTiffMeta = {
    val geoMeta = new GeoTiffIIOMetadataAdapter(meta)

    GeoTiffMeta(geoMeta.getWidth, geoMeta.getHeight,
      geoMeta.getSamplesPerPixel, geoMeta.getBitsPerSample,
      geoMeta.getFirstStripOffset, geoMeta.getEndOffset,
      geoMeta.getModelTiePoints, geoMeta.getModelPixelScales,
      geoMeta.getPhotometricInterpretation, geoMeta.getPlanarConfiguration,
      geoMeta.getExtraSamples, geoMeta.getSampleFormat)
  }

  def apply(old : GeoTiffMeta) : GeoTiffMeta = GeoTiffMeta(
    old.width, old.height,
    old.samplesPerPixel, old.bitsPerSample,
    old.startOffset, old.endOffset,
    old.tiePoints, old.pixelScales,
    old.colourMode, old.planarConfiguration,
    old.extraSamples, old.sampleFormat
  )

  def apply(file : File) : (GeoTiffMeta, IIOMetadata) = {
    if (file == null || !file.canRead || !file.isFile) {
      throw new IOException("Can not read " + file.getAbsolutePath)
    }

    val iis = ImageIO.createImageInputStream(file)
    val readers = ImageIO.getImageReaders(iis)

    if (readers.hasNext) {
      val reader = readers.next()
      reader.setInput(iis, true, false)
      val meta = reader.getImageMetadata(0)
      apply(meta) -> meta
    } else {
      throw new IOException("could not read metadata")
    }
  }

  def apply(fileName : String)  : (GeoTiffMeta, IIOMetadata) = {
    apply(new File(fileName))
  }
}
