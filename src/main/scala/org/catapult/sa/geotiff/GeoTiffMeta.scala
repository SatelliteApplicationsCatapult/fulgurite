package org.catapult.sa.geotiff

import java.io.{File, IOException}
import javax.imageio.ImageIO
import javax.imageio.metadata.IIOMetadata

/**
  * Metadata for geo tiff files
  *
  * TODO: Create more stuff in here.
  */
case class GeoTiffMeta(width : Long, height : Long,
                       samplesPerPixel : Int, bitsPerSample: Array[Int],
                       startOffset : Long, endOffset : Long,
                       tiePoints : Array[Double], pixelScales : Array[Double]) {
  override def toString : String = {
    "GeoTiffMeta(width=" + width + " height=" + height +
      " samplesPerPixel=" + samplesPerPixel + " bitsPerSample=[" + bitsPerSample.mkString(", ") +
    "] startOffset=" + startOffset + " endOffset=" + endOffset +
      " tiePoints=[" + (if (tiePoints == null || tiePoints.isEmpty) { "" } else { tiePoints.mkString(", ") }) + "] pixelScales=[" + (if (pixelScales == null || pixelScales.isEmpty) { "" } else { pixelScales.mkString(", ") }) + "])"
  }
}

object GeoTiffMeta {

  def apply(meta: IIOMetadata) : GeoTiffMeta = {
    val geoMeta = new GeoTiffIIOMetadataAdapter(meta)

    GeoTiffMeta(geoMeta.getWidth,geoMeta.getHeight,
      geoMeta.getSamplesPerPixel, geoMeta.getBitsPerSample,
      geoMeta.getFirstStripOffset, geoMeta.getEndOffset,
      geoMeta.getModelTiePoints, geoMeta.getModelPixelScales)
  }


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
