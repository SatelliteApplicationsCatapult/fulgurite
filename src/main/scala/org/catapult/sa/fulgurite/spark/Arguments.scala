package org.catapult.sa.fulgurite.spark

/**
  * trait to handle arguments.
  */
trait Arguments {

  def allowedArgs() : List[Argument]

  private lazy val allowed = allowedArgs().map(a => a.name -> a).toMap

  def processArgs(args : Array[String]) : Map[String, String] = {

    def loop(a : List[String], result : Map[String, String]) : Map[String, String] = {
      a match {
        case List() => result
        case head :: tail =>
          allowed.get(head.stripPrefix("-").stripPrefix("-")) match {
            case Some(arg) =>
              if (arg.flag) {
                loop(tail, result + (arg.name -> "true"))
              } else {
                tail match {
                  case List() => throw new IllegalArgumentException("Missing parameter: " + head)
                  case value :: t => loop(t, result + (arg.name -> value))
                }
              }
            case None => throw new IllegalArgumentException("unknown argument: " + head)
          }
      }
    }

    loop(args.toList, allowed.map(e => e._1 -> e._2.defaultValue))
  }

}

case class Argument(name : String, defaultValue : String = "", flag : Boolean = false)