package org.catapult.sa.fulgurite.spark

/**
  * trait to handle arguments.
  */
trait Arguments {

  def defaultArgs(): Map[String, String]
  def allArgs() : List[Argument]

  private lazy val allowed = allArgs().map(a => a.name -> a).toMap

  def processArgs(args : Array[String], defaultArgs : Map[String, String]) : Map[String, String] = {

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

    loop(args.toList, defaultArgs)
  }

}

case class Argument(name : String, flag : Boolean = false)

object Argument {
  implicit def stringWrapper(s : String) : Argument = Argument(s)
}