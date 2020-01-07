import java.io.File

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try
object FileUtils {
  @tailrec
  final def listFilesRec(dirs: Seq[File], files: Seq[File] = Seq.empty): Seq[File] = {
    val f = Await.result(Future.sequence(dirs.map(d => Future{Try { d.listFiles().toSeq}.recover{
      case e => Seq.empty[File]
    }})), Duration.Inf)
      .map(_.get)
    val (newFiles, newDirs) = f.flatten.partition(_.isFile)
    newDirs match {
      case e if e.isEmpty => files ++ newFiles
      case _ => listFilesRec(newDirs, files ++ newFiles)
    }
  }
}
