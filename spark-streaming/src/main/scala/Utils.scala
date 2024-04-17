import java.io.File
import java.nio.file.{Files, Paths}
import scala.util.{Failure, Success, Try}

object Utils {

  def transferCSV(csvPath: String, storeCSVPath: String): Unit = {
    val csvDirectory = new File(csvPath)
    val storeCSVDirectory = new File(storeCSVPath)

    if (!storeCSVDirectory.exists()) {
      storeCSVDirectory.mkdir()
    }

    if (csvDirectory.exists() && csvDirectory.isDirectory) {
      val csvFiles = csvDirectory.listFiles().filter(_.getName.endsWith(".csv"))
      csvFiles.foreach { file =>
        val sourcePath = file.toPath
        val destPath = Paths.get(storeCSVPath, file.getName)
        Try(Files.move(sourcePath, destPath)) match {
          case Success(_) => println(s"Moved ${file.getName} to $storeCSVPath")
          case Failure(e) => println(s"Failed to move ${file.getName}: ${e.getMessage}")
        }
      }
    } else {
      println("CSV directory does not exist or is not a directory")
    }
  }

}
