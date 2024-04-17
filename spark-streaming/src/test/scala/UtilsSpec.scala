import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.{File, IOException}

class UtilsSpec extends AnyFlatSpec with Matchers {

  "transferCSV" should "move CSV files from source to destination" in {
    val csvPath = "src/test/resources/csv"
    val storeCSVPath = "src/test/resources/storeCSV"

    val csvDirectory = new File(csvPath)
    val storeCSVDirectory = new File(storeCSVPath)

    // Create the CSV directory if it doesn't exist
    if (!csvDirectory.exists()) {
      csvDirectory.mkdirs()
    }

    // Create the CSV directory if it doesn't exist
    if (!storeCSVDirectory.exists()) {
      storeCSVDirectory.mkdirs()
    }

    // Create a CSV file in the source directory
    val csvFile = new File(csvPath, "test.csv")
    csvFile.createNewFile()

    try{
      Utils.transferCSV(csvPath, storeCSVPath)

      // Check if the file was moved
      val movedFile = new File(storeCSVPath, "test.csv")
      movedFile.exists() should be(true)

      // Check if the file was removed from the source directory
      csvFile.exists() should be(false)
    }
    finally {
      // Cleanup: delete the test files and directories
      csvFile.delete()
      new File(storeCSVPath, "test.csv").delete()
      csvDirectory.delete()
      storeCSVDirectory.delete()
    }




  }



}
