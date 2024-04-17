import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KafkaDataCSVTest extends AnyFlatSpec with Matchers {

  "generateEventData" should "return Some(EventData) for valid input" in {
    val validInput = "0,tcp,http,SF,181,5450,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,8,8,0.0,0.0,0.0,0.0,1.0,0.0,0.0,9,9,1.0,0.0,0.11,0.0,0.0,0.0,0.0,0.0,0.0"
    val result = KafkaDataCSV.generateEventData(validInput)
    result shouldBe a [Some[_]]
  }

  it should "return None for invalid input" in {
    val invalidInput = "invalid data"
    val result = KafkaDataCSV.generateEventData(invalidInput)
    result shouldBe None
  }

  it should "return None for empty input" in {
    val emptyInput = ""
    val result = KafkaDataCSV.generateEventData(emptyInput)
    result shouldBe None
  }

  it should "return None for out-of-range value" in {
    val outOfRangeValue = "2147483648" // Max Integer value + 1
    val input = s"0,tcp,http,SF,$outOfRangeValue,5450,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,8,8,0.0,0.0,0.0,0.0,1.0,0.0,0.0,9,9,1.0,0.0,0.11,0.0,0.0,0.0,0.0,0.0,0.0"
    val result = KafkaDataCSV.generateEventData(input)
    result shouldBe None
  }

  it should "return Some(EventData) for zero duration" in {
    val zeroDuration = "0"
    val input = s"$zeroDuration,tcp,http,SF,181,5450,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,8,8,0.0,0.0,0.0,0.0,1.0,0.0,0.0,9,9,1.0,0.0,0.11,0.0,0.0,0.0,0.0,0.0,0.0"
    val result = KafkaDataCSV.generateEventData(input)
    result shouldBe a [Some[_]]
  }

  it should "return correct EventData for valid input" in {
    val input = "0,tcp,http,SF,181,5450,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,8,8,0.0,0.0,0.0,0.0,1.0,0.0,0.0,9,9,1.0,0.0,0.11,0.0,0.0,0.0,0.0,0.0,0.0"
    val result = KafkaDataCSV.generateEventData(input).get
    result.duration shouldBe 0
    result.protocol_type shouldBe "tcp"
    result.service shouldBe "http"

  }



}
