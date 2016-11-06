import com.google.gson.JsonParser
import com.pseudo.enricher.LogEnricher
import org.apache.flume.event.JSONEvent
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Created by prayagupd
  * on 9/28/16.
  */

class LogEnricherSpec extends FunSuite {

  test("converts key values to json") {
    val logEnricher = new LogEnricher
    val json =
      """
        {
        "timeMillis" : "1234567890",
        "message" : "key1=value1,key2=value2"
        }
      """.stripMargin

    val event = new JSONEvent()
    event.setBody(json.getBytes())

    val actualEvent = logEnricher.intercept(event)

    val parser = new JsonParser()

    assert(parser.parse(new String(actualEvent.getBody)) ==
      parser.parse("""
        {
          "timeMillis" : "1234567890",
          "key1":value1,
          "key2":value2
        } """.stripMargin))
  }

  test("converts event with xml to json") {
    val logEnricher = new LogEnricher
    val json =
      """
        {
        "timeMillis" : "1234567890",
        "message" : "requestType=TransferItems,requestData=<TransferId>123456</TransferId>"
        }
      """.stripMargin

    val event = new JSONEvent()
    event.setBody(json.getBytes())

    val actualEvent = logEnricher.intercept(event)

    val parser = new JsonParser()

    assert(parser.parse(new String(actualEvent.getBody)) ==
      parser.parse("""
        {
          "timeMillis" : "1234567890",
          "requestType" : "TransferItems",
          "requestData":{
            "TransferId" : 123456
          }
        } """.stripMargin))
  }


  test("converts event with weird xml to json") {
    val logEnricher = new LogEnricher
    val json =
      """
        {
        "timeMillis" : "1234567890",
        "message" : "requestType=TransferItems,requestData=<ns1:TransferId>123456</ns1:TransferId>"
        }
      """.stripMargin

    val event = new JSONEvent()
    event.setBody(json.getBytes())

    val actualEvent = logEnricher.intercept(event)

    val parser = new JsonParser()

    assert(parser.parse(new String(actualEvent.getBody)) ==
      parser.parse("""
        {
          "timeMillis" : "1234567890",
          "requestType" : "TransferItems",
          "requestData":{
            "ns1:TransferId" : 123456
          }
        } """.stripMargin))
  }

  test("converts xml with multiple nodes to json") {
    val logEnricher = new LogEnricher
    val json =
      """
        {
        "timeMillis" : "1234567890",
        "message" : "requestType=TransferItems,requestData=<TransferId>123456</TransferId><TransferDate>Sunday</TransferDate>"
        }
      """.stripMargin

    val event = new JSONEvent()
    event.setBody(json.getBytes())

    val actualEvent = logEnricher.intercept(event)

    val parser = new JsonParser()

    assert(parser.parse(new String(actualEvent.getBody)) ==
      parser.parse("""
        {
          "timeMillis" : "1234567890",
          "requestType" : "TransferItems",
          "requestData":{
            "TransferId" : 123456,
            "TransferDate" : "Sunday"
          }
        } """.stripMargin))
  }
}
