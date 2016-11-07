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



  test("converts event with xml nodes having attributes to json") {
    val logEnricher = new LogEnricher
    val json =
      """
        {
        "timeMillis" : "1234567890",
        "message" : "requestType=TransferItems,requestData=<TransferId attr=\"someAttrValue\">123456</TransferId>"
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
            "TransferId" : {
              "attr":"someAttrValue",
              "content" : 123456
             }
          }
        } """.stripMargin))
  }

  test("converts event with xml node that has \n characters to json") {
    val logEnricher = new LogEnricher
    val json =
      """
        {
        "timeMillis" : "1234567890",
        "message" : "requestType=TransferItems,requestData=\n<TransferId>123456</TransferId>\n   <SomeId>someValue</SomeId>"
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
            "SomeId" : "someValue"
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

  test("converts xml with multiple weird nodes to json") {
    val logEnricher = new LogEnricher
    val json =
      """
        {
        "timeMillis" : "1234567890",
        "message" : "eventId=applicationId,applicationId=ScalTransferService,environment=development,requestType=HTTP-API-RECEIVE,hostName=local-server,hostPort=443,requestVersion=1.0,requestStatus=SUCCESS,requestName=PublishLPNDataRequest,requestHeaders=null,requestData=\n<PublishLPNDataRequest xmlns:ns2=\"http://service.nordstrom.net/scaltransfers/PKScalTransfersService/v1\" xmlns:ns3=\"http://service.nordstrom.net/scaltransfers/PKScalTransfersServiceTypes/v1\">\n    <ns2:CaseLPNData>\n        <ns3:CaseLPN>caseLPN</ns3:CaseLPN>\n        <ns3:UserID>userid</ns3:UserID>\n        <ns3:ShippedQuantity>0</ns3:ShippedQuantity>\n        <ns3:ReturnOrderNumber>ordernumber</ns3:ReturnOrderNumber>\n        <ns3:SKU>sku</ns3:SKU>\n        <ns3:Warehouse>warehouse</ns3:Warehouse>\n        <ns3:PathNumber>path</ns3:PathNumber>\n    </ns2:CaseLPNData>\n    <ns2:RequestContext>\n        <ns3:ApplicationId>applicationId</ns3:ApplicationId>\n        <ns3:SourceId>sourceId</ns3:SourceId>\n    </ns2:RequestContext>\n</PublishLPNDataRequest>\n"
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
