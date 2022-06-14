// https://riptutorial.com/akka/example/31018/akka-http-server--hello-world--scala-dsl-
import SparkMongo.{fetchAllJsonString, query1_date, query2_date, query3_date, query4_date, query5_date, query5_limit, query6_date, query6_sort, query7_country, query7_date, query8_country, query8_date}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
object Server extends App {

  implicit val system: ActorSystem = ActorSystem("ProxySystem")

  val route = pathPrefix("api") {
    concat(

      get {
        path("hello") {
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Welcome to the webpage of our server...</h1>"))
        }
      },

      get {
        path("all") {
          complete(HttpEntity(ContentTypes.`application/json`, fetchAllJsonString()))
        }
      },

      // Query - 1
      get {
        path("query1") {
          parameters('startdate.as[String], 'enddate.as[String]) { (startdate, enddate) =>
            complete {
              query1_date(startdate,enddate)
            }
          }
        }
      },

      // Query - 2
      get {
        path("query2") {
          parameters('startdate.as[String], 'enddate.as[String]) { (startdate, enddate) =>
            complete {
              query2_date(startdate,enddate)
            }
          }
        }
      },

      // Query - 3
      get {
        path("query3") {
          parameters('startdate.as[String], 'enddate.as[String]) { (startdate, enddate) =>
            complete {
              query3_date(startdate,enddate)
            }
          }
        }
      },

      // Query - 4
      get {
        path("query4") {
          parameters('startdate.as[String], 'enddate.as[String]) { (startdate, enddate) =>
            complete {
              query4_date(startdate,enddate)
            }
          }
        }
      },

      // Query - 5
      get {
        path("query5") {
          // Filters are start_date and end_date
          parameters('startdate.as[String], 'enddate.as[String]) { (startdate, enddate) =>
            complete {
              query5_date(startdate, enddate)
            }
          }
        }
      },
      get {
        path("query5") {
          // Filter are no. of tweets
          parameters('limit.as[Int]) { limit =>
            complete {
              query5_limit(limit)
            }
          }
        }
      },

      // Query - 6
      get {
        path("query6") {
          // Filters are start_date and end_date
          parameters('startdate.as[String], 'enddate.as[String]) { (startdate, enddate) =>
            complete {
              query6_date(startdate, enddate)
            }
          }
        }
      },
      get {
        path("query6") {
          // Filter is sortBy and no. of records
          parameters('sort.as[String], 'limit.as[Int]) { (sort, limit) =>
            complete {
              query6_sort(sort, limit)
            }
          }
        }
      },

      // Query - 7
      get {
        path("query7") {
          // Filters are start_date and end_date
          parameters('startdate.as[String], 'enddate.as[String]) { (startdate, enddate) =>
            complete {
              query7_date(startdate, enddate)
            }
          }
        }
      },
      get {
        path("query7") {
          // Filter is country name
          parameters('country.as[String]) { country =>
            complete {
              query7_country(country)
            }
          }
        }
      },

      // Query - 8
      get {
        path("query8") {
          // Filters are start_date and end_date
          parameters('startdate.as[String], 'enddate.as[String]) { (startdate, enddate) =>
            complete {
              query8_date(startdate, enddate)
            }
          }
        }
      },
      get {
        path("query8") {
          // Filter is country name
          parameters('country.as[String]) { country =>
            complete {
              query8_country(country)
            }
          }
        }
      }

    )
  }
  val bindingFuture = Http().newServerAt("127.0.0.1", port = 8086).bindFlow(route)
  Await.result(system.whenTerminated, Duration.Inf)

}