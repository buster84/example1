package controllers

import play.api._
import play.api.mvc._
import play.api.Play
import play.api.Play.current
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import play.api.libs.ws._
import play.api.libs.json._
import akka.actor.{ Actor, Props, Cancellable }
import akka.routing.RoundRobinRouter
import akka.pattern.{ask, pipe}
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future, ExecutionContext }
import scala.util.{ Success, Failure }

import play.api.libs.concurrent.Akka
import play.api.libs.json.{Json, JsValue, JsArray, JsError}
import play.api.libs.concurrent.Execution
import play.api.Play
import play.api.Logger

import org.joda.time.DateTime


object Application extends Controller {

  def index = Action {
    val worker = Akka.system.actorOf( JobWorker.props() )
    worker ! JobWorker.StartJob
    Ok("ok")
  }

  def api = Action {
    Ok(views.html.index("Your new application is ready."))
  }

}



object JobWorker {

  def props(): Props = {
    Props(classOf[JobWorker])
  }

  case object StartJob
  case class Result(visitorId : String, list: List[String])
}

class JobWorker() extends Actor {

  import JobWorker._
  private val leadWorkerRouter = context.actorOf( LeadWorker.props().withRouter( RoundRobinRouter( 5 ) ) )

  var processedVisitor = Set.empty[String]


  def receive = {
    case StartJob =>
      for(i <- 1 to 200000){
        processedVisitor = processedVisitor + i.toString
      }

      processedVisitor.foreach{ visitorId =>
        leadWorkerRouter ! LeadWorker.Work(visitorId)
      }

    case Result(visitorId, histories) =>
      processedVisitor = processedVisitor - visitorId
      if( processedVisitor.isEmpty ) context.stop(self)

  }
}
object LeadWorker {

  def props(): Props = {
    Props(classOf[LeadWorker])
  }

  case class Work(visitorId: String)
}

class LeadWorker() extends Actor {

  import LeadWorker._


  def receive = {
    case Work(visitorId) => {
      try {
        val resourceCache = Await.result({
          // The below api return { "aaaa": "aaaa" }
          WS.url("http://localhost:9001/").get.map{ res =>
            res.json.asOpt[JsObject]
          }
        }, Duration( 180, SECONDS ))

        sender ! JobWorker.Result(visitorId, List.empty[String])

      } catch {
        case e: Exception => {
          Logger.error("aaa", e)
        }
      }
    }
  }
}
