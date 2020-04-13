package com.martinetherton

import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, IOResult}
import akka.stream.scaladsl.{FileIO, Flow, Framing, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

object Main extends App {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher

  val file = Paths.get("test.csv")
  val res: Source[ByteString, Future[IOResult]] = FileIO.fromPath(file)
  val f = res.via(Framing.delimiter(ByteString("\n"), 256, true)).map(_.utf8String)
  val sink = Sink.foreach[String](println(_))
  val runnable = f.toMat(sink)(Keep.right)
  val a = runnable.run()

  //  val source = Source(1 to 10)
//  val doubles = (x: Int) => x * 2
//  val doubleSource = source.map(doubles)
//  val sink = Sink.fold[Int, Int](0)(_ + _)
//
//  // connect the Source to the Sink, obtaining a RunnableGraph
//  val runnable: RunnableGraph[Future[Int]] = doubleSource.toMat(sink)(Keep.right)
//
//  // materialize the flow and get the value of the FoldSink
//  val sum: Future[Int] = runnable.run()
//
//  sum.flatMap(x => Future { println(x) })
}
