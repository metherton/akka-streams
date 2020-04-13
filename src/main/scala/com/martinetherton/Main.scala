package com.martinetherton

import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, IOResult}
import akka.stream.scaladsl.{FileIO, Flow, Framing, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString

import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

object Main extends App {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher

//  val outfile = Paths.get("out.wav")
//  val file = Paths.get("swirly.wav")
//  val resSource: Source[ByteString, Future[IOResult]] = FileIO.fromPath(file)
//
//  val sink = FileIO.toPath(outfile)
//
//  val s = Source(1 to 6)
//
//  val x: Future[IOResult] = resSource.runWith(sink)
//
//  x.onComplete(_ => {
//    Files.setLastModifiedTime(outfile, FileTime.from(Instant.ofEpochSecond(100000000L)))
//    system.terminate()
//  })




  val x = FileIO.fromPath(Paths.get("test.csv"))
    .via(Framing.delimiter(ByteString("\n"), 256, true).map(_.utf8String))
    .to(Sink.foreach(y => println(y)))
    .run()

  x.onComplete(_ => {
    system.terminate()
  })


 // val foreach: Future[IOResult] = res.to(Sink.ignore).run()
 // val outfile = Paths.get("out.wav")
//  val f = res.via(Framing.delimiter(ByteString("\n"), 256, true)).map(_.utf8String)

//  val fileNames = f.map(x => x.split(";").head)
////
//  val sink = Sink.foreach[String](println(_))
//  val runnable = fileNames.toMat(sink)(Keep.right)
 // val a = runnable.run()

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
