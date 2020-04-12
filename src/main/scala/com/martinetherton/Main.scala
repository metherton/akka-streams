package com.martinetherton

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink, Source}

import scala.concurrent.Future

object Main extends App {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher

  // Create a source from an Iterable
  Source(List(1, 2, 3))

  // Create a source from a Future
  Source.fromFuture(Future.successful("Hello Streams!"))

  // Create a source from a single element
  Source.single("only one element")

  // an empty source
  Source.empty

  // Sink that folds over the stream and returns a Future
  // of the final result as its materialized value
  Sink.fold[Int, Int](0)(_ + _)

  // Sink that returns a Future as its materialized value,
  // containing the first element of the stream
  Sink.head

  // A Sink that consumes a stream without doing anything with the elements
  Sink.ignore

  // A Sink that executes a side-effecting call for every element of the stream
  Sink.foreach[String](println(_))
}
