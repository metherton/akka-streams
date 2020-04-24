package com.martinetherton

import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.Instant

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.alpakka.csv.scaladsl.CsvParsing
import akka.stream.{ActorMaterializer, IOResult}
import akka.stream.scaladsl.{FileIO, Flow, FlowWithContext, Keep, Sink, Source, SourceWithContext}
import akka.util.ByteString

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.{Failure, Random, Success}

object Main extends App {

  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end

  case class Person(cols: Array[String]) {
    val file: String = cols(0)
    val name: String = cols(1)

    //override def toString = f"$file: and $name"
  }

  val csvFile = Paths.get("/Users/martin/myprojects/sbt/streams/test.csv")
  val in: Source[ByteString, Future[IOResult]] = FileIO.fromPath(csvFile)
  def encryptBytes(byteString: ByteString): ByteString = byteString
  val lineChunks: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner()
  val createPerson: Flow[List[ByteString], Person, NotUsed] = Flow[List[ByteString]].map { x => Person(x.map(y => y.utf8String.mkString).toArray) }
  val personObjects: Future[Seq[Person]] = in.via(lineChunks).throttle(2, 1.second).via(createPerson).runWith(Sink.seq)
  val sourcePersons: Source[Seq[Person], NotUsed] = Source.future(personObjects)
  val sourceOfPersons: Source[Person, NotUsed] = sourcePersons.mapConcat(identity)
  val sourceOfPersonsWithContext: SourceWithContext[Person, Person, NotUsed] = sourceOfPersons.asSourceWithContext(p => p)
  val readFile = FlowWithContext[Person, Person].map(person => (FileIO.fromPath(Paths.get("/Users/martin/myprojects/sbt/streams/source/" + person.file))))
//  val copyFile = FlowWithContext[Person, Person].map(person => person).via(readFile)//.map(bytesAndRecording => bytesAndRecording._1.runWith(writeFile(bytesAndRecording._2.file)))
  def outFile(n: String) = Paths.get("/Users/martin/myprojects/sbt/streams/dest/" + n)
  def writeFile(n: String) = FileIO.toPath(outFile(n))
 // val copyFile = FlowWithContext[Person, Person].via(readFile).asFlow.map(bytesAndPerson => bytesAndPerson._1.runWith(writeFile(bytesAndPerson._2.file)))
  val copyFile = FlowWithContext[Person, Person].via(readFile).asFlow.map(bytesAndPerson => bytesAndPerson._1.runWith(writeFile(bytesAndPerson._2.file))).asFlowWithContext((u: Person, ctu: Person) => (u, ctu))(ec => ec)

  val writeMyFile = Flow[Person]

//  val result = in.via(lineChunks).throttle(2, 1.second).via(createPerson)
//    .via(copyFileAndRecording).runForeach(x => println(x))


//  val result: Source[(Person, Person), NotUsed] = sourceOfPersonsWithContext.asSource

 // val readFileFlow = Flow[Person, Source[ByteString, Future[IOResult]], NotUsed]
 // val copyFandP = Flow[Person].map(person => person).via(readFileFlow)

 // val result = sourceOfPersonsWithContext.asSource.via(copyFandP).runWith(Sink.seq)

  val result = sourceOfPersonsWithContext.via(copyFile).runWith(Sink.ignore)
  implicit val ec = system.dispatcher
  result onComplete {
    case Success(value) => {
      println(value); system.terminate()
    }
    case Failure(e) => println(e.getMessage)
  }


  /* works fine..no encryption

  val csvFile = Paths.get("/Users/martin/myprojects/sbt/streams/test.csv")
  val in: Source[ByteString, Future[IOResult]] = FileIO.fromPath(csvFile)
  def encryptBytes(byteString: ByteString): ByteString = byteString
  val lineChunks: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner()
  val createPerson: Flow[List[ByteString], Person, NotUsed] = Flow[List[ByteString]].map { x => Person(x.map(y => y.utf8String.mkString).toArray) }
  val readFile: Flow[Person, (Source[ByteString, Future[IOResult]], Person), NotUsed] = Flow[Person].map(person => (FileIO.fromPath(Paths.get("/Users/martin/myprojects/sbt/streams/source/" + person.file)), person))
  def outFile(n: String) = Paths.get("/Users/martin/myprojects/sbt/streams/dest/" + n)
  def writeFile(n: String) = FileIO.toPath(outFile(n))
  val copyFileAndRecording = Flow[Person].map(person => person).via(readFile).map(bytesAndRecording => bytesAndRecording._1.runWith(writeFile(bytesAndRecording._2.file)))
  val result = in.via(lineChunks).throttle(2, 1.second).via(createPerson)
    .via(copyFileAndRecording).runForeach(x => println(x))

  implicit val ec = system.dispatcher
  result onComplete {
    case Success(value) => {
      println(value); system.terminate()
    }
    case Failure(e) => println(e.getMessage)
  }

   */
}