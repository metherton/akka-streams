package com.martinetherton

import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.time.Instant

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.alpakka.csv.scaladsl.CsvParsing
import akka.stream.{ActorMaterializer, Attributes, IOResult}
import akka.stream.scaladsl.{FileIO, Flow, FlowWithContext, Keep, Sink, Source, SourceWithContext}
import akka.util.ByteString

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.{Failure, Random, Success}

object Main extends App {

  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher
  // needed for the future flatMap/onComplete in the end

  case class Person(cols: Array[String]) {
    val file: String = cols(0)
    val name: String = cols(1)

    override def toString = f"$file and $name"
  }


  val csvFile = "/Users/martin/myprojects/sbt/streams/test.csv"
  val source = scala.io.Source.fromFile(csvFile)

  val persons: List[Person] = (for {
    line <- source.getLines
  } yield line).toList.map(l => Person(l.split(",").map(_.trim)))

  //val simpleSink = Sink.foreach[Person](println)
  val simpleSink = Sink.seq[String]

  val myVector = Vector("one", "two")
  val mapped = for {
    num <- myVector
  } yield num

  println(s"mapped: $mapped")

  val runnableGraph = Source(persons).map(p => p.file).toMat(simpleSink)(Keep.right)
  val futureFiles: Future[Seq[String]] = runnableGraph.run()
  futureFiles.onComplete {
    case Success(value) => {   val bla1 = for {
                                files <- value
                             } yield files

                             println(s"bla1 : $bla1")
    }
    case Failure(ex) => println(s"Stream processing finished with: $ex")
  }

  def setExpiryDates(value: Seq[String]): Int = {
    1
  }



  //  val sourcePerson = Source(List(Person("martin"), Person("john"), Person("freddie"), Person("william"), Person("bob")))
//  val mapPersonName = Flow[Person].map(p => p.name)
//  val filterNames = Flow[String].filter(n => n.length > 5)
//  val firstTwo = Flow[String].take(2)
//  sourcePerson.via(mapPersonName).via(filterNames).via(firstTwo).runForeach(println)


//  val csvFile = Paths.get("/Users/martin/myprojects/sbt/streams/test.csv")
//  val in: Source[ByteString, Future[IOResult]] = FileIO.fromPath(csvFile)
//  def encryptBytes(byteString: ByteString): ByteString = byteString
//  val lineChunks: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner()
//  val createRecordingToImport: Flow[List[ByteString], Person, NotUsed] = Flow[List[ByteString]].map{ x => Person(x.map(y => y.utf8String.mkString).toArray)}
//  val secondRecordingToImport = Flow[List[ByteString]].map{ x => Person(x.map(y => y.utf8String.mkString).toArray)}
//  val readFileEncryptedBytes: Flow[Person, Source[ByteString, Future[IOResult]], NotUsed] = Flow[Person].map(person => FileIO.fromPath(Paths.get("/Users/martin/myprojects/sbt/streams/source/" + person.file)))
//  val readFileEncryptedBytesAndRecording: Flow[Person, (Source[ByteString, Future[IOResult]], Person), NotUsed] = Flow[Person].map(recording => {
//    val first = FileIO.fromPath(Paths.get("/Users/martin/myprojects/sbt/streams/source/" + recording.file))
//    val second = recording
//    (first, second)
//  })
//  val justTheFiles = Flow[(Source[ByteString, Future[IOResult]], Person)].map(t => t._1)
//  def outFile(n: String) = Paths.get("/Users/martin/myprojects/sbt/streams/dest/" + n)
//  def outFileName(f: String) = Paths.get("/Users/martin/myprojects/sbt/streams/dest/" + f)
//  def writeFile(n: String) = FileIO.toPath(outFile(n))
//  val copyFileAndRecording =
//    Flow[Person].map(recording => recording).via(readFileEncryptedBytesAndRecording).
//      map(bytesAndRecording => (bytesAndRecording._1.runWith(writeFile(bytesAndRecording._2.file)), bytesAndRecording._2))
//
//  val setFileOutputName = Flow[Person].map(recording => "/Users/martin/myprojects/sbt/streams/dest/" + recording.file)
//
//
//  def copyFiles() = {
//
//    val result: Future[Done] = in.via(lineChunks).throttle(100, 5.second).via(createRecordingToImport).via(copyFileAndRecording).runForeach(x => println(x))
//    result
//  }
//
//  def setExpiryDateOfFiles(futureDone: Done) = {
//    val csvFile = Paths.get("/Users/martin/myprojects/sbt/streams/test.csv")
//    val in: Source[ByteString, Future[IOResult]] = FileIO.fromPath(csvFile)
//    def encryptBytes(byteString: ByteString): ByteString = byteString
//    val lineChunks: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner()
//    val createRecordingToImport: Flow[List[ByteString], Person, NotUsed] = Flow[List[ByteString]].map{ x => Person(x.map(y => y.utf8String.mkString).toArray)}
//    val secondRecordingToImport = Flow[List[ByteString]].map{ x => Person(x.map(y => y.utf8String.mkString).toArray)}
//    val readFileEncryptedBytes: Flow[Person, Source[ByteString, Future[IOResult]], NotUsed] = Flow[Person].map(recording => FileIO.fromPath(Paths.get("/Users/martin/myprojects/sbt/streams/source/" + recording.file)))
//    val readFileEncryptedBytesAndRecording: Flow[Person, (Source[ByteString, Future[IOResult]], Person), NotUsed] = Flow[Person].map(recording => {
//      val first = FileIO.fromPath(Paths.get("/Users/martin/myprojects/sbt/streams/source/" + recording.file))
//      val second = recording
//      (first, second)
//    })
//    val justTheFiles = Flow[(Source[ByteString, Future[IOResult]], Person)].map(t => t._1)
//    def outFile(n: String) = Paths.get("/Users/martin/myprojects/sbt/streams/dest/" + n)
//    def outFileName(f: String) = Paths.get("/Users/martin/myprojects/sbt/streams/dest/" + f)
//    def writeFile(n: String) = FileIO.toPath(outFile(n))
//
//    val setFileOutputName = Flow[Person].map(recording => "/Users/martin/myprojects/sbt/streams/source/" + recording.file)
//
//
//    val setLastModifiedDate = Flow[Person].map(recording => Files.setLastModifiedTime(Paths.get("/Users/martin/myprojects/sbt/streams/dest/" + recording.file), FileTime.from(Instant.ofEpochSecond(9000000000L))))
//
//    val result = in.via(lineChunks).throttle(100, 5.second).via(createRecordingToImport).via(setLastModifiedDate).runForeach(x => println(x))
//    result
//  }


//  val result: Future[Done] = for {
//    x <- copyFiles()
//    y <- setExpiryDateOfFiles(x)
//  } yield y
//
//  result onComplete {
//    case Success(value) => {system.terminate()}
//    case Failure(e) => println(e.getMessage)
//  }

  /* source with tuples waiting for next step

  val csvFile = Paths.get("/Users/martin/myprojects/sbt/streams/test.csv")
  val in: Source[ByteString, Future[IOResult]] = FileIO.fromPath(csvFile)
  def encryptBytes(byteString: ByteString): ByteString = byteString
  val lineChunks: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner()
  val createPerson: Flow[List[ByteString], Person, NotUsed] = Flow[List[ByteString]].map { x => Person(x.map(y => y.utf8String.mkString).toArray) }
  val personObjects: Future[Seq[Person]] = in.via(lineChunks).throttle(2, 1.second).via(createPerson).runWith(Sink.seq)
  val sourcePersons: Source[Seq[Person], NotUsed] = Source.future(personObjects)
  val sourceOfPersons: Source[Person, NotUsed] = sourcePersons.mapConcat(identity)

  val sourceOfPersonsWithContext: SourceWithContext[Person, Person, NotUsed] = SourceWithContext.fromTuples(sourceOfPersons.map {
    case Person(x) => (Person(x), Person(x))
  })
  val readFile = FlowWithContext[Person, Person].map(person => (FileIO.fromPath(Paths.get("/Users/martin/myprojects/sbt/streams/source/" + person.file))))
  def outFile(n: String) = Paths.get("/Users/martin/myprojects/sbt/streams/dest/" + n)
  def writeFile(n: String) = FileIO.toPath(outFile(n))
  val copyFile = FlowWithContext[Person, Person].via(readFile).asFlow.map(bytesAndPerson => bytesAndPerson._1.runWith(writeFile(bytesAndPerson._2.file))).asFlowWithContext((u: Person, ctu: Person) => (u, ctu))(ec => ec).asFlow
  val result = sourceOfPersonsWithContext.via(copyFile).runWith(Sink.ignore)
  implicit val ec = system.dispatcher
  result onComplete {
    case Success(value) => {
      println(value); system.terminate()
    }
    case Failure(e) => println(e.getMessage)
  }
*/

//  val csvFile = Paths.get("/Users/martin/myprojects/sbt/streams/test.csv")
//  val in: Source[ByteString, Future[IOResult]] = FileIO.fromPath(csvFile)
//  def encryptBytes(byteString: ByteString): ByteString = byteString
//  val lineChunks: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner()
//  val createPerson: Flow[List[ByteString], Person, NotUsed] = Flow[List[ByteString]].map { x => Person(x.map(y => y.utf8String.mkString).toArray) }
//  val personObjects: Future[Seq[Person]] = in.via(lineChunks).throttle(2, 1.second).via(createPerson).runWith(Sink.seq)


//  def persons = {
//    val sourcePersons: Source[Seq[Person], NotUsed] = Source.future(personObjects)
//    val sourceOfPersons: Source[Person, NotUsed] = sourcePersons.log("before-map").withAttributes(Attributes
//      .logLevels(onElement = Logging.WarningLevel, onFinish = Logging.InfoLevel, onFailure = Logging.DebugLevel))
//      .mapConcat(identity)
//
//    val sourceOfPersonsWithContext: SourceWithContext[Person, Person, NotUsed] = SourceWithContext.fromTuples(sourceOfPersons.map {
//      case Person(x) => (Person(x), Person(x))
//    })
//    sourceOfPersonsWithContext
//  }
//
//  def copyFileOld() = {
//    val csvFile = Paths.get("/Users/martin/myprojects/sbt/streams/test.csv")
//    val in: Source[ByteString, Future[IOResult]] = FileIO.fromPath(csvFile)
//    def encryptBytes(byteString: ByteString): ByteString = byteString
//    val lineChunks: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner()
//    val createPerson: Flow[List[ByteString], Person, NotUsed] = Flow[List[ByteString]].map { x => Person(x.map(y => y.utf8String.mkString).toArray) }
//    val personObjects: Future[Seq[Person]] = in.via(lineChunks).throttle(2, 1.second).via(createPerson).runWith(Sink.seq)
//
//    val sourcePersons: Source[Seq[Person], NotUsed] = Source.future(personObjects)
//    val sourceOfPersons: Source[Person, NotUsed] = sourcePersons.log("before-map").withAttributes(Attributes
//      .logLevels(onElement = Logging.WarningLevel, onFinish = Logging.InfoLevel, onFailure = Logging.DebugLevel))
//      .mapConcat(identity)
//
//    val sourceOfPersonsWithContext: SourceWithContext[Person, Person, NotUsed] = SourceWithContext.fromTuples(sourceOfPersons.map {
//      case Person(x) => (Person(x), Person(x))
//    })
//
//    val readFile = FlowWithContext[Person, Person].map(person => (FileIO.fromPath(Paths.get("/Users/martin/myprojects/sbt/streams/source/" + person.file))))
//    def outFile(n: String) = Paths.get("/Users/martin/myprojects/sbt/streams/dest/" + n)
//    def writeFile(n: String) = FileIO.toPath(outFile(n))
//    val copyFile = FlowWithContext[Person, Person].via(readFile).asFlow.map(bytesAndPerson => bytesAndPerson._1.runWith(writeFile(bytesAndPerson._2.file))).asFlowWithContext((u: Person, ctu: Person) => (u, ctu))(ec => ec)
//    val result = persons.via(copyFile).runWith(Sink.ignore)
//    result
//  }
//
//  def copyFile() = {
//    val csvFile = Paths.get("/Users/martin/myprojects/sbt/streams/test.csv")
//
//    val flow: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner(",", "'", "\")
//
//    val in: Source[ByteString, Future[IOResult]] = FileIO.fromPath(csvFile)
//    def encryptBytes(byteString: ByteString): ByteString = byteString
//    val lineChunks: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner()
//    val createPerson: Flow[List[ByteString], Person, NotUsed] = Flow[List[ByteString]].map { x => Person(x.map(y => y.utf8String.mkString).toArray) }
//    val personObjects: Future[Seq[Person]] = in.via(lineChunks).throttle(2, 1.second).via(createPerson).runWith(Sink.seq)
//
//    val sourcePersons: Source[Seq[Person], NotUsed] = Source.future(personObjects)
//    val sourceOfPersons: Source[Person, NotUsed] = sourcePersons.log("before-map").withAttributes(Attributes
//      .logLevels(onElement = Logging.WarningLevel, onFinish = Logging.InfoLevel, onFailure = Logging.DebugLevel))
//      .mapConcat(identity)
//
//    val sourceOfPersonsWithContext: SourceWithContext[Person, Person, NotUsed] = SourceWithContext.fromTuples(sourceOfPersons.map {
//      case Person(x) => (Person(x), Person(x))
//    })
//
//    val readFile = FlowWithContext[Person, Person].map(person => (FileIO.fromPath(Paths.get("/Users/martin/myprojects/sbt/streams/source/" + person.file))))
//    def outFile(n: String) = Paths.get("/Users/martin/myprojects/sbt/streams/dest/" + n)
//    def writeFile(n: String) = FileIO.toPath(outFile(n))
//    val copyFile = FlowWithContext[Person, Person].via(readFile).asFlow.map(bytesAndPerson => bytesAndPerson._1.runWith(writeFile(bytesAndPerson._2.file))).asFlowWithContext((u: Person, ctu: Person) => (u, ctu))(ec => ec)
//    val result = persons.via(copyFile).runWith(Sink.ignore)
//    result
//  }
//
//  def setLastModified(done: Done) = {
//    val csvFile = Paths.get("/Users/martin/myprojects/sbt/streams/test.csv")
//    val in: Source[ByteString, Future[IOResult]] = FileIO.fromPath(csvFile)
//    def encryptBytes(byteString: ByteString): ByteString = byteString
//    val lineChunks: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner()
//    val createPerson: Flow[List[ByteString], Person, NotUsed] = Flow[List[ByteString]].map { x => Person(x.map(y => y.utf8String.mkString).toArray) }
//    val personObjects: Future[Seq[Person]] = in.via(lineChunks).throttle(2, 1.second).via(createPerson).runWith(Sink.seq)
//
//    val sourcePersons: Source[Seq[Person], NotUsed] = Source.future(personObjects)
//    val sourceOfPersons: Source[Person, NotUsed] = sourcePersons.log("before-map").withAttributes(Attributes
//      .logLevels(onElement = Logging.WarningLevel, onFinish = Logging.InfoLevel, onFailure = Logging.DebugLevel))
//      .mapConcat(identity)
//
//    val sourceOfPersonsWithContext: SourceWithContext[Person, Person, NotUsed] = SourceWithContext.fromTuples(sourceOfPersons.map {
//      case Person(x) => (Person(x), Person(x))
//    })
//
//    val result = sourceOfPersonsWithContext.map(person => Files.setLastModifiedTime(Paths.get("/Users/martin/myprojects/sbt/streams/dest/" + person.file), FileTime.from(Instant.ofEpochSecond(9000000000L)))).runWith(Sink.ignore)
//    result
//  }
//
//  implicit val ec = system.dispatcher
//




  /*  works fine with Context

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
  def outFile(n: String) = Paths.get("/Users/martin/myprojects/sbt/streams/dest/" + n)
  def writeFile(n: String) = FileIO.toPath(outFile(n))
  val copyFile = FlowWithContext[Person, Person].via(readFile).asFlow.map(bytesAndPerson => bytesAndPerson._1.runWith(writeFile(bytesAndPerson._2.file))).asFlowWithContext((u: Person, ctu: Person) => (u, ctu))(ec => ec)
  val result = sourceOfPersonsWithContext.via(copyFile).runWith(Sink.ignore)
  implicit val ec = system.dispatcher
  result onComplete {
    case Success(value) => {
      println(value); system.terminate()
    }
    case Failure(e) => println(e.getMessage)
  }

*/

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