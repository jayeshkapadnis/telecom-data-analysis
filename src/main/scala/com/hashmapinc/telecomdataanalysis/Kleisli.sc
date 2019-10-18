import cats.FlatMap
import cats.implicits._
import cats._
import cats.data.Kleisli
import com.hashmapinc.telecomdataanalysis.SparkReaderJob._
import cats.instances.function._


def ranges(min: Double, max: Double, numOfRanges: Int): Array[Double] ={
  val step = (max - min) / numOfRanges.toDouble
  println(step)
  val doubles: Seq[Double] = (1 until numOfRanges) map { i =>
    min + (step * i)
  }

  (min +: doubles :+ max).toArray
}


val test = ranges(1, 12, 3)
