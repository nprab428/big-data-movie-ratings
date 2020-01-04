import scala.reflect.runtime.universe._


case class RatingRecord(
    userId: Int,
    movieId: Int,
    rating: Float,
    timestamp: BigInt)