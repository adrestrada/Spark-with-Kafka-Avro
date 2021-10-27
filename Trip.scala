package bixiproject3

case class Trip(start_date: String,
                start_station_code: Integer,
                end_date: String,
                end_station_code: Integer,
                duration_sec: Integer,
                is_member: Integer
               )

object Trip {
  def apply(csv: String): Trip = {
    val f = csv.split(",", -1)
    Trip(f(0), f(1).toInt, f(2), f(3).toInt, f(4).toInt, f(5).toInt)
  }
}
