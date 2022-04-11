import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

import scala.util.parsing.json._

object KafkaToMongo {

  // Function which returns string by replacing every "." to "_"
  def replaceDotsForColName(name: String): String = {
    if(name.contains('.')){
      return name.replace(".", "_")
    }
    else{
      return name
    }
  }

  def main(args: Array[String]): Unit = {
    // Configure the session with MongoDB
    val spark = SparkSession.builder
                  .master("local")
                  .appName("demo")
                  .config("spark.mongodb.output.uri", "mongodb://localhost:27017")
                  .getOrCreate()

    // Add this for $"value" i.e. explicitly mentioning that value is a attribute not string
    import spark.implicits._

    // READ the stream from Kafka Topic (Source)
    // df will be in binary format b"------"
    val df = spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", "localhost:9092")
                  .option("subscribe", "test-topic")
                  .option("startingOffsets", "earliest")
                  .load()
    // df
    //     KEY                      VALUE
    //    "value"         |  { "$binary" : "eyJjcmVhdGVkX2F0IjoiV2VkIEFwciAwNiAxNTozMDo1NSArMDAwMCAyMDIyIiwiaWQiOjE1MTE3MjgxNjI4NTc2MTk0NTgsImlkX3N0ciI6IjE1MTE3MjgxNjI4NTc2MTk0NTgiLCJ0ZXh0IjoiQG9zY2FyaHVtYiBATGFuZ21hblZpbmNlIFllYWggQmlkZW4gaXMgYSBwaWVjZSBvZiBzaCp0IGxpYXIgYW5kIGEgZmFpbHVyZSFcblxuV2hhdCBraW5kIG9mIHN0dXBpZHR5IGRvZXMgaXQgdGFrZSB0byBiXHUyMDI2IGh0dHBzOlwvXC90LmNvXC9MM3A4bmNGZUZhIiwiZGlzcGxheV90ZXh0X3JhbmdlIjpbMjUsMTQwXSwic291cmNlIjoiXHUwMDNjYSBocmVmPVwiaHR0cDpcL1wvdHdpdHRlci5jb21cL2Rvd25sb2FkXC9hbmRyb2lkXCIgcmVsPVwibm9mb2xsb3dcIlx1MDAzZVR3aXR0ZXIgZm9yIEFuZHJvaWRcdTAwM2NcL2FcdTAwM2UiLCJ0cnVuY2F0ZWQiOnRydWUsImluX3JlcGx5X3RvX3N0YXR1c19pZCI6MTUxMTYzMDA5MDMyNDU2NjAxNiwiaW5fcmVwbHlfdG9fc3RhdHVzX2lkX3N0ciI6IjE1MTE2MzAwOTAzMjQ1NjYwMTYiLCJpbl9yZXBseV90b191c2VyX2lkIjoyMTAzMjA1OTUsImluX3JlcGx5X3RvX3VzZXJfaWRfc3RyIjoiMjEwMzIwNTk1IiwiaW5fcmVwbHlfdG9fc2NyZWVuX25hbWUiOiJvc2Nhcmh1bWIiLCJ1c2VyIjp7ImlkIjo2MTI5MzEzMDcsImlkX3N0ciI6IjYxMjkzMTMwNyIsIm5hbWUiOiJBIERldm90ZWQgWW9naSIsInNjcmVlbl9uYW1lIjoiQURldm90ZWRZb2dpIiwibG9jYXRpb24iOiJmYWJ1bG91cyBMYXMgVmVnYXMsIE5WIiwidXJsIjoiaHR0cDpcL1wvd3d3Lmluc3RhZ3JhbS5jb21cL2FkZXZvdGVkeW9naSIsImRlc2NyaXB0aW9uIjoiQXl1cnZlZGljIExpdmluZywgTGlmZSBFbnRodXNpYXN0LCBGdW5jdGlvbmFsIE51dHJpdGlvbiwgTG9uZ2V2aXR5LlxuIEFsc28gT2JzZXJ2YXRpb25zICYgYSBjb21tZW50YXJ5IG9uIGN1cnJlbnQgZXZlbnRzLlxuaHR0cDpcL1wvTGlua2VkSW4uY29tXC9pblwvQ2hhdHRlbVxuY3J5cHRvcyIsInRyYW5zbGF0b3JfdHlwZSI6Im5vbmUiLCJwcm90ZWN0ZWQiOmZhbHNlLCJ2ZXJpZmllZCI6ZmFsc2UsImZvbGxvd2Vyc19jb3VudCI6MzM4MDAxLCJmcmllbmRzX2NvdW50IjoxMDUwNjAsImxpc3RlZF9jb3VudCI6MjU4NSwiZmF2b3VyaXRlc19jb3VudCI6MTc0MTQ4LCJzdGF0dXNlc19jb3VudCI6MzMwMjU2LCJjcmVhdGVkX2F0IjoiVHVlIEp1biAxOSAyMzowMDoyNiArMDAwMCAyMDEyIiwidXRjX29mZnNldCI6bnVsbCwidGltZV96b25lIjpudWxsLCJnZW9fZW5hYmxlZCI6dHJ1ZSwibGFuZyI6bnVsbCwiY29udHJpYnV0b3JzX2VuYWJsZWQiOmZhbHNlLCJpc190cmFuc2xhdG9yIjpmYWxzZSwicHJvZmlsZV9iYWNrZ3JvdW5kX2NvbG9yIjoiRkZGMDREIiwicHJvZmlsZV9iYWNrZ3JvdW5kX2ltYWdlX3VybCI6Imh0dHA6XC9cL2Ficy50d2ltZy5jb21cL2ltYWdlc1wvdGhlbWVzXC90aGVtZTE5XC9iZy5naWYiLCJwcm9maWxlX2JhY2tncm91bmRfaW1hZ2VfdXJsX2h0dHBzIjoiaHR0cHM6XC9cL2Ficy50d2ltZy5jb21cL2ltYWdlc1wvdGhlbWVzXC90aGVtZTE5XC9iZy5naWYiLCJwcm9maWxlX2JhY2tncm91bmRfdGlsZSI6ZmFsc2UsInByb2ZpbGVfbGlua19jb2xvciI6IjAwMTM0RCIsInByb2ZpbGVfc2lkZWJhcl9ib3JkZXJfY29sb3IiOiJGRkY4QUQiLCJwcm9maWxlX3NpZGViYXJfZmlsbF9jb2xvciI6IkY2RkZEMSIsInByb2ZpbGVfdGV4dF9jb2xvciI6IjMzMzMzMyIsInByb2ZpbGVfdXNlX2JhY2tncm91bmRfaW1hZ2UiOnRydWUsInByb2ZpbGVfaW1hZ2VfdXJsIjoiaHR0cDpcL1wvcGJzLnR3aW1nLmNvbVwvcHJvZmlsZV9pbWFnZXNcLzkwNTUwNzE0OTg2NTgyODM1MlwvejlQQWZpY1Ffbm9ybWFsLmpwZyIsInByb2ZpbGVfaW1hZ2VfdXJsX2h0dHBzIjoiaHR0cHM6XC9cL3Bicy50d2ltZy5jb21cL3Byb2ZpbGVfaW1hZ2VzXC85MDU1MDcxNDk4NjU4MjgzNTJcL3o5UEFmaWNRX25vcm1hbC5qcGciLCJwcm9maWxlX2Jhbm5lcl91cmwiOiJodHRwczpcL1wvcGJzLnR3aW1nLmNvbVwvcHJvZmlsZV9iYW5uZXJzXC82MTI5MzEzMDdcLzE0NDUyMTE4MzIiLCJkZWZhdWx0X3Byb2ZpbGUiOmZhbHNlLCJkZWZhdWx0X3Byb2ZpbGVfaW1hZ2UiOmZhbHNlLCJmb2xsb3dpbmciOm51bGwsImZvbGxvd19yZXF1ZXN0X3NlbnQiOm51bGwsIm5vdGlmaWNhdGlvbnMiOm51bGwsIndpdGhoZWxkX2luX2NvdW50cmllcyI6W119LCJnZW8iOm51bGwsImNvb3JkaW5hdGVzIjpudWxsLCJwbGFjZSI6bnVsbCwiY29udHJpYnV0b3JzIjpudWxsLCJxdW90ZWRfc3RhdHVzX2lkIjoxNDc5MTM2NTIzMzU4NDA4NzA0LCJxdW90ZWRfc3RhdHVzX2lkX3N0ciI6IjE0NzkxMzY1MjMzNTg0MDg3MDQiLCJxdW90ZWRfc3RhdHVzIjp7ImNyZWF0ZWRfYXQiOiJUaHUgSmFuIDA2IDE3OjAzOjIyICswMDAwIDIwMjIiLCJpZCI6MTQ3OTEzNjUyMzM1ODQwODcwNCwiaWRfc3RyIjoiMTQ3OTEzNjUyMzM1ODQwODcwNCIsInRleHQiOiJXaGF0IGhhcHBlbmVkIHRvLi5cblxuXCJJJ20gZ29pbmcgdG8gc2h1dGRvd24gdGhlIHZpcnVzIG5vdCB0aGUgZWNvbm9teVwiIFx1ZDgzZFx1ZGU0NFxuXG5hbmRcblxuXCJBbnlvbmUgd2hvIGlzIHJlc3BvbnNpYmxlIGZvciB0aGF0IG1cdTIwMjYgaHR0cHM6XC9cL3QuY29cL3E0d0plU283YkMiLCJkaXNwbGF5X3RleHRfcmFuZ2UiOlswLDE0MF0sInNvdXJjZSI6Ilx1MDAzY2EgaHJlZj1cImh0dHBzOlwvXC9tb2JpbGUudHdpdHRlci5jb21cIiByZWw9XCJub2ZvbGxvd1wiXHUwMDNlVHdpdHRlciBXZWIgQXBwXHUwMDNjXC9hXHUwMDNlIiwidHJ1bmNhdGVkIjp0cnVlLCJpbl9yZXBseV90b19zdGF0dXNfaWQiOm51bGwsImluX3JlcGx5X3RvX3N0YXR1c19pZF9zdHIiOm51bGwsImluX3JlcGx5X3RvX3VzZXJfaWQiOm51bGwsImluX3JlcGx5X3RvX3VzZXJfaWRfc3RyIjpudWxsLCJpbl9yZXBseV90b19zY3JlZW5fbmFtZSI6bnVsbCwidXNlciI6eyJpZCI6ODg4OTEyNjY4LCJpZF9zdHIiOiI4ODg5MTI2NjgiLCJuYW1lIjoiVGltamJvIFx1ZDgzY1x1ZGRlNlx1ZDgzY1x1ZGRmYSBcdWQ4M2NcdWRkZmFcdWQ4M2NcdWRkZjhcdWQ4M2RcdWRjYTdcdTI2MTQiLCJzY3JlZW5fbmFtZSI6IlRpbV9qYm8iLCJsb2NhdGlvbiI6IkF1c3RyYWxpYSIsInVybCI6Imh0dHBzOlwvXC95b3V0dS5iZVwvcTVoT1pQNjV4aDQiLCJkZXNjcmlwdGlvbiI6IiNBdXN0cmFsaWEgaXMgb3ZlciBnb3Zlcm5lZCBcbiNUaGVDbGltYXRlQWx3YXlzQ2hhbmdlc1xuI0dvV29rZUdvQnJva2UgXG4jQW50aUdsb2JhbGlzdFxuI1BlYWNlIFxuUlRzIG5vdCBhbHdheXMgYW4gZW5kb3JzZW1lbnQiLCJ0cmFuc2xhdG9yX3R5cGUiOiJub25lIiwicHJvdGVjdGVkIjpmYWxzZSwidmVyaWZpZWQiOmZhbHNlLCJmb2xsb3dlcnNfY291bnQiOjE0OTgxLCJmcmllbmRzX2NvdW50Ijo1NDE2LCJsaXN0ZWRfY291bnQiOjk4LCJmYXZvdXJpdGVzX2NvdW50Ijo5MDA2Nywic3RhdHVzZXNfY291bnQiOjE0MTExMSwiY3JlYXRlZF9hdCI6IlRodSBPY3QgMTggMTQ6MDQ6MjIgKzAwMDAgMjAxMiIsInV0Y19vZmZzZXQiOm51bGwsInRpbWVfem9uZSI6bnVsbCwiZ2VvX2VuYWJsZWQiOmZhbHNlLCJsYW5nIjpudWxsLCJjb250cmlidXRvcnNfZW5hYmxlZCI6ZmFsc2UsImlzX3RyYW5zbGF0b3IiOmZhbHNlLCJwcm9maWxlX2JhY2tncm91bmRfY29sb3IiOiI4OUM5RkEiLCJwcm9maWxlX2JhY2tncm91bmRfaW1hZ2VfdXJsIjoiaHR0cDpcL1wvYWJzLnR3aW1nLmNvbVwvaW1hZ2VzXC90aGVtZXNcL3RoZW1lMVwvYmcucG5nIiwicHJvZmlsZV9iYWNrZ3JvdW5kX2ltYWdlX3VybF9odHRwcyI6Imh0dHBzOlwvXC9hYnMudHdpbWcuY29tXC9pbWFnZXNcL3RoZW1lc1wvdGhlbWUxXC9iZy5wbmciLCJwcm9maWxlX2JhY2tncm91bmRfdGlsZSI6dHJ1ZSwicHJvZmlsZV9saW5rX2NvbG9yIjoiMUI5NUUwIiwicHJvZmlsZV9zaWRlYmFyX2JvcmRlcl9jb2xvciI6IjAwMDAwMCIsInByb2ZpbGVfc2lkZWJhcl9maWxsX2NvbG9yIjoiMDAwMDAwIiwicHJvZmlsZV90ZXh0X2NvbG9yIjoiMDAwMDAwIiwicHJvZmlsZV91c2VfYmFja2dyb3VuZF9pbWFnZSI6dHJ1ZSwicHJvZmlsZV9pbWFnZV91cmwiOiJodHRwOlwvXC9wYnMudHdpbWcuY29tXC9wcm9maWxlX2ltYWdlc1wvMTQxMDE3NTU3MjY4MTAwMzAxMlwvRkhWRUZCSWZfbm9ybWFsLmpwZyIsInByb2ZpbGVfaW1hZ2VfdXJsX2h0dHBzIjoiaHR0cHM6XC9cL3Bicy50d2ltZy5jb21cL3Byb2ZpbGVfaW1hZ2VzXC8xNDEwMTc1NTcyNjgxMDAzMDEyXC9GSFZFRkJJZl9ub3JtYWwuanBnIiwicHJvZmlsZV9iYW5uZXJfdXJsIjoiaHR0cHM6XC9cL3Bicy50d2ltZy5jb21cL3Byb2ZpbGVfYmFubmVyc1wvODg4OTEyNjY4XC8xNTY4MDM2NzM5IiwiZGVmYXVsdF9wcm9maWxlIjpmYWxzZSwiZGVmYXVsdF9wcm9maWxlX2ltYWdlIjpmYWxzZSwiZm9sbG93aW5nIjpudWxsLCJmb2xsb3dfcmVxdWVzdF9zZW50IjpudWxsLCJub3RpZmljYXRpb25zIjpudWxsLCJ3aXRoaGVsZF9pbl9jb3VudHJpZXMiOltdfSwiZ2VvIjpudWxsLCJjb29yZGluYXRlcyI6bnVsbCwicGxhY2UiOm51bGwsImNvbnRyaWJ1dG9ycyI6bnVsbCwiaXNfcXVvdGVfc3RhdHVzIjpmYWxzZSwiZXh0ZW5kZWRfdHdlZXQiOnsiZnVsbF90ZXh0IjoiV2hhdCBoYXBwZW5lZCB0by4uXG5cblwiSSdtIGdvaW5nIHRvIHNodXRkb3duIHRoZSB2aXJ1cyBub3QgdGhlIGVjb25vbXlcIiBcdWQ4M2RcdWRlNDRcblxuYW5kXG5cblwiQW55b25lIHdobyBpcyByZXNwb25zaWJsZSBmb3IgdGhhdCBtYW55IGRlYXRocyBzaG91bGQgbm90IHJlbWFpbiBhcyBQcmVzaWRlbnQgb2YgdGhlIFVuaXRlZCBTdGF0ZXMgb2YgQW1lcmljYVwiICNKb2VCaWRlbiBcblxuI0NPVklEMTkgaHR0cHM6XC9cL3QuY29cL0pNNVkxRnhJTWoiLCJkaXNwbGF5X3RleHRfcmFuZ2UiOlswLDIxMF0sImVudGl0aWVzIjp7Imhhc2h0YWdzIjpbeyJ0ZXh0IjoiSm9lQmlkZW4iLCJpbmRpY2VzIjpbMTkwLDE5OV19LHsidGV4dCI6IkNPVklEMTkiLCJpbmRpY2VzIjpbMjAyLDIxMF19XSwidXJscyI6W10sInVzZXJfbWVudGlvbnMiOltdLCJzeW1ib2xzIjpbXSwibWVkaWEiOlt7ImlkIjoxNDc5MTMzNzY2MzgzNjQ4NzY5LCJpZF9zdHIiOiIxNDc5MTMzNzY2MzgzNjQ4NzY5IiwiaW5kaWNlcyI6WzIxMSwyMzRdLCJhZGRpdGlvbmFsX21lZGlhX2luZm8iOnsibW9uZXRpemFibGUiOmZhbHNlfSwibWVkaWFfdXJsIjoiaHR0cDpcL1wvcGJzLnR3aW1nLmNvbVwvZXh0X3R3X3ZpZGVvX3RodW1iXC8xNDc5MTMzNzY2MzgzNjQ4NzY5XC9wdVwvaW1nXC9fY1VGa0xwdmc4enJtdlhnLmpwZyIsIm1lZGlhX3VybF9odHRwcyI6Imh0dHBzOlwvXC9wYnMudHdpbWcuY29tXC9leHRfdHdfdmlkZW9fdGh1bWJcLzE0NzkxMzM3NjYzODM2NDg3NjlcL3B1XC9pbWdcL19jVUZrTHB2Zzh6cm12WGcuanBnIiwidXJsIjoiaHR0cHM6XC9cL3QuY29cL0pNNVkxRnhJTWoiLCJkaXNwbGF5X3VybCI6InBpYy50d2l0dGVyLmNvbVwvSk01WTFGeElNaiIsImV4cGFuZGVkX3VybCI6Imh0dHBzOlwvXC90d2l0dGVyLmNvbVwvVGltX2pib1wvc3RhdHVzXC8xNDc5MTM2NTIzMzU4NDA4NzA0XC92aWRlb1wvMSIsInR5cGUiOiJ2aWRlbyIsInZpZGVvX2luZm8iOnsiYXNwZWN0X3JhdGlvIjpbMTYsOV0sImR1cmF0aW9uX21pbGxpcyI6NDE2NzksInZhcmlhbnRzIjpbeyJiaXRyYXRlIjoyNTYwMDAsImNvbnRlbnRfdHlwZSI6InZpZGVvXC9tcDQiLCJ1cmwiOiJodHRwczpcL1wvdmlkZW8udHdpbWcuY29tXC9leHRfdHdfdmlkZW9cLzE0NzkxMzM3NjYzODM2NDg3NjlcL3B1XC92aWRcLzQ4MHgyNzBcLy1wZjg3ZTFZbUdkeFVWUVAubXA0P3RhZz0xMiJ9LHsiY29udGVudF90eXBlIjoiYXBwbGljYXRpb25cL3gtbXBlZ1VSTCIsInVybCI6Imh0dHBzOlwvXC92aWRlby50d2ltZy5jb21cL2V4dF90d192aWRlb1wvMTQ3OTEzMzc2NjM4MzY0ODc2OVwvcHVcL3BsXC84QndBU3lienJLQUxuRW1aLm0zdTg/dGFnPTEyJmNvbnRhaW5lcj1mbXA0In0seyJiaXRyYXRlIjo4MzIwMDAsImNvbnRlbnRfdHlwZSI6InZpZGVvXC9tcDQiLCJ1cmwiOiJodHRwczpcL1wvdmlkZW8udHdpbWcuY29tXC9leHRfdHdfdmlkZW9cLzE0NzkxMzM3NjYzODM2NDg3NjlcL3B1XC92aWRcLzY0MHgzNjBcLzc3bWxOZWhPWERhOTc4a2kubXA0P3RhZz0xMiJ9XX0sInNpemVzIjp7InRodW1iIjp7InciOjE1MCwiaCI6MTUwLCJyZXNpemUiOiJjcm9wIn0sImxhcmdlIjp7InciOjY0MCwiaCI6MzYwLCJyZXNpemUiOiJmaXQifSwibWVkaXVtIjp7InciOjY0MCwiaCI6MzYwLCJyZXNpemUiOiJmaXQifSwic21hbGwiOnsidyI6NjQwLCJoIjozNjAsInJlc2l6ZSI6ImZpdCJ9fX1dfSwiZXh0ZW5kZWRfZW50aXRpZXMiOnsibWVkaWEiOlt7ImlkIjoxNDc5MTMzNzY2MzgzNjQ4NzY5LCJpZF9zdHIiOiIxNDc5MTMzNzY2MzgzNjQ4NzY5IiwiaW5kaWNlcyI6WzIxMSwyMzRdLCJhZGRpdGlvbmFsX21lZGlhX2luZm8iOnsibW9uZXRpemFibGUiOmZhbHNlfSwibWVkaWFfdXJsIjoiaHR0cDpcL1wvcGJzLnR3aW1nLmNvbVwvZXh0X3R3X3ZpZGVvX3RodW1iXC8xNDc5MTMzNzY2MzgzNjQ4NzY5XC9wdVwvaW1nXC9fY1VGa0xwdmc4enJtdlhnLmpwZyIsIm1lZGlhX3VybF9odHRwcyI6Imh0dHBzOlwvXC9wYnMudHdpbWcuY29tXC9leHRfdHdfdmlkZW9fdGh1bWJcLzE0NzkxMzM3NjYzODM2NDg3NjlcL3B1XC9pbWdcL19jVUZrTHB2Zzh6cm12WGcuanBnIiwidXJsIjoiaHR0cHM6XC9cL3QuY29cL0pNNVkxRnhJTWoiLCJkaXNwbGF5X3VybCI6InBpYy50d2l0dGVyLmNvbVwvSk01WTFGeElNaiIsImV4cGFuZGVkX3VybCI6Imh0dHBzOlwvXC90d2l0dGVyLmNvbVwvVGltX2pib1wvc3RhdHVzXC8xNDc5MTM2NTIzMzU4NDA4NzA0XC92aWRlb1wvMSIsInR5cGUiOiJ2aWRlbyIsInZpZGVvX2luZm8iOnsiYXNwZWN0X3JhdGlvIjpbMTYsOV0sImR1cmF0aW9uX21pbGxpcyI6NDE2NzksInZhcmlhbnRzIjpbeyJiaXRyYXRlIjoyNTYwMDAsImNvbnRlbnRfdHlwZSI6InZpZGVvXC9tcDQiLCJ1cmwiOiJodHRwczpcL1wvdmlkZW8udHdpbWcuY29tXC9leHRfdHdfdmlkZW9cLzE0NzkxMzM3NjYzODM2NDg3NjlcL3B1XC92aWRcLzQ4MHgyNzBcLy1wZjg3ZTFZbUdkeFVWUVAubXA0P3RhZz0xMiJ9LHsiY29udGVudF90eXBlIjoiYXBwbGljYXRpb25cL3gtbXBlZ1VSTCIsInVybCI6Imh0dHBzOlwvXC92aWRlby50d2ltZy5jb21cL2V4dF90d192aWRlb1wvMTQ3OTEzMzc2NjM4MzY0ODc2OVwvcHVcL3BsXC84QndBU3lienJLQUxuRW1aLm0zdTg/dGFnPTEyJmNvbnRhaW5lcj1mbXA0In0seyJiaXRyYXRlIjo4MzIwMDAsImNvbnRlbnRfdHlwZSI6InZpZGVvXC9tcDQiLCJ1cmwiOiJodHRwczpcL1wvdmlkZW8udHdpbWcuY29tXC9leHRfdHdfdmlkZW9cLzE0NzkxMzM3NjYzODM2NDg3NjlcL3B1XC92aWRcLzY0MHgzNjBcLzc3bWxOZWhPWERhOTc4a2kubXA0P3RhZz0xMiJ9XX0sInNpemVzIjp7InRodW1iIjp7InciOjE1MCwiaCI6MTUwLCJyZXNpemUiOiJjcm9wIn0sImxhcmdlIjp7InciOjY0MCwiaCI6MzYwLCJyZXNpemUiOiJmaXQifSwibWVkaXVtIjp7InciOjY0MCwiaCI6MzYwLCJyZXNpemUiOiJmaXQifSwic21hbGwiOnsidyI6NjQwLCJoIjozNjAsInJlc2l6ZSI6ImZpdCJ9fX1dfX0sInF1b3RlX2NvdW50IjoyLCJyZXBseV9jb3VudCI6OSwicmV0d2VldF9jb3VudCI6MjIsImZhdm9yaXRlX2NvdW50IjozOSwiZW50aXRpZXMiOnsiaGFzaHRhZ3MiOltdLCJ1cmxzIjpbeyJ1cmwiOiJodHRwczpcL1wvdC5jb1wvcTR3SmVTbzdiQyIsImV4cGFuZGVkX3VybCI6Imh0dHBzOlwvXC90d2l0dGVyLmNvbVwvaVwvd2ViXC9zdGF0dXNcLzE0NzkxMzY1MjMzNTg0MDg3MDQiLCJkaXNwbGF5X3VybCI6InR3aXR0ZXIuY29tXC9pXC93ZWJcL3N0YXR1c1wvMVx1MjAyNiIsImluZGljZXMiOlsxMTcsMTQwXX1dLCJ1c2VyX21lbnRpb25zIjpbXSwic3ltYm9scyI6W119LCJmYXZvcml0ZWQiOmZhbHNlLCJyZXR3ZWV0ZWQiOmZhbHNlLCJwb3NzaWJseV9zZW5zaXRpdmUiOmZhbHNlLCJmaWx0ZXJfbGV2ZWwiOiJsb3ciLCJsYW5nIjoiZW4ifSwicXVvdGVkX3N0YXR1c19wZXJtYWxpbmsiOnsidXJsIjoiaHR0cHM6XC9cL3QuY29cL283YW8zNGJNTUMiLCJleHBhbmRlZCI6Imh0dHBzOlwvXC90d2l0dGVyLmNvbVwvVGltX2pib1wvc3RhdHVzXC8xNDc5MTM2NTIzMzU4NDA4NzA0P3Q9T2JyTmhTWFp6WTg2eXJoQUVnbXZhUSZzPTE5IiwiZGlzcGxheSI6InR3aXR0ZXIuY29tXC9UaW1famJvXC9zdGF0dXNcdTIwMjYifSwiaXNfcXVvdGVfc3RhdHVzIjp0cnVlLCJleHRlbmRlZF90d2VldCI6eyJmdWxsX3RleHQiOiJAb3NjYXJodW1iIEBMYW5nbWFuVmluY2UgWWVhaCBCaWRlbiBpcyBhIHBpZWNlIG9mIHNoKnQgbGlhciBhbmQgYSBmYWlsdXJlIVxuXG5XaGF0IGtpbmQgb2Ygc3R1cGlkdHkgZG9lcyBpdCB0YWtlIHRvIGJlbGlldmUgaGlzIGxpZXM/XG5cbmh0dHBzOlwvXC90LmNvXC9vN2FvMzRiTU1DIiwiZGlzcGxheV90ZXh0X3JhbmdlIjpbMjUsMTU2XSwiZW50aXRpZXMiOnsiaGFzaHRhZ3MiOltdLCJ1cmxzIjpbeyJ1cmwiOiJodHRwczpcL1wvdC5jb1wvbzdhbzM0Yk1NQyIsImV4cGFuZGVkX3VybCI6Imh0dHBzOlwvXC90d2l0dGVyLmNvbVwvVGltX2pib1wvc3RhdHVzXC8xNDc5MTM2NTIzMzU4NDA4NzA0P3Q9T2JyTmhTWFp6WTg2eXJoQUVnbXZhUSZzPTE5IiwiZGlzcGxheV91cmwiOiJ0d2l0dGVyLmNvbVwvVGltX2pib1wvc3RhdHVzXHUyMDI2IiwiaW5kaWNlcyI6WzEzMywxNTZdfV0sInVzZXJfbWVudGlvbnMiOlt7InNjcmVlbl9uYW1lIjoib3NjYXJodW1iIiwibmFtZSI6Ik9zY2FyIGxhcnNlbiBVbHJpayAoIGpvZGlkZW5tYXJrKSIsImlkIjoyMTAzMjA1OTUsImlkX3N0ciI6IjIxMDMyMDU5NSIsImluZGljZXMiOlswLDEwXX0seyJzY3JlZW5fbmFtZSI6IkxhbmdtYW5WaW5jZSIsIm5hbWUiOiJWaW5jZSBMYW5nbWFuIiwiaWQiOjEwMjY1NzY3MDA2MTA2MjE0NDEsImlkX3N0ciI6IjEwMjY1NzY3MDA2MTA2MjE0NDEiLCJpbmRpY2VzIjpbMTEsMjRdfV0sInN5bWJvbHMiOltdfX0sInF1b3RlX2NvdW50IjowLCJyZXBseV9jb3VudCI6MCwicmV0d2VldF9jb3VudCI6MCwiZmF2b3JpdGVfY291bnQiOjAsImVudGl0aWVzIjp7Imhhc2h0YWdzIjpbXSwidXJscyI6W3sidXJsIjoiaHR0cHM6XC9cL3QuY29cL0wzcDhuY0ZlRmEiLCJleHBhbmRlZF91cmwiOiJodHRwczpcL1wvdHdpdHRlci5jb21cL2lcL3dlYlwvc3RhdHVzXC8xNTExNzI4MTYyODU3NjE5NDU4IiwiZGlzcGxheV91cmwiOiJ0d2l0dGVyLmNvbVwvaVwvd2ViXC9zdGF0dXNcLzFcdTIwMjYiLCJpbmRpY2VzIjpbMTE3LDE0MF19XSwidXNlcl9tZW50aW9ucyI6W3sic2NyZWVuX25hbWUiOiJvc2Nhcmh1bWIiLCJuYW1lIjoiT3NjYXIgbGFyc2VuIFVscmlrICggam9kaWRlbm1hcmspIiwiaWQiOjIxMDMyMDU5NSwiaWRfc3RyIjoiMjEwMzIwNTk1IiwiaW5kaWNlcyI6WzAsMTBdfSx7InNjcmVlbl9uYW1lIjoiTGFuZ21hblZpbmNlIiwibmFtZSI6IlZpbmNlIExhbmdtYW4iLCJpZCI6MTAyNjU3NjcwMDYxMDYyMTQ0MSwiaWRfc3RyIjoiMTAyNjU3NjcwMDYxMDYyMTQ0MSIsImluZGljZXMiOlsxMSwyNF19XSwic3ltYm9scyI6W119LCJmYXZvcml0ZWQiOmZhbHNlLCJyZXR3ZWV0ZWQiOmZhbHNlLCJwb3NzaWJseV9zZW5zaXRpdmUiOmZhbHNlLCJmaWx0ZXJfbGV2ZWwiOiJsb3ciLCJsYW5nIjoiZW4iLCJ0aW1lc3RhbXBfbXMiOiIxNjQ5MjU5MDU1MjMzIn0NCg==", "$type" : "00" },
    //    "topic"         | "test-topic",
    //    "partition"     |  0,
    //    "offset"        |  NumberLong(0),
    //    "timestamp"     | ISODate("2022-04-06T15:31:00.696Z"),
    //    "timestampType" | 0

    val raw_json: DataFrame = df.selectExpr("CAST(value AS STRING)")
    // raw_json
    // KEY                 VALUE
    //             v this \(backslash) is to let json know to take " (double quote) literally and not as elimination of string
    // "value"  | "{\"created_at\":\"Wed Apr 06 15:30:55 +0000 2022\",\"id\":1511728162857619458,\"id_str\":\"1511728162857619458\",

    // Mention column to be extracted from "value" column
    val columns: Seq[String] =  Seq(
      "created_at",
      "id",
      "text",
      "truncated",
      "user.name",
      "user.screen_name",
      "user.location",
      "geo",
      "coordinates",
      "place",
      "entities.hashtags",
      "lang"
    )

    // get_json_object(col, path): Extracts json object from a json string based on json path specified, and returns json string of the extracted json object.
    // path = "$$.$c" => First $ specifies second $ as variable not str, now one $ implies root document in MongoDB
    //                   Dot (.) implies go inside root document and find the value of key written right after it ($c)
    //                   Third dollar specifies c as variable not str which is `columns` values one by one
    // .alias(fun(c)) => Because MongoDB doesn't support (.) or ($) in its column, so if any value in columns contain
    //                   data like `user.id` (which means go inside `user` and extract value of key `id`), we have to
    //                    replace every dots with _ (underscore)
    val cleaned_columns: Seq[Column] = columns
          .map(c =>
            get_json_object($"value", s"$$.$c").alias(replaceDotsForColName(c))
          )

    // Select all columns which is mentioned in the plan of cleaned_columns [PLAN]
    // cleaned_columns is a PLAN, not an actual collection
    // We can't actually collect data in streaming data, we can only use `data.write` to collect it, which will be used as sink
    // $"*" +:  => Add this before cleaned_columns to get value columns as well
    val table_with_null_values: DataFrame = raw_json.select(cleaned_columns: _*)
    // Remove document which doesn't contain user_location
    val table = table_with_null_values.na.drop(Seq("user_location"))
    // table
    // KEY                VALUE
    // "text"       | "RT @RepThomasMassie: You’re at least 200 times (20,000%) more likely to die of something other than COVID, than to die with COVID.\n\nCOVID i…",
    // "created_at" | "Wed Apr 06 15:55:03 +0000 2022",
    // "user_id"    | "980238526305460224"

    // For each batch(batchDF), provided what should be done inside it as a function
    // Storing in mongo, with awaitTermination
    table.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      batchDF.write
      .format("mongo")
      .mode("append")
      .option("database", "twitter_db")
      .option("collection", "covid_tweets")
      .save()
    }.start().awaitTermination()
  }
}
