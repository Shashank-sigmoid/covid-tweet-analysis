import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkMongo {

  // To remove INFO lines from terminal while running the code
  Logger.getLogger("org").setLevel(Level.OFF)

  // Fetch ALL Content from mongoDB
  def fetchAllJsonString(): String = {

    // Configure the session with MongoDB
    val spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter_db.covid_tweets")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter_db.covid_tweets")
      .getOrCreate()

    val tweets_all: DataFrame = spark.read.format("com.mongodb.spark.sql.DefaultSource")
      .option("uri", "mongodb://localhost:27017/twitter_db.covid_tweets")
      .load()
    // tweets_all
    // "_id"             : {  "oid": "624dd4a49898fd74d9963671"},
    // "created_at"       : "Wed Apr 06 15:30:55 +0000 2022",
    // "entities_hashtags": "[]",
    // "id"               : "1511728162857619458",

    dataframeToJsonString(tweets_all)
  }

  def fetchQueryJsonString(pipeline: String): String = {

    // Configure the session with MongoDB
    val spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter_db.covid_tweets")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter_db.covid_tweets")
      .getOrCreate()

    val df = spark.read.format("com.mongodb.spark.sql.DefaultSource")
      .option("pipeline", pipeline)  // db.getCollection("covid_tweets").aggregate(pipeline)
      .option("uri","mongodb://localhost:27017/twitter_db.covid_tweets")
      .load()
    dataframeToJsonString(df)
  }

  // Query - 1 with filter on "date"
  def query1_date(startdate:String = "",enddate:String = ""): String = {

    // Configure the session with MongoDB
    val spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter_db.table1")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter_db.table1")
      .getOrCreate()

    if (startdate != "" && enddate != "") {
      val dfr = spark.sqlContext.read.option("pipeline",
        s"""[{ $$match: { "date": { $$gte: ISODate("$startdate"), $$lt: ISODate("$enddate")}}},
            |{ $$group: { _id: "$$location", tweet_count: { $$sum: "$$tweet_count"}}},
            |{ $$sort: { tweet_count: -1 }},
            |{ $$limit: 10 }]""".stripMargin)

      val df = dfr.option("uri", "mongodb://localhost:27017/twitter_db.table1").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(df)
    }

    else if (startdate != "") {
      val dfr = spark.sqlContext.read.option("pipeline",
        s"""[{ $$match: { "date": { $$gte: ISODate("$startdate")}}},
            |{ $$group: { _id: "$$location", tweet_count: { $$sum: "$$tweet_count"}}},
            |{ $$sort: { tweet_count: -1 }}
            |{ $$limit: 10 }]""".stripMargin)

      val df = dfr.option("uri", "mongodb://localhost:27017/twitter_db.table1").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(df)
    }

    else if (enddate != "") {
      val dfr = spark.sqlContext.read.option("pipeline",
        s"""[{ $$match: { "date": { $$lt: ISODate("$enddate")}}},
            |{ $$group: { _id: "$$location", tweet_count: { $$sum: "$$tweet_count"}}},
            |{ $$sort: { tweet_count: -1}}
            |{ $$limit: 10 }]""".stripMargin)

      val df = dfr.option("uri", "mongodb://localhost:27017/twitter_db.table1").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(df)
    }

    else {
      val dfr = spark.sqlContext.read.option("pipeline",
        s"""[{ $$group: { _id: "$$location", tweet_count: { $$sum: "$$tweet_count"}}},
            |{ $$sort: { tweet_count: -1}}
            |{ $$limit: 10 }]""".stripMargin)

      val df = dfr.option("uri", "mongodb://localhost:27017/twitter_db.table1").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(df)
    }
  }

  // Query - 2 with filter on "date"
  def query2_date(startdate:String = "",enddate:String = ""): String = {

    // Configure the session with MongoDB
    val spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter_db.table1")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter_db.table1")
      .getOrCreate()

    if (startdate != "" && enddate != "") {
      val dfr = spark.sqlContext.read.option("pipeline",
        s"""[{ $$match: { "date": { $$gte: ISODate("$startdate"), $$lt: ISODate("$enddate")}}},
            |{ $$sort: {tweet_count: -1 }},
            |{ $$limit: 10 }]""".stripMargin)

      val df = dfr.option("uri", "mongodb://localhost:27017/twitter_db.table1").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(df)
    }

    else if (startdate != "") {
      val dfr = spark.sqlContext.read.option("pipeline",
        s"""[{ $$match: { "date": { $$gte: ISODate("$startdate")}}},
            |{ $$sort: { tweet_count: -1 }}
            |{ $$limit: 10 }]""".stripMargin)

      val df = dfr.option("uri", "mongodb://localhost:27017/twitter_db.table1").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(df)
    }

    else if (enddate != "") {
      val dfr = spark.sqlContext.read.option("pipeline",
        s"""[{ $$match: { "date": { $$lt: ISODate("$enddate")}}},
            |{ $$sort: { tweet_count: -1}}
            |{ $$limit: 10 }]""".stripMargin)

      val df = dfr.option("uri", "mongodb://localhost:27017/twitter_db.table1").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(df)
    }

    else {
      val dfr = spark.sqlContext.read.option("pipeline",
        s"""[{ $$sort: { tweet_count: -1}}
            |{ $$limit: 10 }]""".stripMargin)

      val df = dfr.option("uri", "mongodb://localhost:27017/twitter_db.table1").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(df)
    }
  }

  // Query - 3 with filter on "date"
  def query3_date(startdate: String = "", enddate: String = ""): String = {

    // Configure the session with MongoDB
    val spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter_db.table2")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter_db.table2")
      .getOrCreate()

    if(startdate != "" && enddate != "") {
      val df = spark.sqlContext.read.option("pipeline",
        s"""[{ $$match: {"date": { $$gte: ISODate("$startdate"), $$lt: ISODate("$enddate")}}},
            |{ $$group: {"_id": "$$word", "frequency": { $$sum: "$$count"}}},
            |{ $$sort: {"frequency": -1}}]""".stripMargin)

      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.table2").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

    else if(startdate != "") {
      val df = spark.sqlContext.read.option("pipeline",
        s"""[{ $$match: {"date": { $$gte: ISODate("$startdate")}}},
            |{ $$group: {"_id": "$$word", "frequency": { $$sum: "$$count"}}},
            |{ $$sort: {"frequency": -1}}]""".stripMargin)

      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.table2").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

    else if(enddate != "") {
      val df = spark.sqlContext.read.option("pipeline",
        s"""[{ $$match: { "date": { $$lt: ISODate("$enddate")}}},
            |{ $$group: { "_id": "$$word", "frequency": { $$sum: "$$count"}}},
            |{ $$sort: { "frequency": -1}}]""".stripMargin)

      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.table2").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

    else {
      val df = spark.sqlContext.read.option("pipeline",
        s"""[{ $$group: { "_id": "$$word", "frequency": { $$sum: "$$count"}}},
            |{ $$sort: { "frequency": -1}}
            |]""".stripMargin)
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.table2").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }
  }

  // Query - 4 with filter on "created_at"
  def query4_date(startdate:String = "", enddate:String = ""): String = {

    // Configure the session with MongoDB
    val spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter_db.covid_tweets")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter_db.covid_tweets")
      .getOrCreate()

      if(startdate != "" && enddate != "") {
      val df = spark.sqlContext.read.option("pipeline", s"""[{ $$match : {"created_at": { $$gte: ISODate("$startdate"), $$lt: ISODate("$enddate") } } },{ $$project: { user_location: "$$user_location", words: { $$split: ["$$text", " "] } } },{ $$unwind: "$$words" },{ $$match : { words: { $$nin: ["a", "I", "are", "is", "to", "the", "of", "and", "in", "RT", "was", "on" , "for"]} } },{ $$group: { _id: { location: "$$user_location", word: "$$words"}, total: { "$$sum": 1 } } },{ $$sort: { total : -1 } },{ $$limit: 100 }]""")
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.covid_tweets").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

      else if(startdate!=""){
      val df = spark.sqlContext.read.option("pipeline", s"""[{ $$match : {"created_at": { $$gte: ISODate("$startdate") } } },{ $$project: { user_location: "$$user_location", words: { $$split: ["$$text", " "] } } },{ $$unwind: "$$words" },{ $$match : { words: { $$nin: ["a", "I", "are", "is", "to", "the", "of", "and", "in", "RT", "was", "on" , "for"]} } },{ $$group: { _id: { location: "$$user_location", word: "$$words"}, total: { "$$sum": 1 } } },{ $$sort: { total : -1 } },{ $$limit: 100 }]""")
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.covid_tweets").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

      else if(enddate !=""){
      val df = spark.sqlContext.read.option("pipeline", s"""[{ $$match : {"created_at": { $$lt: ISODate("$enddate") } } },{ $$project: { user_location: "$$user_location", words: { $$split: ["$$text", " "] } } },{ $$unwind: "$$words" },{ $$match : { words: { $$nin: ["a", "I", "are", "is", "to", "the", "of", "and", "in", "RT", "was", "on" , "for"]} } },{ $$group: { _id: { location: "$$user_location", word: "$$words"}, total: { "$$sum": 1 } } },{ $$sort: { total : -1 } },{ $$limit: 100 }]""")
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.covid_tweets").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

      else {
      val df = spark.sqlContext.read.option("pipeline", s"""[{ $$project: { user_location: "$$user_location", words: { $$split: ["$$text", " "] } } },{ $$unwind: "$$words" },{ $$match : { words: { $$nin: ["a", "I", "are", "is", "to", "the", "of", "and", "in", "RT", "was", "on" , "for"]} } },{ $$group: { _id: { location: "$$user_location", word: "$$words"}, total: { "$$sum": 1 } } },{ $$sort: { total : -1 } },{ $$limit: 100 }]""")
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.covid_tweets").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }
  }

  // Query - 5 with filter on "created_at"
  def query5_date( startdate:String = "",enddate:String = ""): String = {

    // Configure the session with MongoDB
    val spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter_db.who_tweets")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter_db.who_tweets")
      .getOrCreate()

    if(startdate != "" && enddate != "") {
      val df = spark.sqlContext.read.option("pipeline", s"""[{ $$match: {$$and: [{ "full_text": { $$regex: "prevent.*|precaut.*", $$options: "i" } }, {"created_at": { $$gte: ISODate("$startdate"), $$lt: ISODate("$enddate") }}]}},{ $$limit: 10 }]""")
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.who_tweets").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

    else if(startdate != "") {
      val df = spark.sqlContext.read.option("pipeline", s"""[{ $$match: {$$and: [{ "full_text": { $$regex: "prevent.*|precaut.*", $$options: "i" } }, {"created_at": { $$gte: ISODate("$startdate") }}]}},{ $$limit: 10 }]""")
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.who_tweets").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

    else if(enddate != "") {
      val df = spark.sqlContext.read.option("pipeline", s"""[{ $$match: {$$and: [{ "full_text": { $$regex: "prevent.*|precaut.*", $$options: "i" } }, {"created_at": { $$lt: ISODate("$enddate") } }]}},{ $$limit: 10 }]""")
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.who_tweets").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

    else {
      val df = spark.sqlContext.read.option("pipeline", s"""[{ $$match: {$$and: [{ "full_text": { $$regex: "prevent.*|precaut.*", $$options: "i" } }]}},{ $$limit: 10 }]""")
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.who_tweets").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }
  }

  // Query - 5 with filter on the "no. of tweets"
  def query5_limit(Limit:Int): String = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter_db.who_tweets")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter_db.who_tweets")
      .getOrCreate()

    val df = spark.sqlContext.read.option("pipeline", s"""[{ $$match: {$$and: [{ "full_text": { $$regex: "prevent.*|precaut.*", $$options: "i" } }]}},{ $$limit: $Limit }]""")
    val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.who_tweets").format("com.mongodb.spark.sql.DefaultSource").load()
    dataframeToJsonString(newdf)
  }

  // Query - 6 with filter on "dateConfirmed"
  def query6_date(startdate:String = "", enddate:String = ""): String = {

    // Configure the session with MongoDB
    val spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter_db.donations")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter_db.donations")
      .getOrCreate()

    if(startdate != "" && enddate != "") {
      val df = spark.sqlContext.read.option("pipeline", s"""[{ $$match : {"dateConfirmed": { $$gte: ISODate("$startdate"), $$lt: ISODate("$enddate") } } },
                                                            |{ $$group: { _id: "$$source", Count: { $$sum: 1 }, Total: { $$sum: "$$amount" } } },{ $$sort: { Total: -1 } },{ $$limit: 10 }]""".stripMargin)
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.donations").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

    else if(startdate != "") {
      val df = spark.sqlContext.read.option("pipeline", s"""[{ $$match : {"dateConfirmed": { $$gte: ISODate("$startdate") } } },
                                                            |{ $$group: { _id: "$$source", Count: { $$sum: 1 }, Total: { $$sum: "$$amount" } } },{ $$sort: { Total: -1 } },{ $$limit: 10 }]""".stripMargin)
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.donations").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

    else if(enddate != "") {
      val df = spark.sqlContext.read.option("pipeline", s"""[{ $$match : {"dateConfirmed": {$$lt: ISODate("$enddate") } } },
                                                            |{ $$group: { _id: "$$source", Count: { $$sum: 1 }, Total: { $$sum: "$$amount" } } },{ $$sort: { Total: -1 } },{ $$limit: 10 }]""".stripMargin)
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.donations").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

    else {
      val df = spark.sqlContext.read.option("pipeline", s"""[
                                                            |{ $$group: { _id: "$$source", Count: { $$sum: 1 }, Total: { $$sum: "$$amount" } } },{ $$sort: { Total: -1 } },{ $$limit: 10 }]""".stripMargin)
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.donations").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }
  }

  // Query - 6 with filter on "sortBy" and the "no. of records"
  def query6_sort(Sort:String = "", Limit: Int): String = {

    // Configure the session with MongoDB
    val spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter_db.donations")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter_db.donations")
      .getOrCreate()

    if(Sort == "count") {
      val df = spark.sqlContext.read.option("pipeline", s"""[
                                                            |{ $$group: { _id: "$$source", Count: { $$sum: 1 }, Total: { $$sum: "$$amount" } } },{ $$sort: { Count: -1 } },{ $$limit: $Limit }]""".stripMargin)
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.donations").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

    else {
      val df = spark.sqlContext.read.option("pipeline", s"""[
                                                            |{ $$group: { _id: "$$source", Count: { $$sum: 1 }, Total: { $$sum: "$$amount" } } },{ $$sort: { Total: -1 } },{ $$limit: $Limit }]""".stripMargin)
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.donations").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }
  }

  // Query - 7 with filter on "Last Updated"
  def query7_date(startdate:String = "", enddate:String = ""): String = {

    // Configure the session with MongoDB
    val spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter_db.cases_data")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter_db.cases_data")
      .getOrCreate()

    if(startdate != "" && enddate != "") {
      val df = spark.sqlContext.read.option("pipeline",
        s"""[{ $$match : {"Last Updated": { $$gte: ISODate("$startdate"), $$lt: ISODate("$enddate") } } },{ $$project:{"location": "$$Country", "RankOfImpactedCountry":1,"confirmedCases":"$$Confirmed cases", _id:0}},
            |{ $$setWindowFields: {sortBy: { confirmed: -1 },output: {RankOfImpactedCountry: {$$rank: {}}}}}]""".stripMargin)
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.cases_data").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

    else if(startdate != "") {
      val df = spark.sqlContext.read.option("pipeline",
        s"""[{ $$match : {"Last Updated": { $$gte: ISODate("$startdate") } } },{ $$project:{"location": "$$Country", "RankOfImpactedCountry":1,"confirmedCases":"$$Confirmed cases", _id:0}},
            |{ $$setWindowFields: {sortBy: { confirmed: -1 },output: {RankOfImpactedCountry: {$$rank: {}}}}}]""".stripMargin)
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.cases_data").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

    else if(enddate != "") {
      val df = spark.sqlContext.read.option("pipeline",
        s"""[{ $$match : {"Last Updated": {  $$lt: ISODate("$enddate") } } },{ $$project:{"location": "$$Country", "RankOfImpactedCountry":1,"confirmedCases":"$$Confirmed cases", _id:0}},
            |{ $$setWindowFields: {sortBy: { confirmed: -1 },output: {RankOfImpactedCountry: {$$rank: {}}}}}]""".stripMargin)
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.cases_data").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

    else {
      val df = spark.sqlContext.read.option("pipeline",
        s"""[{ $$project:{"location":"$$Country", "RankOfImpactedCountry":1,"confirmedCases":"$$Confirmed cases", _id:0}},
            |{ $$setWindowFields: { sortBy: { confirmed: -1 },output: {RankOfImpactedCountry: {$$rank: {}}}}}]""".stripMargin)
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.cases_data").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }
  }

  // Query - 7 with filter on "Country"
  def query7_country(country:String = ""): String = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter_db.cases_data")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter_db.cases_data")
      .getOrCreate()

    val df = spark.sqlContext.read.option("pipeline",
      s"""[
          |{ $$match: { "Country": { $$regex: "$country", $$options: "i" } } },
          ]""".stripMargin)
    val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.cases_data").format("com.mongodb.spark.sql.DefaultSource").load()
    dataframeToJsonString(newdf)
  }

  // Query - 8 with filter on "Date"
  def query8_date(startdate:String = "", enddate:String = ""): String = {

    // Configure the session with MongoDB
    val spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter_db.global_economy")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter_db.global_economy")
      .getOrCreate()

    if(startdate != "" && enddate != "") {
      val df = spark.sqlContext.read.option("pipeline",
        s"""[{ $$match: { "Date": { $$gt: ISODate("$startdate"), $$lt: ISODate("$enddate") } } },
            |{ $$group: { _id: "$$Country", total: { $$sum: "$$GDP" } } },
            |{ $$sort: { total: -1 } },
            |{ $$limit: 10 }]""".stripMargin)
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.global_economy").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

    else if(startdate != "") {
      val df = spark.sqlContext.read.option("pipeline",
        s"""[{ $$match: { "Date": { $$gt: ISODate("$startdate")} } },
            |{ $$group: { _id: "$$Country", total: { $$sum: "$$GDP" } } },
            |{ $$sort: { total: -1 } },
            |{ $$limit: 10 }]""".stripMargin)
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.global_economy").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

    else if(enddate != "") {
      val df = spark.sqlContext.read.option("pipeline",
        s"""[{ $$match: { "Date": { $$lt: ISODate("$enddate") } } },
            |{ $$group: { _id: "$$Country", total: { $$sum: "$$GDP" } } },
            |{ $$sort: { total: -1 } },
            |{ $$limit: 10 }]""".stripMargin)
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.global_economy").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }

    else {
      val df = spark.sqlContext.read.option("pipeline",
        s"""[
            |{ $$group: { _id: "$$Country", total: { $$sum: "$$GDP" } } },
            |{ $$sort: { total: -1 } },
            |{ $$limit: 10 }]""".stripMargin)
      val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.global_economy").format("com.mongodb.spark.sql.DefaultSource").load()
      dataframeToJsonString(newdf)
    }
  }

  // Query - 8 with filter on "Country"
  def query8_country(country:String = ""): String = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("demo")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter_db.global_economy")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter_db.global_economy")
      .getOrCreate()

    val df = spark.sqlContext.read.option("pipeline",
      s"""[
          |{ $$match: { "Country": { $$regex: "$country", $$options: "i" } } },
          ]""".stripMargin)
    val newdf = df.option("uri", "mongodb://localhost:27017/twitter_db.global_economy").format("com.mongodb.spark.sql.DefaultSource").load()
    dataframeToJsonString(newdf)
  }

  def dataframeToJsonString(df: DataFrame): String = {

    val output_df = df.select(to_json(struct(col("*"))).alias("content"))
    // output_df
    //       content
    // {"_id":{"oid":"62"...
    // {"_id":{"oid":"63...

    val json_string: Array[Any] = output_df.select("content").rdd.map(r => r(0)).collect()
    // json_string
    // { "_id": { "oid": "624dd4a49898fd74d9963671"}, "created_at": "Wed Apr 06 15:30:55 +0000 2022", "entities_hashtags": "[]", "id": "15117281...
    // { "_id": { "oid": "624dd4a49898fd74d9963672"}, "created_at": "Wed Apr 06 15:30:55 +0000 2022", "entities_hashtags": "[]", "id": "151172816...

    json_string.mkString("[", ",", "]")  // [  {}, {}, {} ]
  }

  def main(args: Array[String]): Unit = {
    //    print(fetchAllJsonString())
  }
}