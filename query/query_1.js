// Total number of tweets for a particular location

db.covid_tweets.aggregate
 ([ { $group { _id : "user_location", text : { $count : { } } } ] )

 
