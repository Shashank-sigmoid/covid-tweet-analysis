// Ranking of impacted countries over the last 2 months


db.cases_data.aggregate([{$project:{"location":"$location", "RankOfImpactedCountries":1,"confirmedCases":"$confirmed", _id:0}},{
$setWindowFields: {
sortBy: { confirmed: -1 },
output: {
RankOfImpactedCountries: {
$rank: {}}}}
}])
