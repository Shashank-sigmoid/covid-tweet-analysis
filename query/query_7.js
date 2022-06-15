// Ranking of impacted countries over the last 2 months


db.cases_data.aggregate([
{ $project: { "location": "$Country", "RankOfImpactedCountries": 1, "confirmedCases": "$Confirmed cases", _id: 0 } },
{ $setWindowFields:
{ sortBy: { confirmed: -1 },
output: { RankOfImpactedCountries:
{ $rank: {}}}}}
])

