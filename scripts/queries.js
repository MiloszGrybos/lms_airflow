db.reviews.aggregate([
  { $group: { _id: "$content", count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 5 }
])

db.reviews.find(
  { $expr: { $lt: [{ $strLenCP: "$content" }, 5] } },
  { _id: 0, review_id: 1, content: 1 }
)

db.reviews.aggregate([
  { $match: { created_date: { $type: "date" }, score: { $type: "number" } } },
  { $group: {
      _id: { $dateTrunc: { date: "$created_date", unit: "day" } },
      avg_score: { $avg: "$score" }
  } },
  { $sort: { _id: 1 } }
])
