[
  { "$match": { "flags.is_purchased": { "$ne": null } } },
  { "$group": {
      "_id": "$campaign_id",
      "total_sent": { "$sum": 1 },
      "total_purchased": { "$sum": { "$cond": ["$flags.is_purchased", 1, 0] } }
  } },
  { "$project": {
      "campaign_id": "$_id",
      "total_sent": 1,
      "total_purchased": 1
  } },
  { "$sort": { "campaign_id": 1 } }
]