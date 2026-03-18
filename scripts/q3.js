[
  { "$match": { "user_id": "548885063" } },
  { "$facet": {
      "user_products": [
          { "$group": { "_id": null, "products": { "$addToSet": "$product.product_id" } } }
      ],
      "top_categories": [
          { "$group": { "_id": "$product.category_id", "count": { "$sum": 1 } } },
          { "$sort": { "count": -1 } },
          { "$limit": 3 }
      ]
  } },
  { "$project": {
      "user_products": { "$arrayElemAt": ["$user_products.products", 0] },
      "top_categories": 1
  } },
  { "$unwind": "$top_categories" },
  { "$lookup": {
      "from": "events",
      "let": { "cat_id": "$top_categories._id", "user_prods": "$user_products" },
      "pipeline": [
          { "$match": {
              "$expr": { "$eq": ["$product.category_id", "$$cat_id"] }
          } },
          { "$group": {
              "_id": "$product.product_id",
              "total_interactions": { "$sum": 1 },
              "category_code": { "$first": "$product.category_code" }
          } },
          { "$match": {
              "$expr": { "$not": { "$in": ["$_id", "$$user_prods"] } }
          } },
          { "$sort": { "total_interactions": -1 } },
          { "$limit": 5 }
      ],
      "as": "base_products"
  } },
  { "$unwind": "$base_products" },
  { "$project": {
      "original_product_id": "$base_products._id",
      "original_category_code": "$base_products.category_code"
  } }
]