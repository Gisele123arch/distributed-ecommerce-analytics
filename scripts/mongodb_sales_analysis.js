
// MongoDB Aggregation Pipeline 1
// Sales performance by product category

db = db.getSiblingDB("ecommerce_analytics");

printjson(db.transactions.aggregate([
    // Include completed purchases
    {
        $match: {
            status: "delivered"
        }
    },

    // Expand purchased items
    {
        $unwind: "$items"
    },

    // Match each item to its product
    {
        $lookup: {
            from: "products",
            localField: "items.product_id",
            foreignField: "product_id",
            as: "product_details"
        }
    },

    {
        $unwind: "$product_details"
    },

    // Match products to categories
    {
        $lookup: {
            from: "categories",
            localField: "product_details.category_id",
            foreignField: "category_id",
            as: "category_details"
        }
    },

    {
        $unwind: "$category_details"
    },

    // Calculate category performance
    {
        $group: {
            _id: "$category_details.name",
            total_revenue: {
                $sum: "$items.subtotal"
            },
            units_sold: {
                $sum: "$items.quantity"
            },
            number_of_transactions: {
                $addToSet: "$transaction_id"
            }
        }
    },

    // Calculate distinct transaction count
    {
        $project: {
            category: "$_id",
            total_revenue: 1,
            units_sold: 1,
            transaction_count: {
                $size: "$number_of_transactions"
            },
            _id: 0
        }
    },

    // Highest revenue first
    {
        $sort: {
            total_revenue: -1
        }
    }
]).toArray());
