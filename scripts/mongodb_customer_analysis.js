
// MongoDB Aggregation Pipeline 2
// Customer spending and purchasing behaviour

db = db.getSiblingDB("ecommerce_analytics");

printjson(db.transactions.aggregate([
    // Include completed purchases only
    {
        $match: { status: "delivered" }
    },

    // Calculate spending per customer
    {
        $group: {
            _id: "$user_id",
            total_spent: { $sum: "$total" },
            purchase_count: { $sum: 1 },
            average_order_value: { $avg: "$total" }
        }
    },

    // Connect customer spending to user profiles
    {
        $lookup: {
            from: "users",
            localField: "_id",
            foreignField: "user_id",
            as: "customer"
        }
    },

    {
        $unwind: {
            path: "$customer",
            preserveNullAndEmptyArrays: true
        }
    },

    // Format the output
    {
        $project: {
            _id: 0,
            user_id: "$_id",
            total_spent: { $round: ["$total_spent", 2] },
            purchase_count: 1,
            average_order_value: {
                $round: ["$average_order_value", 2]
            }
        }
    },

    // Rank customers by spending
    {
        $sort: { total_spent: -1 }
    }
]).toArray());
