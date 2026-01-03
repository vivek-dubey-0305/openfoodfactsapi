import json
import pymongo
from flask import request, abort, json
import flask_restful as restful
from flask_rest_service import app, api, mongo
from bson.objectid import ObjectId

# ----- /products -----
class ProductsList(restful.Resource):

    # ----- GET Request -----
    def get(self):
        # ----- Get limit # in the request, 50 by default -----
        limit = request.args.get('limit',default=50,type=int)
        # ----- Get skip # in the request, 0 by default -----
        skip = request.args.get('skip',default=0,type=int)
        # ----- Get count # in the request, 0 by default -----
        count = request.args.get('count',default=0, type=int)
        # ----- See if we want short response or not, 0 by default -----
        short = request.args.get('short',default=0, type=int)
        # ----- Query -----
        query = request.args.get('q')


        # ----- The regex param makes it search "like" things, the options one tells to be case insensitive
        data = dict((key, {'$regex' : request.args.get(key), '$options' : 'i'}) for key in request.args.keys())

        # ----- Delete custom parameters from the request, since they are not in MongoDB -----
        # ----- And add filters to custom parameters : -----
        if request.args.get('limit'):
            del data['limit']

        if request.args.get('skip'):
            del data['skip']

        if request.args.get('count'):
            del data['count']

        if request.args.get('short'):
            del data['short']

        if request.args.get('q'):
            del data['q']

        # ----- Filter data returned -----
        # ----- If we just want a short response -----
        # ----- Tell which fields we want -----
        fieldsNeeded = {'code':1, 'lang':1, 'product_name':1}
        if request.args.get('short') and short == 1:
            if request.args.get('count') and count == 1 and not request.args.get('q'):
                return mongo.db.products.count_documents(data)
            elif not request.args.get('count') and not request.args.get('q'):
                return mongo.db.products.find(data, fieldsNeeded).sort('created_t', pymongo.DESCENDING).skip(skip).limit(limit)
            elif request.args.get('count') and count == 1 and request.args.get('q'):
                return mongo.db.products.count_documents({ "$text" : { "$search": query } })
            else:
                return mongo.db.products.find({ "$text" : { "$search": query } }, fieldsNeeded).sort('created_t', pymongo.DESCENDING).skip(skip).limit(limit)
        else:
            if request.args.get('count') and count == 1 and not request.args.get('q'):
                return mongo.db.products.count_documents(data)
            elif not request.args.get('count') and not request.args.get('q'):
                return mongo.db.products.find(data).sort('created_t', pymongo.DESCENDING).skip(skip).limit(limit)
            elif request.args.get('count') and count == 1 and request.args.get('q'):
                return mongo.db.products.count_documents({ "$text" : { "$search": query } })
            else:
                return mongo.db.products.find({ "$text" : { "$search": query } }).sort('created_t', pymongo.DESCENDING).skip(skip).limit(limit)


# ----- /products/stats/info-----
class ProductsStats(restful.Resource):

    # ----- GET Request -----
    def get(self):

        pipeline = [
            {"$match": {"created_t": {"$exists": True}}},
            {"$project": {
                "year": {"$year": {"$toDate": {"$multiply": ["$created_t", 1000]}}},
                "month": {"$month": {"$toDate": {"$multiply": ["$created_t", 1000]}}},
                "saltlevels": "$nutrient_levels.salt",
                "fatlevels": "$nutrient_levels.fat",
                "saturatedfatlevels": "$nutrient_levels.saturated-fat",
                "sugarslevels": "$nutrient_levels.sugars"
            }},
            {"$group": {
                "_id": {
                    "year": "$year",
                    "month": "$month",
                    "saltlevels": "$saltlevels",
                    "fatlevels": "$fatlevels",
                    "saturatedfatlevels": "$saturatedfatlevels",
                    "sugarslevels": "$sugarslevels"
                },
                "count": {"$sum": 1}
            }},
            {"$project": {
                "_id": 0,
                "dateyear": "$_id.year",
                "datemonth": "$_id.month",
                "count": "$count",
                "saltlevels": "$_id.saltlevels",
                "saturatedfatlevels": "$_id.saturatedfatlevels",
                "sugarslevels": "$_id.sugarslevels",
                "fatlevels": "$_id.fatlevels"
            }}
        ]

        listRes = list(mongo.db.products.aggregate(pipeline))
        return listRes


# ----- /product/<product_id> -----
class ProductId(restful.Resource):

    # ----- GET Request -----
    def get(self, barcode):
        return  mongo.db.products.find_one({"code":barcode})


# ----- /products/brands -----
class ProductsBrands(restful.Resource):

    # ----- GET Request -----
    def get(self):
        # ----- Get count # in the request, 0 by default -----
        count = request.args.get('count',default=0, type=int)
        query = request.args.get('query')

        if request.args.get('query'):
            pipeline = [
                {"$match": {"brands": {"$regex": query, "$options": "i"}}},
                {"$group": {"_id": "$brands"}}
            ]
            res = [doc['_id'] for doc in mongo.db.products.aggregate(pipeline)]
            if request.args.get('count') and count == 1:
                return len(res)
            else:
                return res
        else:
            if request.args.get('count') and count == 1:
                result = mongo.db.products.aggregate([{"$group": {"_id": "$brands"}}, {"$count": "total"}])
                count_result = list(result)
                return count_result[0]['total'] if count_result else 0
            else:
                brands = [doc['_id'] for doc in mongo.db.products.aggregate([{"$group": {"_id": "$brands"}}])]
                return brands


# ----- /products/categories -----
class ProductsCategories(restful.Resource):

    # ----- GET Request -----
    def get(self):
        # ----- Get count # in the request, 0 by default -----
        count = request.args.get('count',default=0, type=int)
        query = request.args.get('query')

        if request.args.get('query'):
            pipeline = [
                {"$match": {"categories": {"$regex": query, "$options": "i"}}},
                {"$group": {"_id": "$categories"}}
            ]
            res = [doc['_id'] for doc in mongo.db.products.aggregate(pipeline)]
            if request.args.get('count') and count == 1:
                return len(res)
            else:
                return res
        else:
            if request.args.get('count') and count == 1:
                result = mongo.db.products.aggregate([{"$group": {"_id": "$categories"}}, {"$count": "total"}])
                count_result = list(result)
                return count_result[0]['total'] if count_result else 0
            else:
                categories = [doc['_id'] for doc in mongo.db.products.aggregate([{"$group": {"_id": "$categories"}}])]
                return categories


# ----- /products/countries -----
class ProductsCountries(restful.Resource):

    # ----- GET Request -----
    def get(self):
        # ----- Get count # in the request, 0 by default -----
        count = request.args.get('count',default=0, type=int)
        query = request.args.get('query')

        if request.args.get('query'):
            pipeline = [
                {"$match": {"countries": {"$regex": query, "$options": "i"}}},
                {"$group": {"_id": "$countries"}}
            ]
            res = [doc['_id'] for doc in mongo.db.products.aggregate(pipeline)]
            if request.args.get('count') and count == 1:
                return len(res)
            else:
                return res
        else:
            if request.args.get('count') and count == 1:
                result = mongo.db.products.aggregate([{"$group": {"_id": "$countries"}}, {"$count": "total"}])
                count_result = list(result)
                return count_result[0]['total'] if count_result else 0
            else:
                countries = [doc['_id'] for doc in mongo.db.products.aggregate([{"$group": {"_id": "$countries"}}])]
                return countries


# ----- /products/additives -----
class ProductsAdditives(restful.Resource):

    # ----- GET Request -----
    def get(self):
        # ----- Get count # in the request, 0 by default -----
        count = request.args.get('count',default=0, type=int)
        query = request.args.get('query')

        if request.args.get('query'):
            pipeline = [
                {"$unwind": "$additives_tags"},
                {"$match": {"additives_tags": {"$regex": query, "$options": "i"}}},
                {"$group": {"_id": "$additives_tags"}}
            ]
            res = [doc['_id'] for doc in mongo.db.products.aggregate(pipeline)]
            if request.args.get('count') and count == 1:
                return len(res)
            else:
                return res
        else:
            if request.args.get('count') and count == 1:
                result = mongo.db.products.aggregate([{"$unwind": "$additives_tags"}, {"$group": {"_id": "$additives_tags"}}, {"$count": "total"}])
                count_result = list(result)
                return count_result[0]['total'] if count_result else 0
            else:
                additives = [doc['_id'] for doc in mongo.db.products.aggregate([{"$unwind": "$additives_tags"}, {"$group": {"_id": "$additives_tags"}}])]
                return additives


# ----- /products/allergens -----
class ProductsAllergens(restful.Resource):

    # ----- GET Request -----
    def get(self):
        # ----- Get count # in the request, 0 by default -----
        count = request.args.get('count',default=0, type=int)
        query = request.args.get('query')

        if request.args.get('query'):
            pipeline = [
                {"$unwind": "$allergens_tags"},
                {"$match": {"allergens_tags": {"$regex": query, "$options": "i"}}},
                {"$group": {"_id": "$allergens_tags"}}
            ]
            res = [doc['_id'] for doc in mongo.db.products.aggregate(pipeline)]
            if request.args.get('count') and count == 1:
                return len(res)
            else:
                return res
        else:
            if request.args.get('count') and count == 1:
                result = mongo.db.products.aggregate([{"$unwind": "$allergens_tags"}, {"$group": {"_id": "$allergens_tags"}}, {"$count": "total"}])
                count_result = list(result)
                return count_result[0]['total'] if count_result else 0
            else:
                allergens = [doc['_id'] for doc in mongo.db.products.aggregate([{"$unwind": "$allergens_tags"}, {"$group": {"_id": "$allergens_tags"}}])]
                return allergens



api.add_resource(ProductsList, '/products')
api.add_resource(ProductsStats, '/products/stats/info')
api.add_resource(ProductId, '/product/<string:barcode>')
api.add_resource(ProductsBrands, '/products/brands')
api.add_resource(ProductsCategories, '/products/categories')
api.add_resource(ProductsCountries, '/products/countries')
api.add_resource(ProductsAdditives, '/products/additives')
api.add_resource(ProductsAllergens, '/products/allergens')