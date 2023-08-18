# Importando pacotes
import pandas as pd
from pymongo import MongoClient
from createDF import create_dfs
from bson.son import SON

# Chamar função pra criar os dataframes
dfCars, dfDealership = create_dfs()

# Conectando ao servidor Mongo
client = MongoClient("localhost", 27017)

# Declarando o database
db = client.tecnicaltest
collectionNames = db.list_collection_names()

# Declarando as collections
carsCollection = db.Carros
dealsCollection = db.Montadoras
aggr_all = db.Agregação
group_all = db.Agrupamento

# Para não acumular dados durante o teste, foi usado uma condicional para adicionar valores
if "Carros" not in collectionNames:
    carsCollection.insert_many(dfCars.to_dict('records'))
else: 
    carsCollection.drop()
    carsCollection.insert_many(dfCars.to_dict('records'))

if "Montadoras" not in collectionNames:
    dealsCollection.insert_many(dfDealership.to_dict('records'))
else:
    dealsCollection.drop()
    dealsCollection.insert_many(dfDealership.to_dict('records'))

# Declaração do pipeline para agregação
aggr_pipeline = [
    {
        "$lookup": {
            "from": "Montadoras",
            "localField": "Montadora",
            "foreignField": "Montadora",
            "as": "Montadoras"
        }
    },
    {
        "$addFields": {
            "País": "$Montadoras.País"
        }
    },
    {
        "$project": { "Montadoras.País": 0 }
    },
    {
        "$unwind": "$País"
    },
    {
        "$out": "Agregação"
    }
]

# Declaração do pipeline para agrupamento
group_pipeline = [
    {
        "$lookup": {
            "from": "Carros",
            "localField": "Carros",
            "foreignField": "Carros",
            "as": "Carros"
        }
    },
    {
        "$unwind": "$Carros"
    },
    {
        "$group": {
            "_id": "$País",
            "Carros": {"$push": "$Carros"}
        }
    },
    {
        "$out": "Agrupamento"
    }
]

# Funções de agregações para
carsCollection.aggregate(aggr_pipeline)

aggr_all.aggregate(group_pipeline)
