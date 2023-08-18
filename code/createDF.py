# Importando pacotes
import pandas as pd
from bson.son import SON
# Definição dos dados do DataFrame dfCars
carsData = SON({"Carros": ["Onix","Polo","Sandero","Fiesta","City"],
            "Cor": ["Prata","Branco","Prata","Vermelho","Preto"],
            "Montadora": ["Chevrolet","Volkswagen","Renault","Ford","Honda"]})

# Definição dos dados do DataFrame dfDealership
dealershipData = SON({"Montadora": ["Chevrolet","Volkswagen","Renault","Ford","Honda"], 
                  "País": ["EUA","Alemanha","França","EUA","Japão"]})
def create_dfs():
    # Declaração dos dataframes
    dfCars= pd.DataFrame(data=carsData)
    dfDealership = pd.DataFrame(data=dealershipData)
    
    return dfCars, dfDealership