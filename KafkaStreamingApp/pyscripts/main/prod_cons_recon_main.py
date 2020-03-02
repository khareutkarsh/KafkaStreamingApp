"""
This is a main file to execute respective app modules
"""
from pyscripts.monitoring.prod_cons_recon import ProdConsRecon

if __name__ == "__main__":
    reconObj=ProdConsRecon()
    reconObj.producer_consumer_recon()