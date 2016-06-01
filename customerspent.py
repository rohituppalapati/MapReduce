from mrjob.job import MRJob

class MRMoneyspent(MRJob):
    
    def mapper(self, _, line):
        (customerID, itemID, amountspent) = line.split(',')
        yield customerID, float(amountspent)
        
    def reducer(self, customerID, moneyspent):
        yield customerID, sum(moneyspent)
        
if __name__ == "__main__":
    MRMoneyspent.run()
    
    