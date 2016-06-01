from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMoneyspentsorted(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_orders,
                   reducer=self.reducer_get_totals),
            MRStep(mapper=self.mapper_make_totals_key,
                   reducer = self.reducer_get_moneyspentsorted)
        ]
        
    def mapper_get_orders(self, _, line):
        (customerID, itemID, amountspent) = line.split(',')
        yield customerID, float(amountspent)
        
    def reducer_get_totals(self, customerID, moneyspent):
        yield customerID, sum(moneyspent)
        
    def mapper_make_totals_key(self, customerID, totalspent):
        yield '%04.02f'%float(totalspent), customerID
        
    def reducer_get_moneyspentsorted(self, moneyspent, customerID):
        for id in customerID:
            yield moneyspent,id

            
if __name__ == "__main__":
    MRMoneyspentsorted.run()