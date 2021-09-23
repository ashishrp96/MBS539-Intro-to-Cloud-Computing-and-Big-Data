from mrjob.job import MRJob
import re
from datetime import datetime
from collections import Counter
from operator import itemgetter
from itertools import islice

WORD_RE = re.compile(r"[\w']+")

def Sort_Tuple(tup):  
      
    # getting length of list of tuples 
    lst = len(tup)  
    for i in range(0, lst):  
          
        for j in range(0, lst-i-1):  
            if (tup[j][1] > tup[j + 1][1]):  
                temp = tup[j]  
                tup[j]= tup[j + 1]  
                tup[j + 1]= temp  
    return tup

class MRMostfrequent(MRJob):
    
    def mapper(self, _, line):
        complaint = line.split(",")[5]
        yield ("top", (complaint,1))
               
    def reducer(self, key, value):
        res = list(Counter(key for key, num in value for idx in range(num)).items())
        fin = sorted(res, key = lambda x: x[1], reverse=True)[0:4]
        yield ("top", fin)
        
        

if __name__ == '__main__':
    MRMostfrequent.run()