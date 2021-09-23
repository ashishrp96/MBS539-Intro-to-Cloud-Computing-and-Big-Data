from mrjob.job import MRJob
import re
from datetime import datetime
from collections import Counter
from operator import itemgetter

WORD_RE = re.compile(r"[\w']+")

#Top four complaint types are stored in list
complaint_types = ["noise - residential", "heat/hot water", "illegal parking", "street condition"]
data_channel_types = ["mobile", "phone", "online"]
year = 2019
format = '%m/%d/%Y %I:%M:%S %p'
month_list = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
month_dict={"Jan":0,"Feb":0,"Mar":0,"Apr":0,"May":0,"Jun":0,"Jul":0,"Aug":0,"Sep":0,"Oct":0,"Nov":0,"Dec":0}
bor_list=["queens", "bronx", "staten island", "manhattan"]
class MR_CT(MRJob):
    
    def mapper(self, _, line):
        cha = line.split(",")[28]
        compnum = line.split(",")[5]
        T = line.split(",")[1]
        bor=line.split(",")[25]
        for complaint in complaint_types:
            if complaint == compnum.lower():
                for channel_type in data_channel_types:
                    if channel_type == cha.lower():
                        yield (complaint+"_"+channel_type, 1)
                
        if T.strip()=="Created Date":
            yield T,1
        else:
            dateTime_var = datetime.strptime(T, format) 
            y = dateTime_var.year
            m = dateTime_var.month
            for complaint in complaint_types:
                if complaint == compnum.lower():
                    if y == year:
                        yield (str(year)+"_"+str(m), 1)
            if y==2018:
                if compnum.lower() == "street condition":
                    for boro in bor_list:
                        if boro in bor.lower():
                            month_number = str(m)
                            ob = datetime.strptime(month_number, "%m")
                            mo = ob.strftime("%b")
                            yield (mo,(boro, 1))

               
                    
                                          
    def reducer(self, key, value):
        if key.startswith("2019"):
            yield(key, sum(value)/4)
        
        elif key.startswith(("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec")):
            res = list(Counter(key for key, num in value  
                  for idx in range(num)).items())
            Output=sorted(res, key = lambda x: x[1], reverse=True)[0:1]
#             fin=max(res,key=itemgetter(1))[1]
#             Output = [item for item in res if item[1] == fin]
            

            yield key, Output
               
        else:
            yield(key, sum(value))
            

                
if __name__ == '__main__':
    MR_CT.run()