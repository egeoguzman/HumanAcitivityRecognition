#! /usr/bin/python3

from mrjob.job import MRJob

class KurtosisMRJob(MRJob):

    DELIMITER = ","
    FILE_HEADER = 'ankleAcc16_1,ankleAcc16_2,ankleAcc16_3,ankleAcc6_1,ankleAcc6_2,ankleAcc6_3,ankleGyro1,ankleGyro2,ankleGyro3,ankleMagne1,ankleMagne2,ankleMagne3,ankleTemperature,chestAcc16_1,chestAcc16_2,chestAcc16_3,chestAcc6_1,chestAcc6_2,chestAcc6_3,chestGyro1,chestGyro2,chestGyro3,chestMagne1,chestMagne2,chestMagne3,chestTemperature,handAcc16_1,handAcc16_2,handAcc16_3,handAcc6_1,handAcc6_2,handAcc6_3,handGyro1,handGyro2,handGyro3,handMagne1,handMagne2,handMagne3,handTemperature,heartrate,timestamp'
    
#     with open('/home/hduser/kdd99-analysis/outputs/mean.csv') as fin:
#         means = [float(line[line.find("\t")+len("\t"):line.rfind("\n")]) for line in fin]
        
#     with open('/home/hduser/kdd99-analysis/outputs/std.csv') as fin:
#         stds = [float(line[line.find("\t")+len("\t"):line.rfind("\n")]) for line in fin]

    def mapper(self, _, row):
        columns = self.FILE_HEADER.split(self.DELIMITER)
        if row != self.FILE_HEADER:
            values = row.split(self.DELIMITER)
            col_count = len(values)
            
            for i in range(col_count):
                yield (columns[i], float(values[i]))
  
    def reducer(self, key, value):
                       
        values = [v for v in list(value)]
                       
        def variance(vals):
            mean = sum(vals) / len(vals)
            deviations = [mean ** 2 for i in vals]
            return (sum(deviations) / (len(deviations) - 1))
                       
        def mean(vals):
            return sum(vals)/len(vals)

        def standard_deviation(vals):
            return variance(vals) ** 0.5
        
        def kurtosis(vals):
            m2_e = m4_e = 0
            means = mean(vals)
            N = len(vals)
            stds = standard_deviation(vals)
            for v in vals:
                m2_e += (v - means)**2
                m4_e += (v - means)**4
            
            m2 = m2_e / N
            m4 = m4_e / N
            
            if m2 != 0 and m4 != 0:
                
                kurtosis = m4 / (m2**2)
            else:
                kurtosis = 0.0
                
            return kurtosis
        
        yield key, kurtosis(values)
        
if __name__ == '__main__':
     KurtosisMRJob.run()           

