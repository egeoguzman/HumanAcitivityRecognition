#!/usr/bin/env python3
# PearsonCorrelation.py

import sys
from CovvarianceMatrixMRJob import CovvarianceMatrixMRJob

if __name__ == '__main__':
    covvariance = {}
    args = sys.argv[1:]

    job = CovvarianceMatrixMRJob(args)
    with job.make_runner() as runner:
        runner.run()
        dims = 0

        # The mapreduce job returns correlation for each pair of
        # variables in a separate output row.

        for key, value in job.parse_output(runner.cat_output()):

            key = [int(i) for i in key.split("-")]

            # We add these to a dictionary with the indices of the
            # variable as the key and the correlation as the value.
            covvariance[tuple(key)] = value

            # We also keep track of how many columns are in the data
            if int(key[0]) > dims:
                dims = int(key[0])

        # add 1 because column index starts at zero and we want a count!
        dims += 2

        # create the matrix as a list of lists
        covv_matrix = [[None] * dims for i in range(dims)]

        covv_matrix[0][0] = ""

        # open the file containing variable names

        fh = open(job.options.variables)
        variable_names = fh.readlines()
        #insert the variable names into the first row and column of the matrix
        var_num = 1
        for column in variable_names:
            covv_matrix[var_num][0] = column.strip()
            covv_matrix[0][var_num] = column.strip()
            var_num += 1
        # insert the correlation values from the dictionary into the matrix
        for key, value in correlations.items():
            covv_matrix[key[0] + 1][key[1] + 1] = str(round(value, 2)).rjust(6, " ")

        # pretty print the matrix
        print('\n'.join(['\t'.join([str(cell) for cell in row]) for row in covv_matrix]))

