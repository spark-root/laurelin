#!/usr/bin/env python3

import ROOT as r
from array import array

f = r.TFile("all-types.root", "recreate")
t = r.TTree("Events", "tree with all simple types")

def maxInBits(b):
    return ((2 ** (b-1)) - 1)

def umaxInBits(b):
    return ((2 ** b) - 1)

def minInBits(b):
    return -1 * (2 ** (b-1))

arrayLen = 3


n = {}
un = {}
n_array = {}
un_array = {}
n_slice = {}
un_slice = {}
n_fill = {}
un_fill = {}
n_fill_array = {}
un_fill_array = {}
nSlice =  array('b', [0])
t.Branch('nSlice', nSlice, 'nSlice/B')
for (bit, uType, sType, ruType, rType) in ((8, 'B', 'b', 'b', 'B'),
                            (16, 'H', 'h', 's', 'S'), 
                            (32, 'I', 'i', 'i', 'I'),
                            (64, 'L', 'l', 'l', 'L')):
    n[bit] = array(sType, [0])
    un[bit] = array(uType, [0])
    n_array[bit] = array(sType, arrayLen*[0])
    un_array[bit] = array(uType, arrayLen*[0])
    n_slice[bit] = array(sType, arrayLen*[0])
    un_slice[bit] = array(uType, arrayLen*[0])

    t.Branch('ScalarI%s' % bit, n[bit], 'ScalarI%s/%s' % (bit, rType))
    t.Branch('ScalarUI%s' % bit, un[bit], 'ScalarUI%s/%s' % (bit, ruType))
    t.Branch('ArrayI%s' % bit, n_array[bit], 'ArrayI%s[%s]/%s' % (bit, arrayLen, rType))
    t.Branch('ArrayUI%s' % bit, un_array[bit], 'ArrayUI%s[%s]/%s' % (bit, arrayLen, ruType))
    t.Branch('SliceI%s' % bit, n_slice[bit], 'SliceI%s[nSlice]/%s' % (bit, rType))
    t.Branch('SliceUI%s' % bit, un_slice[bit], 'SliceUI%s[nSlice]/%s' % (bit, ruType))

    n_fill[bit] = (0, 1, 2,
              minInBits(bit), minInBits(bit) + 1, minInBits(bit) + 2,
              maxInBits(bit), maxInBits(bit) - 1, maxInBits(bit) - 2)

    un_fill[bit] = (0, 1, 2,
              0, 1, 2,
              umaxInBits(bit), umaxInBits(bit) - 1, umaxInBits(bit) - 2)

for i in range(len(n_fill[8])):
    thisSlice = i % 3
    nSlice[0] = thisSlice
    for bit in (8, 16, 32, 64):
        n[bit][0] = n_fill[bit][i]
        un[bit][0] = un_fill[bit][i]
        for j in range(arrayLen):
            n_array[bit][j] = n_fill[bit][i]
            un_array[bit][j] = un_fill[bit][i]

        for j in range(thisSlice):
            n_slice[bit][j] = n_fill[bit][i]
            un_slice[bit][j] = un_fill[bit][i]

        if bit == 8:
            print("filling array %s" % n_array[8])
    t.Fill()

f.Write()
f.Close()
