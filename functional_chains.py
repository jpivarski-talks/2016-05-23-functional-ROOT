#!/usr/bin/env python

import math
import itertools

import ROOT
import fastavro

tfile = ROOT.TFile("Event.root")
ttree = tfile.Get("T")

_inmemory = None

class Datum(dict):
    def __getattr__(self, item):
        return self[item]
    def __repr__(self):
        out = []
        for k in sorted(self):
            v = self[k]
            if isinstance(v, list) and len(v) > 5:
                out.append(k + ": " + repr(v[:5]) + "...")
            else:
                out.append(k + ": " + repr(v))
        return "{" + ", ".join(out) + "}"

class Data(object):
    @staticmethod
    def wrap(obj):
        if isinstance(obj, dict):
            return Datum({k: Data.wrap(v) for k, v in obj.items()})
        elif isinstance(obj, list):
            return Data([Data.wrap(v) for v in obj])
        elif isinstance(obj, tuple):
            return Data(tuple([Data.wrap(v) for v in obj]))
        else:
            return obj

    @staticmethod
    def unwrap(x):
        if isinstance(x, Data):
            return x.eval()
        elif isinstance(x, dict):
            return {k: Data.unwrap(v) for k, v in x.items()}
        elif isinstance(x, list):
            return [Data.unwrap(v) for v in x]
        elif isinstance(x, tuple):
            return tuple([Data.unwrap(v) for v in x])
        else:
            return x

    @staticmethod
    def _source(limit):
        f = fastavro.reader(open("Event.avro"))
        for i, x in enumerate(f):
            if i == limit:
                break
            yield Data.wrap(x)

    @staticmethod
    def _source2(limit):
        global _inmemory
        if _inmemory is None:
            f = fastavro.reader(open("Event.avro"))
            _inmemory = []
            for i, x in enumerate(f):
                if i % 100 == 0:
                    print "Loading %d" % i
                _inmemory.append(Data.wrap(x))
        for i, x in enumerate(_inmemory):
            if i == limit:
                break
            yield x

    @staticmethod
    def source(limit=None):
        return Data(Data._source2(limit))

    def __init__(self, generator):
        self.generator = generator

    def __repr__(self):
        evaluated = self.eval(6)
        if len(evaluated) > 5:
            return "[" + ", ".join(repr(x) for x in evaluated[:5]) + "...]"
        else:
            return repr(evaluated)

    def __iter__(self):
        return iter(self.generator)

    def __len__(self):
        return len(self.eval())

    def flatten(self):
        return Data(itertools.chain.from_iterable(self.generator))
        
    def cartesian(self, other):
        return Data(itertools.product(self.generator, other.generator))

    def triangular(self):
        self.generator = list(self.generator)
        def out():
            for i in xrange(len(self.generator)):
                for j in xrange(i, len(self.generator)):
                    yield (self.generator[i], self.generator[j])
        return Data(out())

    ############################### mapping and filtering (lazy)

    def map(self, fcn):
        return Data(fcn(x) for x in self.generator)

    def filter(self, fcn):
        return Data(x for x in self.generator if fcn(x))

    def flatMap(self, fcn):
        return Data(itertools.chain.from_iterable(fcn(x) for x in self.generator))

    def filterMap(self, fcn):
        def fcn2(x):
            y = fcn(x)
            if y is None:
                return Data([])
            else:
                return Data([y])
        return self.flatMap(fcn2)
        
    def __getitem__(self, item):
        evaluated = self.eval()
        if isinstance(item, tuple):
            def slicer(obj, indexes):
                first, rest = indexes[0], indexes[1:]
                if isinstance(first, slice) and len(rest) > 0:
                    start, stop, stride = first.indices(len(obj))
                    return [slicer(obj[i], rest) for i in xrange(start, stop, stride)]
                elif len(rest) > 0:
                    return slicer(obj[first], rest)
                else:
                    return obj[first]
            return Data.wrap(slicer(evaluated, item))
        else:
            return Data.wrap(evaluated[item])

    def get(self, item):
        return self.__getitem__(item)

    def getOrElse(self, item, default):
        try:
            return self.__getitem__(item)
        except IndexError:
            return default

    def flatMapGet(self, item):
        def fcn2(x):
            try:
                return Data([x.__getitem__(item)])
            except IndexError:
                return Data([])
        return self.flatMap(fcn2)

    @staticmethod
    def zip(*datas):
        return Data(itertools.izip(*datas))

    def skip(self, number):
        if hasattr(self.generator, "next"):
            mine, theirs = itertools.tee(self.generator)
            self.generator = mine
            for i in xrange(number):
                theirs.next()
            return Data(theirs)
        else:
            return Data(self.generator[number:])

    ############################### reduction (which evaluates)

    def reduce(self, fcn):          # for parallel stuff, needs a version with seqOp and combOp
        first = True
        for x in self.generator:
            if first:
                out = x
                first = False
            else:
                out = fcn(out, x)
        return out

    def fold(self, fcn, zero):      # for parallel stuff, needs a version with seqOp and combOp
        out = zero
        for x in self.generator:
            out = fcn(out, x)
        return out

    def scan(self, fcn, zero):      # for parallel stuff, needs a version with seqOp and combOp
        out = [zero]
        for x in self.generator:
            out.append(fcn(out, x))
        return out

    def sum(self):
        return self.reduce(lambda x, y: x + y)

    def min(self):
        return self.reduce(lambda x, y: x if x < y else y)

    def max(self):
        return self.reduce(lambda x, y: x if x > y else y)

    def distinct(self):   # could be implemented as a fold, but mutable set makes for a faster implementation
        memo = set()
        out = []
        for x in self.generator:
            if x not in memo:
                memo.add(x)
                out.append(x)
        return out

    ############################### key-value reduction

    def groupByKey(self):
        out = {}
        for key, value in self.generator:
            if key not in out:
                out[key] = []
            out[key].append(value)
        return out

    def reduceByKey(self, fcn):
        out = {}
        for key, value in self.generator:
            if key not in out:
                out[key] = value
            else:
                out[key] = fcn(out[key], value)
        return out

    def foldByKey(self, fcn, zero):
        out = {}
        for key, value in self.generator:
            if key not in out:
                out[key] = zero
            out[key] = fcn(out[key], value)
        return out

    ############################### evaluation

    def eval(self, limit=None):
        out = []
        for i, x in enumerate(self.generator):
            if i == limit:
                break
            out.append(Data.unwrap(x))
        return out

    def verify(self, name, limit=None):
        h = ROOT.gDirectory.FindObject(name)
        numBins = h.GetNbinsX()
        low = h.GetBinLowEdge(1)
        high = h.GetBinLowEdge(numBins) + h.GetBinWidth(numBins)
        values = [0] * numBins
        tmphist = ROOT.TH1F(name + "_verify", "", numBins, low, high)
        for x in self.eval(limit):
            tmphist.Fill(x)
        diff = 0.0
        for binIndex in xrange(0, numBins + 1):
            diff += abs(h.GetBinContent(binIndex) - tmphist.GetBinContent(binIndex))
        if diff > 1e-5:
            for binIndex in xrange(0, numBins + 1):
                if binIndex == 0:
                    printIndex = "underflow"
                elif binIndex == numBins:
                    printIndex = "overflow"
                else:
                    printIndex = "[%g, %g)" % (h.GetBinLowEdge(binIndex), h.GetBinLowEdge(binIndex) + h.GetBinWidth(binIndex))
                print "%-20s %20g %20g %20g" % (printIndex, h.GetBinContent(binIndex), tmphist.GetBinContent(binIndex), h.GetBinContent(binIndex) - tmphist.GetBinContent(binIndex))
            return False
        else:
            return True

def printRoot(name):
    h = ROOT.gDirectory.FindObject(name)
    numBins = h.GetNbinsX()
    low = h.GetBinLowEdge(1)
    high = h.GetBinLowEdge(numBins) + h.GetBinWidth(numBins)
    for binIndex in xrange(0, numBins + 1):
        if binIndex == 0:
            printIndex = "underflow"
        elif binIndex == numBins:
            printIndex = "overflow"
        else:
            printIndex = "[%g, %g)" % (h.GetBinLowEdge(binIndex), h.GetBinLowEdge(binIndex) + h.GetBinWidth(binIndex))
        print "%-20s %20g" % (printIndex, h.GetBinContent(binIndex))
    
### inspired by dt_DrawTest.C, dt_MakeRef.C, stress.cxx

ttree.Draw("fNtrack >> hNtrack"); # printRoot("hNtrack")
assert Data.source().map(lambda _: _.fNtrack).verify("hNtrack")

ttree.Draw("fNtrack >> hNtrack2", "fFlag == 1"); # printRoot("hNtrack2")
assert Data.source().filter(lambda _: _.fFlag == 1).map(lambda _: _.fNtrack).verify("hNtrack2")

ttree.Draw("fEvtHdr.fEvtNum + fTemperature * 6 >> hWeird2"); # printRoot("hWeird2")
assert Data.source().map(lambda _: _.fEvtHdr.fEvtNum + _.fTemperature * 6).verify("hWeird2")

ttree.Draw("fClosestDistance >> hFlatClosestDistance"); # printRoot("hFlatClosestDistance")
assert Data.source().map(lambda _: _.fClosestDistance).flatten().verify("hFlatClosestDistance")
assert Data.source().flatMap(lambda _: _.fClosestDistance).verify("hFlatClosestDistance")

ttree.Draw("fTemperature - 20 * Alt$(fClosestDistance[9], 0) >> hClosestDistanceAlt"); # printRoot("hClosestDistanceAlt")
assert Data.source().filterMap(lambda _: _.fTemperature - 20 * _.fClosestDistance.getOrElse(9, 0.0)).verify("hClosestDistanceAlt")

ttree.Draw("fClosestDistance[2] >> hClosestDistance2"); # printRoot("hClosestDistance2")
assert Data.source().filterMap(lambda _: _.fClosestDistance.getOrElse(2, None)).verify("hClosestDistance2")
assert Data.source().map(lambda _: _.fClosestDistance).flatMapGet(2).verify("hClosestDistance2")

ttree.Draw("fNtrack >> hNtrackCut", "fEvtHdr.fEvtNum % 10 == 0"); # printRoot("hNtrackCut")
assert Data.source().filter(lambda _: _.fEvtHdr.fEvtNum % 10 == 0).map(lambda _: _.fNtrack).verify("hNtrackCut")

ttree.Draw("fTracks.fPx >> hPx", "", "", 100); # printRoot("hPx")
assert Data.source(100).flatMap(lambda event: event.fTracks).map(lambda track: track.fPx).verify("hPx")
assert Data.source(100).flatMap(lambda event: event.fTracks.map(lambda track: track.fPx)).verify("hPx")

ttree.Draw("fNpoint >> hNpointPx", "fPx < 0", "", 100); # printRoot("hNpointPx")
assert Data.source(100).flatMap(lambda _: _.fTracks).filter(lambda _: _.fPx < 0).map(lambda _: _.fNpoint).verify("hNpointPx")

ttree.Draw("fMatrix >> hFullMatrix"); # printRoot("hFullMatrix")
assert Data.source().map(lambda _: _.fMatrix).flatten().flatten().verify("hFullMatrix")
assert Data.source().map(lambda _: _.fMatrix[:][:]).flatten().flatten().verify("hFullMatrix")
assert Data.source().map(lambda _: _.fMatrix[:,:]).flatten().flatten().verify("hFullMatrix")

ttree.Draw("fMatrix[][0] >> hMatrix0"); # printRoot("hMatrix0")
assert Data.source().map(lambda _: _.fMatrix[:,0]).flatten().verify("hMatrix0")

ttree.Draw("fMatrix[1][] >> hMatrix1"); # printRoot("hMatrix1")
assert Data.source().map(lambda _: _.fMatrix[1,:]).flatten().verify("hMatrix1")

ttree.Draw("fMatrix[2][2] >> hMatrix2"); # printRoot("hMatrix2")
assert Data.source().map(lambda _: _.fMatrix[2,2]).verify("hMatrix2")

ttree.Draw("fTracks.fVertex[0] >> hTrackVertex0", "", "", 100); # printRoot("hTrackVertex0")
# you might think it's this:
# assert not Data.source(100).flatMap(lambda event: event.fTracks.map(lambda track: track.fVertex[0])).verify("hTrackVertex0")
# but no, it's actually this:
assert Data.source(100).flatMap(lambda event: event.fTracks[0].fVertex).verify("hTrackVertex0")

ttree.Draw("fTracks[0].fVertex >> hVertexTrack0", "", "", 100); # printRoot("hVertexTrack0")
# now you might think it's reversed:
# assert not Data.source(100).flatMap(lambda event: event.fTracks.map(lambda track: track.fVertex[0])).verify("hVertexTrack0")
# but no, it's the same thing as before:
assert Data.source(100).flatMap(lambda event: event.fTracks[0].fVertex).verify("hVertexTrack0")

ttree.Draw("Sum$(fTracks.fPx) >> hSumPx", "", "", 100); printRoot("hSumPx")
assert Data.source(100).map(lambda event: event.fTracks.map(lambda track: track.fPx).sum()).verify("hSumPx")

ttree.Draw("Sum$(fTracks.fVertex) >> hSumVertex", "", "", 100); # printRoot("hSumVertex")
assert Data.source(100).map(lambda event: event.fTracks.flatMap(lambda track: track.fVertex).sum()).verify("hSumVertex")

ttree.Draw("Sum$(fTracks.fVertex[0]) >> hSumVertex0", "", "", 100); # printRoot("hSumVertex0")
# you might think it's this:
# assert not Data.source(100).map(lambda event: event.fTracks.map(lambda track: track.fVertex[0]).sum()).verify("hSumVertex0")
# but no, it's actually this:
assert Data.source(100).map(lambda event: event.fTracks[0].fVertex.sum()).verify("hSumVertex0")

ttree.Draw("Sum$(fTracks[0].fVertex) >> hSumTrack0", "", "", 100); # printRoot("hSumTrack0")
# now you might think it's reversed:
# assert not Data.source(100).map(lambda event: event.fTracks.map(lambda track: track.fVertex[0]).sum()).verify("hSumTrack0")
# but no, it's the same thing as before:
assert Data.source(100).map(lambda event: event.fTracks[0].fVertex.sum()).verify("hSumTrack0")

### more general stuff

# make new data structures in map for later processing or histogram weighting
print Data.source(10).map(lambda _: (_.fNtrack, _.fTemperature)).eval()
# [(603, 20.756078720092773), (602, 20.01813507080078), (596, 20.674409866333008), (602, 20.815942764282227), (592, 20.677644729614258), (596, 20.60653305053711), (603, 20.524988174438477), (589, 20.798776626586914), (600, 20.816205978393555), (600, 20.80414390563965)]

# set-like functions
print Data.source().map(lambda _: _.fType).distinct()
# [u'type0', u'type1', u'type2', u'type3', u'type4']

# mix data from different events (intentionally, with "skip" and "zip", not through an indexing error)
print Data.zip(Data.source(10).map(lambda _: _.fEvtHdr.fEvtNum),
               Data.source(10).skip(1).map(lambda _: _.fEvtHdr.fEvtNum)).eval()
# [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8), (8, 9)]

print Data.source(1).map(lambda event: Data.zip(event.fTracks,
                                                event.fTracks.skip(1)).map(lambda (one, two): one.fPx - two.fPx)).eval()

print Data.source(1).map(lambda event: event.fTracks.cartesian(event.fClosestDistance)
                                            .map(lambda (one, two): one.fPx - two)).eval()

print Data.source(1).map(lambda event: event.fTracks.cartesian(event.fTracks)
                                            .map(lambda (one, two): one.fPx - two.fPx)).eval()

print Data.source(1).map(lambda event: event.fTracks.triangular()
                                            .map(lambda (one, two): one.fPx - two.fPx)).eval()

# classic map-reduce
# ... over events
print Data.source(30).map(lambda _: (_.fType, _.fTemperature)).groupByKey()
# {"type4": [20.677644729614258, 20.80414390563965, 20.701528549194336, 20.163637161254883, 20.928194046020508, 20.15508460998535], "type1": [20.01813507080078, 20.524988174438477, 20.92605972290039, 20.882701873779297, 20.41617774963379, 20.33348274230957], "type0": [20.756078720092773, 20.60653305053711, 20.34678840637207, 20.119722366333008, 20.58045196533203, 20.49521827697754], "type3": [20.815942764282227, 20.816205978393555, 20.36102867126465, 20.80663299560547, 20.243196487426758, 20.090187072753906], "type2": [20.674409866333008, 20.798776626586914, 20.943748474121094, 20.81961441040039, 20.769359588623047, 20.79751968383789]}

print Data.source(30).map(lambda event: (event.fType, event.fTracks.map(lambda track: track.fPx).sum())).groupByKey()
# {"type4": [21.409576324222144, 12.520592283748556, -20.86220916104503, 13.177010939019965, 6.412915674445685, -17.333399845578242], "type1": [3.8320341074140742, 24.39618847006932, 15.366899827262387, -2.0011899042874575, 4.224826748948544, 21.05482216272503], "type0": [-12.20141019928269, 25.45827755134087, -21.24391257809475, 1.190686118323356, -19.924689217812556, 36.124268828425556], "type3": [6.2540165627142414, -34.01102355460171, 50.03850438515656, -36.48714441427728, -6.209268557024188, 0.8597903683548793], "type2": [-20.080139497993514, -11.047289501060732, -2.312483975896612, -24.03662702080328, 12.323108680779114, 19.071777526522055]}

print Data.source().map(lambda _: (_.fType, _.fTemperature)).reduceByKey(lambda x, y: x + y)
# {"type4": 4102.499099731445, "type1": 4104.85612487793, "type0": 4103.642580032349, "type3": 4104.000316619873, "type2": 4098.830762863159}

# ... over tracks
print Data.source().flatMap(lambda event: event.fTracks.map(lambda track: (event.fType, track.fPx))).reduceByKey(lambda x, y: x + y)
# {"type4": 158.86978333314983, "type1": -561.5155096719645, "type0": -186.1472605756262, "type3": -49.75646570308527, "type2": -92.16877998295968}


