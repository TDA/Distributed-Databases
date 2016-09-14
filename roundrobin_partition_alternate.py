__author__ = 'saipc'

def round_robin_partition(l, n):
    # takes a list l and partitions it into n partitions
    if (l == None or len(l) == 0):
        l = [str(x) for x in xrange(100, 1001)]
    length = len(l)
    print "Length", length
    for i in xrange(0, n):
        s = l[i::n]
        with open('rr_t'+str(i), 'w') as f:
            f.write('\n'.join([y[0] + ',' + y[1] + ',' + y[2] for y in s]))
    pass

if __name__ == '__main__':
    round_robin_partition([], 3)