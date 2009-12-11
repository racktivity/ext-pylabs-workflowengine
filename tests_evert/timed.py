from timeit import Timer

class testfunction:
    def __init__(self,m):
        self.m = m
    def __call__(self):
        pass

timer = Timer(testfunction('a short message'))
results = timer.repeat(repeat=3,number=100)
print results
