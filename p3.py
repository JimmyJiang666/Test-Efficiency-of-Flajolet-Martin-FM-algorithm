import mmh3
import string
import statistics
import numpy as np

def wordStream(fileName):
	with open(fileName, "r") as infile:
		for line in infile:
			for w in line.strip().lower().split():
				yield w

def countDistinct(stream):
	M={}
	for x in stream: M[x]=1
	return len(M.keys())

def FM(stream, r): #r is the number of estimates needed
	salt = np.random.randint(1<<30, size=r)
	z=[0]*r # z[i] counts the max no. trailing zeros for ith hash fn.

	for x in stream:
		for i in range(r):
			y=mmh3.hash128( str(x) + str(salt[i]))
			itob=bin(y)[2:]#convert integer to binary in string
			zeros=len(itob)-len(itob.rstrip('0'))#compute the trailing zeros
			z[i]=max(z[i],zeros)
	return z

## TASK 1
test=[3,6,9,12]
for r in test:
	print()
	z=FM(wordStream("big.txt"),r)
	print(z)
	zpower=list(map(lambda x : pow(2,x), z))
	print(zpower)
	print("r = ", r)
	print("the mean estimate is:", statistics.mean(zpower))
	print("the median estimate is:", statistics.median(zpower))
	print("the harmonic mean estimate is:", statistics.harmonic_mean(zpower))
	print()

## TASK 2
def HLL(mystream,k):#in this case k = 6
	r = pow(2, k)
	z = [0] * r
	alpha = 1.418#alpha is the correction index
	for x in mystream:
		h=mmh3.hash128(str(x))
		last_bits = h & (r-1) # last bits used as group flag
		remaining_bits = h >> k
		itob=bin(remaining_bits)[2:]#convert integer to binary in string
		zeros=len(itob)-len(itob.rstrip('0'))#compute the trailing zeros
		z[last_bits]=max(z[last_bits],zeros)#update the group corresponding to the element's zeros
	zpower = list(map(lambda x : pow(2,x), z))
	# print(zpower)
	# print("here is the sum of zpower", sum(zpower))
	#observe that the returned value of HLL is \alpha*r*harmonic_mean of zpower
	return alpha*r*statistics.harmonic_mean(zpower)

# #task 2.1
hll=HLL(wordStream("big.txt"),6)
print("estimated #distinct elements for wordStream: ", hll)
realHll=countDistinct(wordStream("big.txt"))
print("real #distinct elements for wordStream: ", realHll)
print("relative error is:", abs(hll-realHll)/realHll)

# #task 2.2
def shingleStream(fileName,k):#generate k-shingles stream
	with open(fileName, "r") as infile:
		for line in infile:
			for index in range(len(line)-k+1):
				yield line[index:index+k]

hll=HLL(shingleStream("big.txt",9),6)
print("estimated #distinct elements for shingleStream: ", hll)
realHll=countDistinct(shingleStream("big.txt",9))
print("real #distinct elements for wordStream: ", realHll)
print("relative error is:", abs(hll-realHll)/realHll)

#task 2.3
def numStream():
	t = 1 << 30
	print("t = ", t)
	for i in range(t):
		if i%10000000==0:	print(i)
		yield int(t*np.random.power(3.5))

hll = HLL(numStream(),6)#this is going to take around 40 mins to compute
print("estimated #distinct elements for numStream : ", hll)
realHll = 499790312 #this is an almost accurate estimate by the nature of power distribution
print("real #distinct elements for wordStream: ", realHll)
print("relative error is:", abs(hll-realHll)/realHll)
