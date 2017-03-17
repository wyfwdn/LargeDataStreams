from pyspark import SparkConf, SparkContext
from datetime import datetime
import matplotlib.pyplot as plt 
conf = SparkConf("local").setAppName("myApp") 
sc = SparkContext(conf = conf)
line=sc.textFile("data.txt")
intline = line.map(lambda x:int(x))
X=[0 for a in range(100)]
Y=[0 for a in range(100)]
Xp=[0 for a in range(100)]
Yp=[0 for a in range(100)]
i=1
while i<=100:
    ii=float(i)/100
    a1=datetime.now()
    aline = intline.filter(lambda x: x % 2==1)
    b1=datetime.now()
    ms1=0.000001*(b1-a1).microseconds
    s1=(b1-a1).seconds
    timea=s1+ms1
    a2=datetime.now()
    bline = aline.filter(lambda x:x<=i)
    b2=datetime.now()
    ms2=0.000001*(b2-a2).microseconds
    s2=(b2-a2).seconds
    timeb=s2+ms2
    throughput = 1/(timea+timeb*0.5)
    X[i-1]=ii
    Y[i-1]=1
    
    a1p=datetime.now()
    alinep = intline.filter(lambda x: x<=ix)
    b1p=datetime.now()
    ms1p=0.000001*(b1p-a1p).microseconds
    s1p=(b1p-a1p).seconds
    timeap=s1p+ms1p
    a2p=datetime.now()
    blinep = alinep.filter(lambda x: x % 2==1)
    b2p=datetime.now()
    ms2p=0.000001*(b2p-a2p).microseconds
    s2p=(b2p-a2p).seconds
    timebp=s2p+ms2p
    throughputp = 1/(timeap+timebp*ii)
    Xp[i-1]=ii
    Yp[i-1]=throughputp/throughput
    i=i+1
plt.figure()  
plt.plot(X,Y,'b',Xp,Yp,'r')   
plt.ylabel('throughput')
plt.xlabel('selectivity of B')  
plt.show()



  
