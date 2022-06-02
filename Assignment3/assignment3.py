import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

df = pd.read_csv('output/timings.txt', header = None)
y = df[0]
x = np.linspace(1, len(y), len(y))
plt.plot(x, y)
plt.xlabel('cpus')
plt.ylabel('Time')
plt.title("timing plot")
 
plt.savefig("output/timings.png")
print("plot is generated successfully")