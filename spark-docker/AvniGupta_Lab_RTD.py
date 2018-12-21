
#python3
#pyspark

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 10 second
sc = SparkContext("local[2]", "NetworkAvni")
ssc = StreamingContext(sc, 10)



# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 4100)




# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))




def get_quadrant(line):
    # Convert the input string into a pair of numbers
    try:
        (x, y) = [float(x) for x in line.split()]
    except:
        print ("Invalid input")
        return ('Invalid points', 1)

    # Map the pair of numbers to the right quadrant
    if x > 0 and y > 0:
        quadrant = 'First quadrant'
    elif x < 0 and y > 0:
        quadrant = 'Second quadrant'
    elif x < 0 and y < 0:
        quadrant = 'Third quadrant'
    elif x > 0 and y < 0:
        quadrant = 'Fourth quadrant'
    elif x == 0 and y != 0:
        quadrant = 'Lies on Y axis'
    elif x != 0 and y == 0:
        quadrant = 'Lies on X axis'
    else:
        quadrant = 'Origin'

    # The pair represents the quadrant and the counter increment
    return (quadrant, 1)




if __name__ == "__main__":

     spc = SparkContext(appName="Quads")

    # Create a StreamingContext with a batch interval of 10 seconds
    ssc = StreamingContext(sc, 10)

#     Checkpointing feature
    ssc.checkpoint("checkpoint")

    # Creating a DStream to connect to hostname:port (like localhost:9999)
#     lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    lines = ssc.socketTextStream("127.0.0.1", 4100)
    
    # Function that's used to update the state
    updateFunction = lambda new_values, running_count: sum(new_values) + (running_count or 0)

    # Update all the current counts of number of points in each quadrant
    running_counts = lines.map(get_quadrant).updateStateByKey(updateFunction)

    # Print the current state
    running_counts.pprint()



ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

