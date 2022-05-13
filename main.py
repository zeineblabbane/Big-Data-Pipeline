from producer import produce
from spark_steaming import spark_stream
import os

os.system('hadoop jar tp-big-data-project-0.0.1-SNAPSHOT.jar tn.insat.FootBallScore inputfootball outputfootball')

produce()
spark_stream()