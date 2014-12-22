error-correction-in-large-volume-ocr-datasets
=============================================

Social Science-Government Group: Error Correction in Large Volume OCR Datasets

By its nature as an unsupervised utility system, this user interface is intentionally minimal.  The user merely provides the name of a csv file located in its own otherwise empty directory and the system will perform its work without user intervention.

The system does provide output during each step of the process.  It is possible for the user to invoke each step independently in the case of failure or experimentation.  Further, the system itself tracks its own process, so the user may always invoke the utility without needing to know what state the system is in.

The system is started with the Java class

E6893.ocr.CorrectErrors dbPath

Optional, position-sensitive arguments may follow

[maxMinutes maxIterations pValue]  

maxMinutes
The maximum number of minutes the system should wait for an index to come online.  The default is 60 minutes.  

maxIterations
The maximum of Steps 9-11 iterations the system should perform.  This is provided as an optional boundary and could always be a very high value since the iterations will always stop when no more tokens can be declared as true.  

pValue
The value used in Step 11 for the binomial probability test.  The default is 0.05.

The system depends on the community edition of Neo4j and the Apache Commons Math library.

