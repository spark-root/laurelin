From my (@perilousapricot) notes with @jpivarski:

The idea is to get a TTree from a ROOT file into a Spark dataframe as optimally
as possible.

My (@perilousapricot) reason for re-doing existing work:

The existing Root4J interface is targeted towards re-implementing the C++ ROOT
API in Java. So, you point it to a ROOT file, and get accessors that let you 
access ROOT objects from a TFile like they were regular Java objects. So, if you were
suitably encouraged, you could modify and use these objects to do an analysis.

Java, however, is a completely different language than C++ (even though ROOT If
you want to get to there, you have to combine the I/O, deserialization, object
instatiation layers.  Once that's done, you have to then add member functions
to these instances you get.


