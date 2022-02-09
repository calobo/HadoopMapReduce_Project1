# Are you My Friend Analytics
In  this  project, we set  up  the  ecosystem.  Then  we created  datasets and uploaded them into Hadoop HDFS. Next, we analyzed the data in a scalable fashion by writing custom analytics tasks  using map-reduce Java code and ran those on Hadoop system.

How to Run:
1. Run dataCreator.java to generate the data for the map-reduce functions.
2. Run the Test file for the selected task.
3. Read output in "part-r-00000" in the output folder.

Task Decriptions:

4.a) Task a 
Write a job(s) that reports all Facebook users (name, and hobby) 
whose Nationality is the same as your own Nationality (pick one). 
Note that nationalities in the data file are a random sequence of 
characters unless you work with meaningful strings like “Chinese” 
or “German”. This is up to you.

4.b) Task b 
Find the top 8 interesting Facebook pages, namely, those that got 
the most accesses based on your AccessLog dataset compared to all 
other pages, and return Id, Name and Nationality.

4.c) Task c 
Write job(s) that reports for each country, how many of its citizens 
have a Facebook page

4.d) Task d 
For  each  Facebook  page,  compute  the  “happiness  factor”  of  its 
owner. That is, for each Facebook page in your dataset, report the 
owner’s name, and the number of people listing him or her as friend. 
For page owners that aren't listed as anybody's friend,  return a 
score of zero. 

4.e) Task e 
Determine which people have favorites. That is, for each Facebook 
page owner, determine how many total accesses to Facebook pages they 
have  made  (as  reported  in  the  AccessLog)  and  how  many  distinct 
Facebook pages they have accessed in total.

4.f) Task f 
Identify people that have declared someone as their friend yet who 
have  never  accessed  their  respective  friend’s  Facebook  page  – 
indicating that they don’t care enough to find out any news about 
their friend (at least not via Facebook).

4.g) Task g
Find the list of all people that have set up a Facebook page, but 
have lost interest, i.e., after some initial time unit (say 14 days) 
have  never  accessed  Facebook  again  (meaning  no  entries  in  the 
Facebook AccessLog exist after that date).

4.h) Task h
Report all owners of a Facebook who are famous and happy, namely, 
those who have more friends than the average number of friends across 
all owners in the data files.
