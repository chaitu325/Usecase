<h1><b> USE CASE </b></h1>


It is about displaying the frequency and the duration of particular USER based on category for different time periods.


<b><h4>
File Contents:
</h4></b>
<ul>
<li> <b> main.py </b> </li> <p> It contains the ain function to call all the functions and to initialized from different files</p>
<li> <b> util.py </b> </li> <p> It will initialize Spark Session </p>
<li> <b> read_files.py </b> </li> <p> It will extract the data from source i.e, local or HDFS</p>
<li> <b> transform.py </b> </li> <p> It contains all the transformation funtions as per the usecase</p>
<li> <b> to_files.py </b> </li> <p> It will load the results into destination folder with specific format</p>
<li> <b> test_usecase.py </b> </li> <p> It will test all the functions using pytest</p>
</ul>
