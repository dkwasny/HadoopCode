HadoopCode
==========

A small place for small Hadoop code


All of the *net.kwaz.chicago* code is written using Weather Underground's Chicago weather data from [opensciencedatacloud.org](https://www.opensciencedatacloud.org/publicdata/wunderground/).
This dataset was chosen only because it was small enough to reasonably process on my multiple VM single machine grid while not being completely insignificant or fake.

[Here](https://drive.google.com/file/d/0BxpgL9f7eLyfSnNXLUprOC1mcmc/edit?usp=sharing) is a tarball of the input files for the raw parser MR job within this codebase.
I combined all of the files for a particular zip code into a single larger file for the sake of not making HDFS freak out.
