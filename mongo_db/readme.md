This folder contains MongoDB and related Spark applications on the DBPL data set. DBPL data set provides open bibliographic information
of major Computer Science journals and proceedings. More information about the data set can be found at :
* https://dblp.uni-trier.de/
* https://dblp.uni-trier.de/xml/docu/dblpxml.pdf

There are 2 different code files in the folder:

1. xml_parser.py : This is the parser code that loads the dblp data set in xml format and import two tables named articles and inproceedings in the MongoDB
2.mongo_spark.scala: This is the scala code for various tastks including:
  * Creating another column based on the areas of the writteng articles
  * Finding top k-number authors with the highest number of published papers in the selected field
  * Finding authors who wrote more papers in journal papers
  * Finding the average number of collobrators withing for each decade and each given field.
