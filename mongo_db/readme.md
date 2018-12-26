This folder contains MongoDB and related Spark applications on the DBPL data set. DBPL data set provides open bibliographic information
of major Computer Science journals and proceedings. More information about the data set can be found at :
* https://dblp.uni-trier.de/
* https://dblp.uni-trier.de/xml/docu/dblpxml.pdf

There are 5 different code files in the folder, including parsing of the xml file and various spark and scala tasks on the mongo database.

1. xml_parser.py : This is the parser code that loads the dblp data set in xml format and import two tables named articles and inproceedings in the MongoDB
2. Creating another column named area into the inproceedings table. Area is based on the field of the papers published in journals.
3. Finding top k-number authors with the highest number of published papers in the selected field.
4. Finding authors who wrote more papers in journal papers.
5. Finding the average number of collobrators within each decade and area.
