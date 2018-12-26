import pymongo
import xml.sax
from pymongo import MongoClient
## Create 2 table from the data set based on the type of journals; article and inproceedings

## Handler Class for Parsing XML

class Handler ( xml.sax.ContentHandler):
    
    def __init__( self):
        self.count=0
        self.isArticle=False
        self.isInproceedings=False
        self.isTitle=False
        self.isYear=False
        self.isBookTitle=False
        self.isJournal=False
        self.isAuthor=False
        self.buffer_year=""
        self.articles=[]
        self.inproceedings=[]
        self.cur=["","","","",[]]
        self.authors=[]
        self.buffer=""
        self.currentdata =""
      
    def startElement( self, name, attrs):
        self.currentdata =name
        if name=="article":
            self.cur=["","","","",[]]
            self.isArticle=True
            self.cur[0]=attrs["key"]
            
        elif name=="inproceedings":
            self.cur=["","","","",[]]
            self.isInproceedings=True
            self.cur[0]=attrs["key"]
            
        elif name=="title":
            self.isTitle=True    
        elif name=="journal":
            self.isJournal=True  
        elif name=="year":
            self.isYear=True
            self.buffer_year=""
        elif name=="booktitle":
            self.isBookTitle=True
        elif name=="author":
            self.buffer=""
            self.isAuthor=True
            

            
    def endElement( self, name):
        if name=="article":
            self.isArticle=False
            self.cur[-1]=self.authors
            self.articles.append(self.cur)
            self.authors=[]
            
        elif name=="inproceedings":
            self.isInproceedings=False
            self.cur[-1]=self.authors
            self.inproceedings.append(self.cur)
            self.authors=[]
    
        elif name=="title":
            self.isTitle=False
            
        elif name=="journal":
            self.isJournal=False
            
        elif name=="year":
            if self.buffer_year!="":
                self.cur[3]=self.buffer_year
            self.isYear=False
            
        elif name=="booktitle":
            self.isBookTitle=False
         
        elif name=="author" :
            if self.buffer!="":
                self.authors.append(self.buffer)
            self.isAuthor=False
            
    def characters( self, data):
        if self.isArticle==True:
            if  self.isTitle==True and self.currentdata=="title":
                self.cur[1]=data
        
            if self.isYear==True and self.currentdata=="year":
                self.buffer_year+=data
                
            if self.isJournal==True and self.currentdata=="journal":
                self.cur[2]=data
                
            if self.isAuthor==True and self.currentdata=="author":
                self.buffer+=data  
                
        elif self.isInproceedings==True:
            if  self.isTitle==True and self.currentdata=="title":
                self.cur[1]=data
        
            if self.isYear==True and self.currentdata=="year":
                self.buffer_year+=data
                
            if self.isBookTitle==True and self.currentdata=="booktitle":
                self.cur[2]=data
                
            if self.isAuthor==True and self.currentdata=="author":
                self.buffer+=data


handler = Handler()
parser = xml.sax.make_parser()
parser.setFeature(xml.sax.handler.feature_validation,False)
parser.setContentHandler(handler)
parser.parse("dblp-2018-08-01.xml")

## Connect to the Mongo Client and create the hw3db database

client = MongoClient()
client = MongoClient('localhost', 27017)
db=client.hw3db

## Inproceedings, each inproceeding has pubkey,title,booktitle, year and authors as columns
## Articles, each article has pubkey,title,journal, year and authors as columns
## Authors can be a list of string as it is allowed by MongoDB

inproceedings=db.Inproceeedings
articles=db.Articles
post=["_id","title","booktitle","year","authors"]

for i in handler.inproceedings:
       inproceedings.insert_one(dict(zip(post,i)))
        
post=["_id","title","journal","year","authors"]

for i in handler.articles:
       articles.insert_one(dict(zip(post,i)))
               
   

