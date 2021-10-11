from configuration.config import Configuration
from models.entities import Entities
from models.columns import Columns
from models.relationships import Relationship
from models.synonyms import Synonyms

from spacy.lemmatizer import Lemmatizer
from spacy.lookups import Lookups


import logging
import pymysql

# import pymssql
#rds settings
rds_host  = "database.cxmsifyr3lmy.us-east-1.rds.amazonaws.com"
db_username = "admin"
db_password = "gc9LUCHguIN1ZQdqCYrR"
db_name = "sys"
name = db_username
password = db_password
db_name = db_name

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    print("ERROR: Unexpected error: Could not connect to MySQL instance.")
    print(e)
    sys.exit()

print("SUCCESS: Connection to RDS MySQL instance succeeded")

# connection_string = Configuration().get_sql_connection_string()
# connection_url = URL.create("mssql", query={"odbc_connect": connection_string})
# engine = create_engine(connection_url)


class DBModel(object):
    def __init__(self):
        self.entities = []
        self.columns = []
        self.relationships = []
        self.synonyms_col = []
        self.synonyms_tab = []
        self.entity_graph = []
        self.loaded_entities = []
        self.config = Configuration()
        self.conn = conn
        # self.conn = engine.raw_connection()
        lookups = Lookups()
        self.lemmatizer = Lemmatizer(lookups)
        self.load_db_model()

    def load_db_model(self):
        # loading the database from sql server
        cursor = self.conn.cursor()
        cursor.execute(self.config.get_tables_sql_query())
        row = cursor.fetchone()
        while row:
            self.entities.append(Entities(row[1], self.config.get_default_column(row[1])))
            row = cursor.fetchone()

        cursor.execute(self.config.get_columns_sql_query())
        current_entity = None
        current_entity_name = ""
        row = cursor.fetchone()
        while row:
            if current_entity_name != row[1]:
                current_entity_name = row[1]
                current_entity = next(en for en in self.entities if en.name == current_entity_name)

            col_type = row[5]
            if col_type == "varchar" or col_type == "nvarchar":
                col_type = "string"
            current_entity.columns.append(Columns(row[2], col_type))
            row = cursor.fetchone()

        current_entity = None
        current_entity_name = ""
        cursor.execute(self.config.get_FK_sql_query())
        row = cursor.fetchone()
        while row:
            self.relationships.append(Relationship(row[0], row[3], row[1], row[4]))
            if len([en for en in self.entity_graph if en[0] == row[0]]) > 0:
                current_entity = next(en for en in self.entity_graph if en[0] == row[0])
                current_entity[1].append(row[3])
            else:
                self.entity_graph.append((row[0], [row[3]]))

            if len([en for en in self.entity_graph if en[0] == row[3]]) > 0:
                current_entity = next(en for en in self.entity_graph if en[0] == row[3])
                current_entity[1].append(row[0])
            else:
                self.entity_graph.append((row[3], [row[0]]))
            row = cursor.fetchone()

        current_entity = None
        current_entity_name = ""
        cursor.execute(self.config.get_PK_sql_query())
        row = cursor.fetchone()
        while row:
            if len([en for en in self.entity_graph if en[0] == row[0]]) == 1:
                current_entity = next(en for en in self.entities if en.name == row[0])
                current_entity.primaryKey = row[1]
            row = cursor.fetchone()

        for entity_to_load in self.config.get_entitites_to_load():
            entity_load_query = "select distinct " + entity_to_load["column"] + " from " + entity_to_load["entity"]
            cursor.execute(entity_load_query)
            entity_data = (entity_to_load["entity"], [])
            row = cursor.fetchone()
            while row:
                entity_data[1].append(row[0])
                # add lemma strings
                lemmas = self.lemmatizer(str(row[0]), u'NOUN')
                for lemma in lemmas:
                    entity_data[1].append(str(lemma))
                row = cursor.fetchone()
            self.loaded_entities.append(entity_data)

        # load synonyms from declarative file
        # table synonyms
        for table_synonym in self.config.get_synonyms()["table"]:
            orginal_val = table_synonym["original"]
            synonyms_vals = table_synonym["synonyms"]
            for synonyms_val in synonyms_vals:
                self.synonyms_tab.append(Synonyms(orginal_val, synonyms_val))

        # column synonyms
        for column_synonym in self.config.get_synonyms()["column"]:
            orginal_val = column_synonym["original"]
            synonyms_vals = column_synonym["synonyms"]
            for synonyms_val in synonyms_vals:
                self.synonyms_col.append(Synonyms(orginal_val, synonyms_val))


        # make a single array
        self.columns = [column for entity in self.entities for column in entity.columns]


    # might have to write a custom matcher TODO
    # build the matcher based upon the original value and domain synonyms defined
    def get_matcher(self, matcher, nlp):
        for entity in self.entities:
            matcher.add(entity.name.upper() + "_TABLE", None, nlp(entity.name.lower()))    
            for column in entity.columns:
                matcher.add(column.name.upper() + "_COLUMN", None, nlp(column.name.lower()))

        # add table synonyms to matcher
        for synonym in self.synonyms_tab:
            for entity in self.entities:
                if synonym.column.lower() == entity.name.lower():
                    matcher.add(entity.name.upper() + "_TABLE", None, nlp(synonym.synonym.lower()))        

        # add column synonyms to matcher
        for synonym in self.synonyms_col:
            for column in self.columns:
                if synonym.column.lower() == column.name.lower():
                    matcher.add(column.name.upper() + "_COLUMN", None, nlp(synonym.synonym.lower()))        
                    

        return matcher

    def get_custom_matcher(self, matcher, nlp):
        for entity in self.entities:
            matcher.add(entity.name.upper() + "_TABLE", nlp(entity.name.lower()))    
            for column in entity.columns:
                matcher.add(column.name.upper() + "_COLUMN", nlp(column.name.lower()))

        # add table synonyms to matcher
        for synonym in self.synonyms_tab:
            for entity in self.entities:
                if synonym.column.lower() == entity.name.lower():
                    matcher.add(entity.name.upper() + "_TABLE", nlp(synonym.synonym.lower()))        

        # add column synonyms to matcher
        for synonym in self.synonyms_col:
            for column in self.columns:
                if synonym.column.lower() == column.name.lower():
                    matcher.add(column.name.upper() + "_COLUMN", nlp(synonym.synonym.lower()))


        return matcher

