import os
from Relation import *
from Tuple import *

class Database:

    def __init__(self):
        self.relations = {}

    # Add relation r to Dictionary if relation does not already exists.
    # return True on successful add; False otherwise
    def addRelation(self,r):
        if r.name not in self.relations:
            self.relations[r.name] = r
            return True
        return False


    # Delete relation with name rname from Dictionary if relation exists. 
    # return True on successful delete; False otherwise
    def deleteRelation(self,rname):
        if rname in self.relations:
            del self.relations[rname]
            return True
        return False

    # Retrieve and return relation with name rname from Dictionary.
    # return None if it does not exist.
    def getRelation(self, rname):
        if rname in self.relations:
            return self.relations[rname]
        else:
            return None

    # Return database schema as a String 
    def displaySchema(self):
        schema = ""
        for relation in self.relations.values():
            schema += relation.displaySchema() + '\n'
        return schema.strip()

    def initializeDatabase(self, dir):
        schema_file_path = "./"+ dir + "/catalog.dat"
        with open(schema_file_path, 'r') as schema_file:
            num_relations = int(schema_file.readline().strip())
            for _ in range(num_relations):
                relation_name = schema_file.readline().strip()
                num_attributes = int(schema_file.readline().strip())
                attributes = []
                domains = []

                for _ in range(num_attributes):
                    attr_name = schema_file.readline().strip()
                    attr_domain = schema_file.readline().strip()
                    attributes.append(attr_name)
                    domains.append(attr_domain)
            
                new_relation = Relation(relation_name, attributes, domains)

                relation_file_path = "./" + dir + "/" + relation_name + ".dat"
                with open(relation_file_path, "r") as relation_file:
                    num_tuples = int(relation_file.readline().strip())
                    for _ in range(num_tuples):
                        new_tuple = Tuple(attributes, domains)
                        for idx in range(len(attributes)):
                            component_value = relation_file.readline().strip()
                            if domains[idx] == "INTEGER":
                                new_tuple.tuple.append(int(component_value))
                            elif domains[idx] == "DECIMAL":
                                new_tuple.tuple.append(float(component_value))
                            else:
                                new_tuple.tuple.append(component_value)
                        new_relation.addTuple(new_tuple)
                self.addRelation(new_relation)

