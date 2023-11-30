class Node:

    def __init__(self, ntype, lc, rc):
        # the following variables are populated in RAParser.py
        self.node_type = ntype		# "relation", "select", "project", "times",...
        self.left_child = lc		# left child
        self.right_child = rc		# right child
        self.columns = None	        # list of column names for RENAME and PROJECT
        # list of conditions for SELECT [(lop,lot,c,rop,rot)..]
        self.conditions = None

        # the following variables are populated in RA.py
        # relation name at node (tempN interior, regular leaf)
        self.relation_name = None
        self.attributes = None		# holds schema attributes at node
        self.domains = None		# holds schema domains of attributes at node

    def get_attributes(self):
        return self.attributes

    def get_columns(self):
        return self.columns

    def get_conditions(self):
        return self.conditions

    def get_right_child(self):
        return self.right_child

    def get_left_child(self):
        return self.left_child

    def get_domains(self):
        return self.domains

    def get_node_type(self):
        return str(self.node_type)

    def get_relation_name(self):
        return str(self.relation_name)

    def set_attributes(self, attributes):
        self.attributes = attributes

    def set_conditions(self, conditions):
        self.conditions = conditions

    def set_right_child(self, right_node):
        self.right_child = right_node

    def set_left_child(self, left_node):
        self.left_child = left_node

    def set_columns(self, cols):
        self.columns = cols

    def set_domains(self, doms):
        self.domains = doms

    def set_node_type(self, n_type):
        self.node_type = n_type

    def set_relation_name(self, r_name):
        self.relation_name = r_name

    def print_tree(self, n):
        if self.node_type == "relation":
            print(" "*n, end="")
            print("NODE TYPE: " + self.node_type + "  ")
            print(" "*n, end="")
            print("Relation Name is : " + self.relation_name)
            if self.attributes != None:
                print(" "*n, end="")
                print("Schema is : " + str(self.attributes))
            if self.domains != None:
                print(" "*n, end="")
                print("Datatypes is : " + str(self.domains)+"\n")
        elif self.node_type == "project" or self.node_type == "rename":
            print(" "*n, end="")
            print("NODE TYPE: " + self.node_type + "  ")
            print(" "*n, end="")
            print("Atributes are : "+str(self.columns))
            if self.relation_name != None:
                print(" "*n, end="")
                print("Relation Name is : " + self.relation_name)
            if self.attributes != None:
                print(" "*n, end="")
                print("Schema is : " + str(self.attributes))
            if self.domains != None:
                print(" "*n, end="")
                print("Datatypes is : " + str(self.domains)+"\n")
            self.left_child.print_tree(n+4)
        elif self.node_type == "select":
            print(" "*n, end="")
            print("NODE TYPE: " + self.node_type + "  ")
            for cond in self.conditions:
                print(" "*n, end="")
                print(cond[0], end="")
                print(":", end="")
                print(cond[1], end="")
                print(":", end="")
                print(cond[2], end="")
                print(":", end="")
                print(cond[3], end="")
                print(":", end="")
                print(cond[4])
            if self.relation_name != None:
                print(" "*n, end="")
                print("Relation Name is : " + self.relation_name)
            if self.attributes != None:
                print(" "*n, end="")
                print("Schema is : " + str(self.attributes))
            if self.domains != None:
                print(" "*n, end="")
                print("Datatypes is : " + str(self.domains)+"\n")
            self.left_child.print_tree(n+4)
        elif self.node_type in ["union", "minus", "join", "intersect", "times"]:
            print(" "*n, end="")
            print("NODE TYPE: "+self.node_type+"  ")
            if self.relation_name != None:
                print(" "*n, end="")
                print("Relation Name is : " + self.relation_name)
            if self.attributes != None:
                print(" "*n, end="")
                print("Schema is : " + str(self.attributes))
            if self.domains != None:
                print(" "*n, end="")
                print("Datatypes is : " + str(self.domains)+"\n")
            self.left_child.print_tree(n+4)
            self.right_child.print_tree(n+4)
        else:
            pass
