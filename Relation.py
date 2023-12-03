class Relation: 
    
    def __init__(self,name,attributes,domains):
        self.name = name.upper() # name of relation
        self.attributes = [a.upper() for a in attributes] # list of names of attributes
        self.domains = [d.upper() for d in domains] # list of "INTEGER", "DECIMAL", "VARCHAR"
        self.table = [] # list of tuple objects

    # Returns True if attribute with name aname exists in relation schema;
    def attribute_exists(self, aname):
        return aname.upper() in self.attributes

    # Returns attribute type of attribute aname; return None if not present
    def attribute_type(self, aname):
        index = self.attributes.index(aname.upper())
        if index != -1:
            return self.domains[index]
        return None

    # Return relation schema as String
    def displaySchema(self):
        schema_str = f"{self.name}("
        for i in range(len(self.attributes)):
            schema_str += f"{self.attributes[i]}:{self.domains[i]}"
            if i < len(self.attributes) - 1:
                schema_str += ","
        schema_str += ")"
        return schema_str
    
    # Set name of relation to rname
    def setName(self, rname):
        self.name = rname.upper()

    # Add tuple tup to relation; Duplicates are fine.
    def addTuple(self,tup):
        self.table.append(tup)

    # Return String version of relation; See output of run for format.
    def __str__(self):
        num_tuples = len(self.table)
        schema_str = f"{self.name}({', '.join([f'{a}:{d}' for a, d in zip(self.attributes, self.domains)])})"
        result = f"{schema_str}\nNumber of tuples:{num_tuples}\n\n"
        for tup in self.table:
            result += str(tup) + "\n"
        return result
    
    # Remove duplicate tuples from this relation
    def removeDuplicates(self):
        unique_tuples = []
        for tup in self.table:
            if all(not tup.equals(existing_tup) for existing_tup in unique_tuples):
                unique_tuples.append(tup.clone(self.attributes))
        self.table = unique_tuples
    
    def member(self, t):
        for tup in self.table:
            if tup.equals(t):
                return True
        return False

    def union(self, r2): 
        # Clone tuples from self and r2
        self_cloned_tuples = [t.clone(self.attributes) for t in self.table]
        r2_cloned_tuples = [t.clone(self.attributes) for t in r2.table]
        
        # Combine the tuples and remove duplicates
        combined_tuples = self_cloned_tuples + r2_cloned_tuples
        result = Relation("UNION_RESULT", self.attributes, self.domains)
        for t in combined_tuples:
            result.addTuple(t)
        result.removeDuplicates()
        
        return result

    def intersect(self, r2): #USE CLONE FROM TUPLE
        # Clone tuples from self and r2
        self_cloned_tuples = [t.clone(self.attributes) for t in self.table]
        r2_cloned_tuples = [t.clone(self.attributes) for t in r2.table]
        
        # Find common tuples
        common_tuples = []
        for t1 in self_cloned_tuples:
            if any(t1.equals(t2) for t2 in r2_cloned_tuples):
                common_tuples.append(t1)
        
        result = Relation("INTERSECT_RESULT", self.attributes, self.domains)
        for t in common_tuples:
            result.addTuple(t)
        
        return result

    def minus(self, r2): #USE CLONE FROM TUPLE
        # Clone tuples from self and r2
        self_cloned_tuples = [t.clone(self.attributes) for t in self.table]
        r2_cloned_tuples = [t.clone(self.attributes) for t in r2.table]
        
        # Find tuples that are in self but not in r2
        difference_tuples = [t1 for t1 in self_cloned_tuples if not any(t1.equals(t2) for t2 in r2_cloned_tuples)]
        
        result = Relation("MINUS_RESULT", self.attributes, self.domains)
        for t in difference_tuples:
            result.addTuple(t)
        
        return result
    
    def rename(self, cnames):
        new_attrs = cnames
        new_doms = self.domains
        rel = Relation(self.name, new_attrs, new_doms)

        for t in self.table:
            rel.addTuple(t.clone(new_attrs))

        return rel

    def times(self, r2):
        attrs1 = self.attributes
        attrs2 = r2.attributes

        new_attrs = attrs1 + attrs2
        new_doms = self.domains + r2.domains
        rel = Relation(f"{self.name}_TIMES_{r2.name}", new_attrs, new_doms)

        for t1 in self.table:
            for t2 in r2.table:
                new_tuple = t1.concatenate(t2, r2.attributes, r2.domains)
                rel.addTuple(new_tuple.clone(new_attrs))

        return rel
    
    def project(self, cnames):
        new_attrs = []
        new_doms = []
        for cname in cnames:
            index = self.attributes.index(cname)
            new_attrs.append(cname)
            new_doms.append(self.domains[index])

        rel = Relation(f"PROJECT_{self.name}", new_attrs, new_doms)
        unique_tuples = []

        for tup in self.table:
            projected_tuple = tup.project(cnames)
            if all(not projected_tuple.equals(existing_tup) for existing_tup in unique_tuples):
                unique_tuples.append(projected_tuple)
                rel.addTuple(projected_tuple.clone(new_attrs))

        return rel
    
    def select(self,lopType,lopValue,comparison,ropType,ropValue):
        new_attrs = self.attributes
        new_doms = self.domains
        result = Relation(f"SELECT_{lopValue}{comparison}{ropValue}", new_attrs, new_doms)

        for tup in self.table:
            if tup.select(lopType, lopValue, comparison, ropType, ropValue):
                result.addTuple(tup.clone(new_attrs))

        return result
    

    def join(self,r2):
        attr = []
        dom = []

        for a, d in zip(self.attributes, self.domains):
            attr.append(a)
            dom.append(d)

        for a, d in zip(r2.attributes, r2.domains):
            if a not in self.attributes:
                attr.append(a)
                dom.append(d)

        rel = Relation("JOIN_RESULT", attr, dom)

        for t1 in self.table:
            for t2 in r2.table:
                joined_tuple = t1.join(t2)
                if joined_tuple is not None:
                    rel.addTuple(joined_tuple)

        return rel
    
    def get_attributes(self):
        return self.attributes

    def get_domains(self):
        return self.domains
   
 
