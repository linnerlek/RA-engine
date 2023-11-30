class Tuple:
    def __init__(self, attributes, domains):
        self.attributes = [a.upper() for a in attributes]
        self.domains = [d.upper() for d in domains]
        self.tuple = []

    # Add a tuple component to the end of the tuple
    def addComponent(self, comp):
        self.tuple.append(comp)

    def __str__(self):
    # Return String representation of tuple; See output of run for format.
        result = ""
        for i in range(len(self.attributes)):
            result += f"{self.tuple[i]}:"
        return result

    # Return True if this tuple is equal to compareTuple; False otherwise
    # make sure the schemas are the same; return False if schema's are not same
    def equals(self, compareTuple):
        if len(self.attributes) != len(compareTuple.attributes):
            return False

        for attr, compare_attr in zip(self.attributes, compareTuple.attributes):
            if attr != compare_attr:
                return False

        for val1, val2 in zip(self.tuple, compareTuple.tuple):
            if val1 != val2:
                return False

        return True

    def clone(self, attr):
        new_attr = [c for c in attr]
        new_doms = [d for d in self.domains]
        new_tuple = Tuple(new_attr, new_doms)
        for component in self.tuple:
            new_tuple.addComponent(component)
        return new_tuple

    def concatenate(self, t, attrs, doms):
        new_attributes = self.attributes + attrs
        new_domains = self.domains + doms
        new_tuple = Tuple(new_attributes, new_domains)

        for component in self.tuple:
            new_tuple.addComponent(component)

        for component in t.tuple:
            new_tuple.addComponent(component)

        return new_tuple

    def project(self, cnames):
        doms = [self.domains[self.attributes.index(cname)] for cname in cnames]
        tup = Tuple(cnames, doms)
        for cname in cnames:
            index = self.attributes.index(cname)
            tup.addComponent(self.tuple[index])
        return tup

    def select(self, lopType, lopValue, comparison, ropType, ropValue):
        operators = {
            "=": lambda x, y: x == y,
            "<": lambda x, y: x < y,
            "<=": lambda x, y: x <= y,
            ">": lambda x, y: x > y,
            ">=": lambda x, y: x >= y,
            "<>": lambda x, y: x != y,
        }

        if lopType == "num":
            lopValue = float(lopValue)
        elif lopType == "str":
            lopValue = str(lopValue)
        else:
            lopValue = self.tuple[self.attributes.index(lopValue)]

        if ropType == "num":
            ropValue = float(ropValue)
        elif ropType == "str":
            ropValue = str(ropValue)
        else:
            ropValue = self.tuple[self.attributes.index(ropValue)]


        comparison_op = operators[comparison]
        return comparison_op(lopValue, ropValue)


    def join(self,t2):
        common_attrs = []
        common_positions = []

        for pos, attr in enumerate(self.attributes):
            if attr in t2.attributes:
                common_attrs.append(attr)
                common_positions.append(pos)

        can_join = all(self.tuple[i] == t2.tuple[t2.attributes.index(attr)] for i, attr in zip(common_positions, common_attrs))

        if not can_join:
            return None

        new_attributes = self.attributes + [attr for attr in t2.attributes if attr not in common_attrs]
        new_domains = self.domains + [dom for attr, dom in zip(t2.attributes, t2.domains) if attr not in common_attrs]
        joined_tuple = Tuple(new_attributes, new_domains)

        for component in self.tuple:
            joined_tuple.addComponent(component)

        for attr in t2.attributes:
            if attr not in common_attrs:
                joined_tuple.addComponent(t2.tuple[t2.attributes.index(attr)])

        return joined_tuple
    