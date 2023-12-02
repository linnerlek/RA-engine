import sys
from RAParser import parser
from Database import *
from Relation import *
from Tuple import *
from Node import *

count = 0


def execute_file(filename, db):
    try:
        with open(filename) as f:
            data = f.read().splitlines()
        result = " ".join(
            list(filter(lambda x: len(x) > 0 and x[0] != "#", data)))
        try:
            tree = parser.parse(result)
            set_temp_table_names(tree)
            msg = semantic_checks(tree, db)
            if msg == 'OK':
                print()
                print(evaluate_query(tree, db))
                print()
            else:
                print(msg)
        except Exception as inst:
            print(inst.args[0])
    except FileNotFoundError:
        print("FileNotFoundError: A file with name " + "'" +
              filename + "' cannot be found")


def read_input():
    result = ''
    data = input('RA: ').strip()
    while True:
        if ';' in data:
            i = data.index(';')
            result += data[0:i+1]
            break
        else:
            result += data + ' '
            data = input('> ').strip()
    return result


def set_temp_table_names(tree):
    global count
    if tree is not None:
        if tree.node_type != "relation":
            tree.set_relation_name("temp_" + str(count))
            count += 1
        if tree.get_left_child() is not None:
            set_temp_table_names(tree.get_left_child())
        if tree.get_right_child() is not None:
            set_temp_table_names(tree.get_right_child())


def semantic_checks(tree, db):
    if tree is not None:
        if tree.node_type == "relation":
            relation_name = tree.get_relation_name()
            relation = db.getRelation(relation_name)
            if relation is None:
                return f"Semantic Error: Relation {relation_name} does not exist in the database."
            tree.set_attributes(relation.get_attributes())
            tree.set_domains(relation.get_domains())

        elif tree.node_type == "select":
            if tree.get_left_child() is not None:
                semantic_checks(tree.get_left_child(), db)

            conditions = tree.get_conditions()
            for cond in conditions:
                lopType, lopValue, comparison, ropType, ropValue = cond

                if lopType == "col" and ropType == "col":
                    left_attributes = tree.get_left_child().get_attributes()
                    left_domains = tree.get_left_child().get_domains()

                    if left_attributes is None or left_domains is None:
                        return f"Semantic Error: Attributes not found in the schema of the left operand."

                    if lopValue not in left_attributes:
                        return f"Semantic Error: Attribute '{lopValue}' not found in the schema."
                    if ropValue not in left_attributes:
                        return f"Semantic Error: Attribute '{ropValue}' not found in the schema."

                    lop_index = left_attributes.index(lopValue)
                    rop_index = left_attributes.index(ropValue)
                    lopDataType = left_domains[lop_index]
                    ropDataType = left_domains[rop_index]

                    if lopDataType != ropDataType:
                        return f"Semantic Error: Data types do not match in comparison: {lopDataType} and {ropDataType}."

                elif lopType == "col":
                    left_attributes = tree.get_left_child().get_attributes()
                    if left_attributes is None:
                        return f"Semantic Error: Attributes not found in the schema of the left operand."
                    if lopValue not in left_attributes:
                        return f"Semantic Error: Attribute '{lopValue}' not found in the schema."
                elif ropType == "col":
                    left_attributes = tree.get_left_child().get_attributes()
                    if left_attributes is None:
                        return f"Semantic Error: Attributes not found in the schema of the left operand."

                    if ropValue not in left_attributes:
                        return f"Semantic Error: Attribute '{ropValue}' not found in the schema."

            tree.set_attributes(tree.get_left_child().get_attributes())
            tree.set_domains(tree.get_left_child().get_domains())

        elif tree.node_type == "rename":
            if tree.get_left_child() is not None:
                semantic_checks(tree.get_left_child(), db)
                new_attributes = tree.get_columns()
                left_child_attributes = tree.get_left_child().get_attributes()

                if left_child_attributes is not None:
                    if len(new_attributes) != len(set(new_attributes)):
                        return "Semantic Error: Duplicate attribute in RENAME operation."

                    if len(new_attributes) != len(left_child_attributes):
                        return "Semantic Error: Not valid number of attributes in RENAME operation."

                    for col in new_attributes:
                        if col not in left_child_attributes:
                            return f"Semantic Error: Attribute '{col}' not found in the schema."

                    tree.set_attributes(new_attributes)
                    tree.set_domains(tree.get_left_child().get_domains())

        elif tree.node_type == "project":
            if tree.get_left_child() is not None:
                semantic_checks(tree.get_left_child(), db)
            columns = tree.get_columns()
            left_child_attributes = tree.get_left_child().get_attributes()
            left_child_domains = tree.get_left_child().get_domains()

            if left_child_attributes is not None:
                for col in columns:
                    if col not in left_child_attributes:
                        return f"Semantic Error: Not a valid attribute: {col}"
                    if columns.count(col) > 1:
                        return f"Semantic Error: Duplicate attribute: {col}"

                new_attributes = columns
                new_domains = [
                    left_child_domains[left_child_attributes.index(col)] for col in columns]

                tree.set_attributes(new_attributes)
                tree.set_domains(new_domains)

        elif tree.node_type in ["union", "minus", "intersect"]:
            if tree.get_left_child() is not None:
                semantic_checks(tree.get_left_child(), db)
            if tree.get_right_child() is not None:
                semantic_checks(tree.get_right_child(), db)

            left_attributes = tree.get_left_child().get_attributes()
            right_attributes = tree.get_right_child().get_attributes()

            if left_attributes is not None and right_attributes is not None:
                if len(left_attributes) != len(right_attributes):
                    return "Semantic Error: Relations do not have the same number of attributes."

                for attr in left_attributes:
                    if attr not in right_attributes:
                        return "Semantic Error: Relations do not have the same attributes."
                    left_attr_type = left_attributes[left_attributes.index(
                        attr)]
                    right_attr_type = right_attributes[right_attributes.index(
                        attr)]

                    if left_attr_type != right_attr_type:
                        return f"Semantic Error: Attributes '{attr}' do not have the same data types."

            tree.set_attributes(left_attributes)
            tree.set_domains(tree.get_left_child().get_domains())

        elif tree.node_type == "join":
            if tree.get_left_child() is not None:
                semantic_checks(tree.get_left_child(), db)
            if tree.get_right_child() is not None:
                semantic_checks(tree.get_right_child(), db)

            left_attributes = tree.get_left_child().get_attributes()
            right_attributes = tree.get_right_child().get_attributes()

            if left_attributes is not None and right_attributes is not None:
                common_attributes = set(
                    left_attributes) & set(right_attributes)

                if not common_attributes:
                    return "Semantic Error: No common attributes for JOIN operation."

                right_unique_attributes = list(
                    set(right_attributes) - common_attributes)

                for common_attr in common_attributes:
                    left_index = left_attributes.index(common_attr)
                    right_index = right_attributes.index(common_attr)

                    left_data_type = tree.get_left_child().get_domains()[
                        left_index]
                    right_data_type = tree.get_right_child().get_domains()[
                        right_index]

                    if left_data_type != right_data_type:
                        return f"Semantic Error: Common attribute '{common_attr}' has different data types."

                tree.set_attributes(left_attributes + right_unique_attributes)
                tree.set_domains(tree.get_left_child().get_domains(
                ) + tree.get_right_child().get_domains())

        elif tree.node_type == "times":
            if tree.get_left_child() is not None:
                semantic_checks(tree.get_left_child(), db)
            if tree.get_right_child() is not None:
                semantic_checks(tree.get_right_child(), db)

            new_attributes = left_attributes + \
                [attr for attr in right_attributes if attr not in common_attributes]
            new_domains = left_domains + [right_domains[right_attributes.index(
                attr)] for attr in right_attributes if attr not in common_attributes]

            tree.set_attributes(new_attributes)
            tree.set_domains(new_domains)

    return "OK"


def evaluate_query(tree, db):
    if tree.node_type == "relation":
        return db.getRelation(tree.get_relation_name())
    elif tree.node_type == "project":
        child_rel = evaluate_query(tree.get_left_child(), db)
        if child_rel is not None:
            columns = tree.get_columns()
            return child_rel.project(columns)
    elif tree.node_type == "rename":
        child_rel = evaluate_query(tree.get_left_child(), db)
        if child_rel is not None:
            new_attrs = tree.get_columns()
            return child_rel.rename(new_attrs)
    elif tree.node_type == "select":
        child_rel = evaluate_query(tree.get_left_child(), db)
        if child_rel is not None:
            conditions = tree.get_conditions()
            for cond in conditions:
                lopType, lopValue, comparison, ropType, ropValue = cond
                child_rel = child_rel.select(
                    lopType, lopValue, comparison, ropType, ropValue)
            return child_rel
    elif tree.node_type == "union":
        left_rel = evaluate_query(tree.get_left_child(), db)
        right_rel = evaluate_query(tree.get_right_child(), db)
        if left_rel is not None and right_rel is not None:
            return left_rel.union(right_rel)
    elif tree.node_type == "minus":
        left_rel = evaluate_query(tree.get_left_child(), db)
        right_rel = evaluate_query(tree.get_right_child(), db)
        if left_rel is not None and right_rel is not None:
            return left_rel.minus(right_rel)
    elif tree.node_type == "intersect":
        left_rel = evaluate_query(tree.get_left_child(), db)
        right_rel = evaluate_query(tree.get_right_child(), db)
        if left_rel is not None and right_rel is not None:
            return left_rel.intersect(right_rel)
    elif tree.node_type == "join":
        left_rel = evaluate_query(tree.get_left_child(), db)
        right_rel = evaluate_query(tree.get_right_child(), db)
        if left_rel is not None and right_rel is not None:
            return left_rel.join(right_rel)
    elif tree.node_type == "times":
        left_rel = evaluate_query(tree.get_left_child(), db)
        right_rel = evaluate_query(tree.get_right_child(), db)
        if left_rel is not None and right_rel is not None:
            return left_rel.times(right_rel)
    return None


def main():
    db = Database()
    db.initializeDatabase(sys.argv[1])

    while True:
        data = read_input()
        if data == 'schema;':
            print(db.displaySchema())
            continue
        if data.strip().split()[0] == "source":
            filename = data.strip().split()[1][:-1]
            execute_file(filename, db)
            continue
        if data == 'help;' or data == "h;":
            print("\nschema; 		# to see schema")
            print("source filename; 	# to run query in file")
            print("exit; or quit; or q; 	# to exit\n")
            continue
        if data == 'exit;' or data == "quit;" or data == "q;":
            break
        try:
            tree = parser.parse(data)
        except Exception as inst:
            print(inst.args[0])
            continue
        msg = semantic_checks(tree, db)
        if msg == 'OK':
            print()
            r = evaluate_query(tree, db)
            r.setName("ANSWER")
            print(r)
        else:
            print(msg)


if __name__ == '__main__':
    main()
