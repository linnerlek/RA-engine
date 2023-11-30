from Node import *
import ply.yacc as yacc
from RALexer import tokens

def p_query(p):
    'query : expr SEMI'
    p[0] = p[1]

def p_expr(p):
    '''expr : proj_expr 
            | rename_expr 
            | union_expr     
            | minus_expr 
            | intersect_expr 
            | join_expr 
            | times_expr 
            | select_expr '''
    p[0] = p[1]

def p_ID(p):
    'expr : ID'
    #p[0] = ['id', p[1]]
    n = Node("relation",None,None)
    n.set_relation_name(p[1])
    p[0] = n

def p_proj_expr(p):
    'proj_expr : PROJECT LBRACKET attr_list RBRACKET LPARENT expr RPARENT'
    #p[0] = ['project', p[3], p[6]]
    n = Node("project",p[6],None)
    n.set_columns(p[3])
    p[0] = n

def p_rename_expr(p):
    'rename_expr : RENAME LBRACKET attr_list RBRACKET LPARENT expr RPARENT'
    #p[0] = ['rename', p[3], p[6]]
    n = Node("rename",p[6],None)
    n.set_columns(p[3])
    p[0] = n

def p_attr_list(p):
    'attr_list : ID'
    p[0] = [p[1].upper()]

def p_attr_list_2(p):
    'attr_list : attr_list COMMA ID'
    p[0] = p[1] + [p[3].upper()]

def p_union_expr(p):
    'union_expr : LPARENT expr UNION expr RPARENT'
    #p[0] = ['union', p[2], p[4]]
    n = Node("union",p[2],p[4])
    p[0] = n

def p_minus_expr(p):
    'minus_expr : LPARENT expr MINUS expr RPARENT'
    #p[0] = ['minus', p[2], p[4]]
    n = Node("minus",p[2],p[4])
    p[0] = n

def p_intersect_expr(p):
    'intersect_expr : LPARENT expr INTERSECT expr RPARENT'
    #p[0] = ['intersect', p[2], p[4]]
    n = Node("intersect",p[2],p[4])
    p[0] = n

def p_join_expr(p):
    'join_expr : LPARENT expr JOIN expr RPARENT'
    #p[0] = ['join', p[2], p[4]]
    n = Node("join",p[2],p[4])
    p[0] = n

def p_times_expr(p):
    'times_expr : LPARENT expr TIMES expr RPARENT'
    #p[0] = ['times', p[2], p[4]]
    n = Node("times",p[2],p[4])
    p[0] = n

def p_select_expr(p):
    'select_expr : SELECT LBRACKET condition RBRACKET LPARENT expr RPARENT'                
    #p[0] = ['select', p[3], p[6]]
    n = Node("select",p[6],None)
    n.set_conditions(p[3])
    p[0] = n

def p_condition(p):
    'condition : simple_condition'
    p[0] = [p[1]]

def p_condition_2(p):
    'condition : condition AND simple_condition'
    p[0] = p[1] + [p[3]]

def p_simple_condition(p):
    'simple_condition : operand COMPARISION operand'
    p[0] = [p[1][0], p[1][1], p[2], p[3][0], p[3][1]]

def p_operand_1(p):
    'operand : ID'
    p[0] = ['col',p[1].upper()]

def p_operand_2(p):
    'operand : STRING'
    p[0] = ['str',p[1]]

def p_operand_3(p):
    'operand : NUMBER'
    p[0] = ['num',float(p[1])]

def p_error(p):
    raise TypeError("Syntax error: '%s'" % p.value)
# print("Syntax error: '%s'" % p.value)

parser = yacc.yacc()