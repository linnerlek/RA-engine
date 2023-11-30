import ply.lex as lex

reserved = {
    'project': 'PROJECT', 'rename': 'RENAME', 'union': 'UNION', 'intersect': 'INTERSECT',
    'minus': 'MINUS', 'join': 'JOIN', 'times': 'TIMES', 'select': 'SELECT', 'and': 'AND'
}

tokens = [
    'SEMI', 'COMPARISION', 'LPARENT', 'RPARENT', 'COMMA', 'NUMBER', 'ID', 'STRING',
    'LBRACKET', 'RBRACKET'
] + list(reserved.values())

t_SEMI = r';'
t_AND = r'[Aa][Nn][Dd]'
t_LPARENT = r'\('
t_RPARENT = r'\)'
t_PROJECT = r'[Pp][Rr][Oo][Jj][Ee][Cc][Tt]'
t_RENAME = r'[Rr][Ee][Nn][Aa][Mm][Ee]'
t_UNION = r'[Uu][Nn][Ii][Oo][Nn]'
t_MINUS = r'[Mm][Ii][Nn][Uu][Ss]'
t_INTERSECT = r'[Ii][Nn][Tt][Ee][Rr][Ss][Ee][Cc][Tt]'
t_JOIN = r'[Jj][Oo][Ii][Nn]'
t_TIMES = r'[Tt][Ii][Mm][Ee][Ss]'
t_SELECT = r'[Ss][Ee][Ll][Ee][Cc][Tt]'
t_COMMA = r','
t_COMPARISION = r'<>|<=|>=|<|>|='
t_RBRACKET = r'\]'
t_LBRACKET = r'\['

t_ignore = ' \t'

def t_STRING(t):
    r"'[^']*'"
    t.value = t.value.strip()[1:-1]
    t.type = 'STRING'
    return t

def t_NUMBER(t):
    r'[-+]?[1-9][0-9]*(\.([0-9]+)?)?'
    # t.value = float(t.value)
    t.type = 'NUMBER'
    return t

def t_ID(t):
    r'[a-zA-Z][_a-zA-Z0-9]*'
    t.type = reserved.get(t.value.lower(), 'ID')
    t.value = t.value.upper()
    return t   

t_ignore_COMMENT = r'\#.*' 

def t_newline(t):
    r'[\r\n]+'
    t.lexer.lineno += len(t.value)

def t_error(t):
    print("Illegal Character '%s'" % t.value[0])
    t.lexer.skip(1)
    # raise Exception("Lexer Error")

lexer = lex.lex()

#data = '''project[dname](
#select[dnumber="25"](department)
#)'''
#
#lexer.input(data)
#
#while True:
#    tok = lexer.token()
#    if not tok:
#        break
#    print(tok)