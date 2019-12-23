import functools
import os
import sqlite3
import copy

import utils


class DB:
    """
    Cette classe représente une base de données
    que l'on veut manipuler.
    """

    def __init__(self, db_name: str):
        self._name = db_name
        self._path = os.path.abspath(self._name)
        self._conn = sqlite3.connect(self._path)

    @property
    def has_df_table(self):
        """True si la table des DF existe dans la base de données"""
        return 'FuncDep' in self.tables

    @property
    def name(self) -> str:
        return self._name

    @property
    def tables(self) -> list:
        c = self._conn.cursor()
        c.execute('SELECT name FROM sqlite_master WHERE type="table";')
        return [t[0] for t in c.fetchall()]

    def get_fields(self, table: str) -> list:

        # La table doit exister
        if table not in self.tables:
            raise UnknownTableError()

        # La table n'est pas celle des DF
        if table == 'FuncDep':
            raise DFTableError()

        c = self._conn.cursor()
        c.execute('PRAGMA table_info(' + table + ')')
        return [t[1] for t in c.fetchall()]

    def add_df(self, table: str, lhs: str, rhs: str):

        # La table doit exister
        if table not in self.tables:
            raise UnknownTableError()

        # La table n'est pas celle des DF
        if table == 'FuncDep':
            raise DFTableError()

        table_fields = self.get_fields(table)

        # Tous les champs de la prémisse existent dans la table
        for field in lhs.split():
            if field not in table_fields:
                raise UnknownFieldsError()

        # La doit être singulière
        if len(rhs.split()) > 1:
            raise DFNotSingularError()

        # Le champ de déffini doit exister dans la table
        if rhs not in self.get_fields(table):
            raise UnknownFieldsError()

        # Le champ rhs ne doit pas être dans les champs lhs
        if rhs in lhs.split():
            raise RHSIncludeToLHSError()

        c = self._conn.cursor()

        # On crée la tables des DF si besoin
        if not self.has_df_table:
            utils.execute_sql_file(c, os.path.join('misc', 'init_df_table.sql'))

        try:
            c.execute('INSERT INTO `FuncDep` VALUES (?, ?, ?)', (table, lhs, rhs))
        except sqlite3.IntegrityError:
            raise DFAddTwiceError()

    def del_df(self, table: str, lhs: str, rhs: str):
        df =  (table, lhs, rhs)

        # La table doit exister
        if table not in self.tables:
            raise UnknownTableError()

        # La DF doit exister
        if df not in self.list_df():
            raise DFNotFoundError()

        c = self._conn.cursor()

        c.execute('DELETE FROM `FuncDep` WHERE `table` = ? AND `lhs` = ? AND `rhs` = ?', df)

    def list_df(self) -> list:
        c = self._conn.cursor()

        # La table doit exister
        if not self.has_df_table:
            return []
        
        c.execute('SELECT * FROM `FuncDep`')
        return c.fetchall()

    def list_table_df(self, table: str) -> list:
        # La table doit exister
        if table not in self.tables:
            raise UnknownTableError()

        c = self._conn.cursor()

        if not self.has_df_table:
            return []
        
        c.execute('SELECT * FROM `FuncDep` WHERE `table` = ?', (table,))
        return c.fetchall()

    def purge_df(self):
        c = self._conn.cursor()
        c.execute('DELETE FROM `FuncDep`')

    def _check_df_set(self, dfs: list) -> dict:
        c = self._conn.cursor()
        res = {}

        for df in dfs:
            unique_lhs = c.execute('SELECT DISTINCT ' + df[1].replace(' ', ', ') + ' FROM ' + df[0] + ';') 

            for lhs in unique_lhs:
                conditions_list = ['{}={}'.format(f, v.__repr__()) for f, v in zip(df[1].split(), lhs)]
                conditions = functools.reduce(lambda a, b : a + ' AND ' + b, conditions_list)

                c.execute('SELECT DISTINCT {} FROM {} WHERE {};'.format(df[2], df[0], conditions))
                assos = c.fetchall()

                res[df] = [t for t in c.execute('SELECT DISTINCT * FROM {} WHERE {};'.format(df[0], conditions))] \
                    if len(assos) > 1 else []

        return res

    def check_df(self) -> dict:
        """Vérifie si les DF sont respectées"""
        return self._check_df_set(self.list_df())

    def check_table_df(self, table: str) -> dict:
        """Vérifie si les DF sont respectées"""

        # La table doit exister
        if table not in self.tables:
            raise UnknownTableError()

        return self._check_df_set(self.list_table_df(table))

    def _is_include(self, sub: list, lset: list) -> bool:
        for e in sub:
            if e not in lset:
                return False
        
        return True

    def df_closure(self, attributes: str, dfs: list) -> list:
        res = attributes.split()
        res_has_changed = True
        dfs = copy.deepcopy(dfs)
        next_dfs = copy.deepcopy(dfs)

        while res_has_changed:
            res_has_changed = False
            dfs = copy.deepcopy(next_dfs)

            for df in dfs:

                lhs = df[1].split()

                if self._is_include(lhs, res):
                    next_dfs.remove(df)
                    if df[2] not in res: 
                        res_has_changed = True
                        res.append(df[2])

        return res

    def is_df_useless(self, check_df: tuple) -> bool:
        dfs = self.list_df()

        if check_df in dfs:
            dfs.remove(check_df)

        deter = self.df_closure(check_df[1], dfs)

        return check_df[2] in deter

    def find_useless_df(self) -> list:
        res = []

        for df in self.list_df():
            if self.is_df_useless(df):
                res.append(df)

        return res

    def clean_useless_df(self):
        clean = 0 == len(self.find_useless_df())

        while not clean:
            clean = True
            useless_dfs = self.find_useless_df()
            if 0 < len(useless_dfs):
                clean = False
                df1 = useless_dfs[0]
                self.del_df(df1[0], df1[1], df1[2])

    def clean_inconsistent_df(self):
        dfs = self.list_df()

        for df in dfs:
            if df[0] not in self.tables:
                self.del_df(df[0], df[1], df[2])

            inconsistent = False

            for att in df[1].split()+[df[2]]:
                if att not in self.get_fields(df[0]):
                    inconsistent = True

            if inconsistent:
                self.del_df(df[0], df[1], df[2])

    def clean(self):
        self.clean_inconsistent_df()
        self.clean_useless_df()

    def is_key(self, table: str, attributes: str) -> bool:
        all_att = self.get_fields(table)
        closure = self.df_closure(attributes, self.list_table_df(table))

        return self._is_include(all_att, closure)

    def super_key(self, table: str) -> list:
        # La table doit exister
        if table not in self.tables:
            raise UnknownTableError()

        att = self.get_fields(table)
        return [sub for sub in utils.get_all_subset(att) if self.is_key(table, utils.list2str(sub))]

    def key(self, table: str) -> list:
        # La table doit exister
        if table not in self.tables:
            raise UnknownTableError()

        super_key = self.super_key(table)
        res = []

        for sk in super_key:
            super_copy = copy.deepcopy(super_key)
            super_copy.remove(sk)
            is_key = True

            for k in super_copy:
                if self._is_include(k, sk):
                    is_key = False

            if is_key:
                res.append(sk)

        return res

    def is_bcnf_table(self, table: str) -> list:
        # La table doit exister
        if table not in self.tables:
            raise UnknownTableError()

        res = []

        for df in self.list_table_df(table):
            if not self.is_key(table, df[1]):
                res.append(df)

        return res

    def is_bcnf(self) -> dict:
        res = {}

        for t in self.tables:
            res[t] = self.is_bcnf_table(t)

        return res

    def is_3nf_table(self, table: str) -> list:
        # La table doit exister
        if table not in self.tables:
            raise UnknownTableError()
        
        bcnf = self.is_bcnf_table(table)
        res = []

        for df in bcnf:
            ok = False
            for sk in  self.key(table):
                if df[2] in sk:
                    ok = True 
            
            if not ok:
                res.append(df)

        return res

    def is_3nf(self) -> dict:
        res = {}

        for t in self.tables:
            res[t] = self.is_3nf_table(t)

        return res

    def find_fields(self, names: list, description: list) -> list:
        res = []

        for name in names:
            for d in description:
                if d[1] == name:
                    res.append(d)
                    break
        
        return res

    def get_content(self, att: list, table: str) -> list:
        para = functools.reduce(lambda a,   b: a+', '+b, att)
        c = self._conn.cursor()
        c.execute('SELECT DISTINCT ' + para  + ' FROM ' + table +  ';')
        return c.fetchall()

    def normalize_table(self, table: str):
        new_tables = []
        dfs = self.list_table_df(table)
        c = self._conn.cursor()
        c.execute('PRAGMA table_info(' + table + ')')

        fields_description = c.fetchall()

        for df in self.is_3nf_table(table):
            field2rm = self.find_fields([df[2]], fields_description)[0]
            new_fields = self.find_fields(df[1].split()+[df[2]], fields_description)
            fields_description.remove(field2rm)
   
            content = self.get_content(df[1].split()+[df[2]], table)

            dfs.remove(df)
            new_tables.append((new_fields, content, [df]))

        content = self.get_content([f[1] for f in fields_description], table)
        new_tables.append((fields_description, content, dfs))

        return new_tables

    def add_content(self, c, nt, n, table):
        request = 'INSERT INTO `{}` VALUES ('.format(table+'_'+str(n))
        insert = ['?' for n in range(len(nt[0]))]
        request += functools.reduce(lambda a, b: a+', '+b, insert) if len(insert) > 1 else insert[0]
        request += ');'

        if len(nt[1]) >= 1:
            c.executemany(request, nt[1])

    def create_new_table(self, c, nt, n, table):
        request = "CREATE TABLE IF NOT EXISTS `{}`(".format(table+'_'+str(n))
        fields = []

        for field in nt[0]:
            freq ="`{}` {}".format(field[1], field[2])
            fields.append(freq)
        
        request += functools.reduce(lambda a, b: a+','+b, fields) if len(fields) > 1 else fields[0]
        request += ');'

        c.execute(request)

    def add_new_df(self, c, nt, n, table):
        new_df = [(table+'_'+str(n), df[1], df[2]) for df in nt[2]]
        if len(new_df) > 0:
            c.executemany('INSERT INTO `FuncDep` VALUES (?, ?, ?);', new_df)

    def normalize(self):
        conn = sqlite3.connect('normalize.sqlite')
        c = conn.cursor()

        utils.execute_sql_file(c, os.path.join('misc', 'init_df_table.sql'))
        tables = self.tables

        if 'FuncDep' in tables:
            tables.remove('FuncDep')
        
        for table in tables:
            decom = self.normalize_table(table)

            for n, nt in enumerate(decom):
                self.create_new_table(c, nt, n, table)
                self.add_content(c, nt,  n, table)
                self.add_new_df(c, nt, n, table)

        conn.commit()
        conn.close()
                
    def close(self):
        self._conn.commit()
        self._conn.close()


class UnknownTableError(Exception):
    pass


class UnknownFieldsError(Exception):
    pass


class DFNotFoundError(Exception):
    pass


class DFNotSingularError(Exception):
    pass


class DFAddTwiceError(Exception):
    pass


class DFTableError(Exception):
    pass


class RHSIncludeToLHSError(Exception):
    pass
