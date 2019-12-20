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
        c.execute('PRAGMA table_info(' + table + ')')  # TODO: use prepare request 
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

    def _is_include(self, sub: list,lset: list) -> bool:
        for e in sub:
            if e not in lset:
                return False
        
        return True

    def df_closure(self, atributes: str, dfs: list) -> list:
        res = atributes.split()
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

    def close(self):
        self._conn.commit()
        self._conn.close()





    #Identification des superclés 2*n-1 possibilité pour n attributs          
    def find_super_key(self, attributes : str, dfs: list) -> list:
        res = attributes.split()  #Exemple :attributes = "num dept name" devient res = ["num","dept","name"]
        tan = [[x] for x in res]  
        fes=[]
        #Toutes les combinaisons d'attributs entre eux (Pas de doublon) 
        for b in tan:
            for a in res:
                if a not in b:
                    sam = b.copy()
                    sam.append(a)
                    sam.sort()
                    if sam not in tan:
                        tan.append(sam)               
                        tan.sort()                                                                     

        #Verification si la combinaison est une superclé avec la cloture
        for n in tan:                                   
            stn= " ".join(n)                            
            test =self.df_closure(stn,dfs)
            test.sort()                                 
            res.sort()                                  
            if test == res and test not in fes:
                fes.append(n)                       
        return fes                                       



    #Partie Identification des clés candidates
    def find_ckey(self, attributes : str, dfs: list) -> list:
        final=[]
        res = attributes.split()
        m_len = len(res)
        ckeys=self.find_super_key(attributes,dfs)
        for key in ckeys:
            if len(key)< m_len:
                final=[key]
                m_len= len(key)
            elif len(key)== m_len:
                final.append(key)
        return final                                    



    #determiner si en BCNF
    def is_bcnf(self,attributes, dfs : list) -> bool:
        key =self.find_super_key(attributes,dfs)
        for fd in dfs:
            lhs = fd[1].split()
            if lhs not in key:
                return False
        return True
    

    #determiner si en  3nf en verifiant une des 2 conditions
    def is_3nf(self,attributes, dfs : list) -> bool:
        #Si elle est en BCNF alors elle est ne 3NF
        if self.is_bcnf(attributes,dfs):                
            return True
        keys =self.find_ckey(attributes,dfs)             
        
        #Test 2eme condition
        for fd in dfs:
            rhs = fd[2]  
            for key in keys:                              
                if rhs in key:
                  return True
        return False


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
