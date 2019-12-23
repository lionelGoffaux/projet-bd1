import argparse
import cmd
import functools

import funcdep
import utils


class CmdParser(argparse.ArgumentParser):

    def __init__(self, cmd=''):
        super().__init__(prog=cmd,  add_help=False)

    def error(self, message):
        self.print_help()
        raise ArgumentError()
        

class FuncDepCLI(cmd.Cmd):
    intro = """
========= FUNC DEP CLI =========
version: 1.0
type help or ? to get help
"""
    prompt = '>> '
    db = None

    def do_connect(self, args):
        """Connecte l'application au fichier sqlite demandé"""
        if self.db:
            print('ERROR: Already connected')
            return

        try:
            parser = CmdParser('connect')
            parser.add_argument('db_name')
            args = parser.parse_args(args.split())
        except ArgumentError:
            return

        self.db = funcdep.DB(args.db_name)
        self.prompt = '({}) '.format(args.db_name) + self.prompt

    def do_disconnect(self, args):
        """Déconnecte de la base de données actuelle"""
        if self.db:
            self.db.close()
            self.db = None
            self.prompt = '>> '

    def do_tables(self, args):
        """Liste les tables de las base de données"""
        if not self.db:
            print('ERROR: No DB connected')
            return

        utils.print_list(self.db.tables)

    def do_fields(self, args):
        """Liste les champs d'une table de la base de données"""
        if not self.db:
            print('ERROR: No DB connected')
            return

        try:
            parser = CmdParser('fields')
            parser.add_argument('table')
            args = parser.parse_args(args.split())
        except ArgumentError:
            return

        fields = None
        try:
            fields = self.db.get_fields(args.table)
        except (funcdep.UnknownTableError, funcdep.DFTableError):
            print('ERROR: Tables not exists')
            return

        utils.print_list(fields)

    def do_list(self, args):
        """Liste les DF pour la base de données ou une table"""
        if not self.db:
            print('ERROR: No DB connected')
            return

        try:
            parser = CmdParser('list')
            parser.add_argument('table', nargs='?')
            args = parser.parse_args(args.split())
        except ArgumentError:
            return

        dfs = None
        try:
            dfs = self.db.list_table_df(args.table) if args.table else self.db.list_df()
        except funcdep.UnknownTableError:
            print('ERROR: Table not exists')
            return

        utils.print_list(dfs)

    def do_add(self, args):
        """Ajoute une DF à la base de données"""
        if not self.db:
            print('ERROR: No DB connected')
            return

        parser = CmdParser('add')
        parser.add_argument('table')
        parser.add_argument('lhs', nargs='*')
        parser.add_argument('rhs')
        try:
            args = parser.parse_args(args.split())
            if args.lhs is None or len(args.lhs) == 0:
                parser.print_help()
                return
        except ArgumentError:
            return

        lhs = functools.reduce(lambda a, b: a + ' ' + b, args.lhs) if len(args.lhs) > 1 else args.lhs[0]

        try:
            self.db.add_df(args.table, lhs, args.rhs)
        except funcdep.UnknownTableError:
            print('ERROR: Unknow table')
        except funcdep.UnknownFieldsError:
            print('ERROR: Unknow fields')
        except funcdep.DFNotSingularError:
            print('ERROR: DF not singular')
        except funcdep.DFTableError:
            print('ERROR: This table is de DF table')
        except funcdep.DFAddTwiceError:
            print('ERROR: DF already added')

    def do_del(self, args):
        """Supprime une DF de la base de données"""
        if not self.db:
            print('ERROR: No DB connected')
            return

        parser = CmdParser('del')
        parser.add_argument('table')
        parser.add_argument('lhs', nargs='*')
        parser.add_argument('rhs')
        try:
            args = parser.parse_args(args.split())
        except ArgumentError:
            return

        lhs = functools.reduce(lambda a, b: a + ' ' + b, args.lhs)       

        try:
            self.db.del_df(args.table, lhs, args.rhs)
        except funcdep.UnknownTableError:
            print('ERROR: Unknow table')
        except funcdep.DFNotFoundError:
            print('ERROR: DF not found')

    def do_check(self, args):
        """Vérifie si les DF de la base ou d'une table sont vérifiées"""
        if not self.db:
            print('ERROR: No DB connected')
            return

        try:
            parser = CmdParser('ckeck')
            parser.add_argument('table', nargs='?')
            args = parser.parse_args(args.split())
        except ArgumentError:
            return

        res = None

        try:
            res = self.db.check_table_df(args.table) if args.table else self.db.check_df()
        except funcdep.UnknownTableError:
            print('ERROR: Table not exists')
            return

        for df in res:
            print(df, end='')
            bad_tuples = res[df]

            if len(bad_tuples) == 0:
                print(' ok ')
            else:
                print('\nThis DF is not respected')
                for t in bad_tuples:
                    print('\t- ', t)

    def do_clean(self, args):
        """Supprime les DF inutiles"""
        self.db.clean()

    def do_purge(self, args):
        """Supprime toutes les DF de la base"""
        self.db.purge_df()

    def do_closure(self, args):
        """Calcule la fermeture d'une liste d'attributs"""
        parser = CmdParser('closure')
        parser.add_argument('attributes', nargs='*')
        try:
            args = parser.parse_args(args.split())
            if args.attributes is None or len(args.attributes) == 0:
                parser.print_help()
                return
        except ArgumentError:
            return

        att = functools.reduce(lambda a, b: str(a) + ' ' + str(b), args.attributes)
        closure = self.db.df_closure(att, self.db.list_df())

        utils.print_list(closure)

    def do_key(self, args):
        """Liste les clefs d'une table"""
        if not self.db:
            print('ERROR: No DB connected')
            return

        try:
            parser = CmdParser('key')
            parser.add_argument('table')
            args = parser.parse_args(args.split())
        except ArgumentError:
            return

        try:
            keys = self.db.key(args.table)
        except funcdep.UnknownTableError:
            print('ERROR: Table not exists')
            return

        utils.print_list(keys)

    def do_super_key(self, args):
        """liste les super clefs d'une table"""
        if not self.db:
            print('ERROR: No DB connected')
            return

        try:
            parser = CmdParser('key')
            parser.add_argument('table')
            args = parser.parse_args(args.split())
        except ArgumentError:
            return

        try:
            keys = self.db.super_key(args.table)
        except funcdep.UnknownTableError:
            print('ERROR: Table not exists')
            return

        utils.print_list(keys)

    def do_3nf(self, args):
        """Verifie si les tables sont en 3NF"""
        if not self.db:
            print('ERROR: No DB connected')
            return

        res = self.db.is_3nf()

        for table in res:
            print(table, end='')
            if len(res[table]) == 0:
                print(' ok')
            else:
                print('\nThis table is not in 3NF')
                for df in res[table]:
                    print('\t- ', df)

    def do_bcnf(self, args):
        """Vérifie si les tables sont en BCNF"""
        if not self.db:
            print('ERROR: No DB connected')
            return

        res = self.db.is_bcnf()

        for table in res:
            print(table, end='')
            if len(res[table]) == 0:
                print(' ok')
            else:
                print('\nThis table is not in BCNF')
                for df in res[table]:
                    print('\t- ', df)

    def do_normalize(self, args):
        """Crée une autre basse de données(normalize.sqlite) normalisée"""
        if not self.db:
            print('ERROR: No DB connected')
            return

        self.db.normalize()

    def do_exit(self, args):
        """Quite l'application"""
        self.do_disconnect("")
        print('bye')
        return True


class ArgumentError(Exception):
    pass


if __name__ == '__main__':
    FuncDepCLI().cmdloop()
