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
        if self.db:
            self.db.close()
            self.db = None
            self.prompt = '>> '

    def do_tables(self, args):
        if not self.db:
            print('ERROR: No DB connected')
            return

        utils.print_list(self.db.tables)

    def do_fields(self, args):
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
        if not self.db:
            print('ERROR: No DB connected')
            return

        parser = CmdParser('add')
        parser.add_argument('table')
        parser.add_argument('lhs', nargs='*')
        parser.add_argument('rhs')
        try:
            args = parser.parse_args(args.split())
        except ArgumentError:
            return

        lhs = functools.reduce(lambda a, b: a + ' ' + b, args.lhs)

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
        self.db.clean()

    def do_purge(self, args):
        self.db.purge_df()

    def do_closure(self, args):
        parser = CmdParser('closure')
        parser.add_argument('attributes', nargs='*')
        try:
            args = parser.parse_args(args.split())
        except ArgumentError:
            return

        att = functools.reduce(lambda a, b: str(a) +  ' ' + str(b), args.attributes)
        closure = self.db.df_closure(att, self.db.list_df())

        utils.print_list(closure)

    def do_key(self, args):
        if not self.db:
            print('ERROR: No DB connected')
            return

        try:
            parser = CmdParser('key')
            parser.add_argument('table')
            args = parser.parse_args(args.split())
        except ArgumentError:
            return

        att = functools.reduce(lambda a, b: str(a)+' '+str(b), self.db.get_fields(args.table))

        keys = self.db.find_ckey(att, self.db.list_table_df(args.table))

        utils.print_list(keys)

    def do_exit(self, args):
        self.do_disconnect("")
        print('bye')
        return True


class ArgumentError(Exception):
    pass


if __name__ == '__main__':
    FuncDepCLI().cmdloop()
