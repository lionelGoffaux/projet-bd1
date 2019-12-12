import argparse
import cmd

import funcdep


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

        print()
        for t in self.db.tables:
            if t != 'FuncDep':
                print('- ' + t)
        print()

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
        except (funcdep.UnknowTableError, funcdep.DFTableError):
            print('ERROR: Tables not exists')
            return

        print()
        for f in fields:
            print('- ' + f)
        print()

    def do_listDF(self, args):
        if not self.db:
            print('ERROR: No DB connected')
            return

        try:
            parser = CmdParser('listDF')
            parser.add_argument('table', nargs='?')
            args = parser.parse_args(args.split())
        except ArgumentError:
            return

        dfs = None
        try:
            dfs = self.db.list_table_df(args.table) if args.table else self.db.list_df()
        except funcdep.UnknowTableError:
            print('ERROR: Tables not exists')
            return

        print()
        for df in dfs:
            print('- ' + str(df))
        print()

    def do_exit(self, args):
        self.do_disconnect("")
        print('bye')
        return True


class ArgumentError(Exception):
    pass


if __name__ == '__main__':
    FuncDepCLI().cmdloop()
