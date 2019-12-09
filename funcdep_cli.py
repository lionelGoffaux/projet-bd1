import argparse
import cmd


class CmdParser(argparse.ArgumentParser):

    def __init__(self, cmd=''):
        super().__init__(prog=cmd)

    def error(self, message):
        self.print_help()


class FuncDepCLI(cmd.Cmd):
    intro = """
========= FUNC DEP CLI =========
version: 1.0
type help or ? to get help
"""
    prompt = '>> '

    def do_test(self, args):
        parser = CmdParser('test')
        parser.parse_args(args.split())

    def do_exit(self, args):
        print('bye')
        return True


if __name__ == '__main__':
    FuncDepCLI().cmdloop()
