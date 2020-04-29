import argparse
import json
import logging
import sys

moduleTag = (
            'rrbs',
            'mon'
            )
submoduleTag = (
            'sms',
            'voice',
            'topup',
            'gprs'
            )


class CommandLineProcessor:
    log = logging.getLogger(__name__)
    common_args = argparse.ArgumentParser(add_help=False)
    log_group = common_args.add_mutually_exclusive_group()
    log_group.add_argument(
        '-v',
        '--verbose',
        dest='verbosity',
        default=[logging.INFO],
        action='append_const',
        const=-10,
        help='more verbose',
    )
    log_group.add_argument(
        '-q',
        '--quiet',
        dest='verbosity',
        action='append_const',
        const=10,
        help='less verbose',
    )

    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.subparsers = self.parser.add_subparsers(dest='command')
        self.subparsers.required = True

    def command(self, name, *args, **kwargs):
        """Register a function to the command-line interface."""

        def wrapper(f):
            f.parser = self.subparsers.add_parser(
                name, *args, description=f.__doc__,
                parents=[self.common_args], **kwargs)
            if getattr(f, 'cli_args', None) is not None:
                for fargs, fkwargs in f.cli_args:
                    f.parser.add_argument(*fargs, **fkwargs)
            f.parser.set_defaults(action=f)
            return f

        return wrapper

    def option(self, *args, **kwargs):
        """Register CLI arguments for function.
           Accepts the same arguments as ArgumentParser().add_argument(...)
            """
        def wrapper(f):
            if getattr(f, 'cli_args', None) is None:
                f.cli_args = []
            f.cli_args.append((args, kwargs))
            return f

        return wrapper

    def processCLIArguments(self):
        """Parse arguments and run the default action."""
        args = self.parser.parse_args()
        # init logging
        log_level = max(logging.DEBUG, min(logging.CRITICAL, sum(args.verbosity)))
        debug_on = log_level <= logging.DEBUG
        logging.basicConfig(level=log_level)
        kwargs = dict(vars(args))
        kwargs.pop('action', None)
        kwargs.pop('command', None)
        kwargs.pop('verbosity', None)
        try:
            # callback action
            args.action(**kwargs)
        except Exception as e:
            self.log.error(e, exc_info=debug_on)
            sys.exit(1)
        sys.exit(0)


cli = CommandLineProcessor()


@cli.command('lycaCommonArg')
@cli.option('-run_date', '--run_date', type=int)
@cli.option('-module', '--module', choices=moduleTag)
@cli.option('-submodule', '--submodule', choices=submoduleTag)
def lycaDailyLoad(run_date, module, submodule):
    """Creates a resources for LycaETL."""
    cli.log.info('creating resources for LycaETL')
    cli.log.info(json.dumps({'run_date': run_date, 'module': module, 'submodule': submodule}))


if __name__ == '__main__':
    cli.processCLIArguments()
