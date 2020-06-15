from commonUtils.CommandLineProcessor import CommandLineProcessor


class ClickFunUtil:
    def __init__(self):
        self.cli = CommandLineProcessor()

        @self.cli.command('lycaCommonArg')
        @self.cli.option('-run_date', '--run_date', type=int)
        @self.cli.option('-module', '--module')
        @self.cli.option('-submodule', '--submodule')
        # def lycaDailyLoad(run_date, module, submodule):
        #     """Creates a resources for LycaETL."""
        #     return json.dumps({'run_date': run_date, 'module': module, 'submodule': submodule})
        #
        # @click.pass_context
        def main(ctx, run_date, module, submodule):
            ctx.obj = {
                'run_date': run_date,
                'module': module,
                'submodule': submodule
            }

        self.cli.processCLIArguments()
