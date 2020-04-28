import click


@click.command()
@click.option('--rundate', '-run_date', help='pass run date', type=int)
@click.option('--module', '-m', help='pass valid module name like "{m}"'.format(m='rrbs'))
@click.option('--submodule', '-sbm', help='pass valid sub module name like "{sbmName}"'.format(sbmName='sms'))
def processCLIArgument(rundate, module, submodule):
    click.argument
    print('Project running for {}'.format(rundate))
    print('Project running for {}'.format(module))
    print('Project running for {}'.format(submodule))


if __name__ == '__main__':
    processCLIArgument()
    # getModuleParam()
    # getSubModule()