import click


@click.command()
@click.option('--rundate', '-run_date', help='pass run date', type=int)
@click.option('--module', '-m', help='pass valid module like rrbs')
# @click.option('--submodule', '-sbm', help='pass valid module like rrbs')
def getBuildParam(rundate, module):
    print('Project running for {}'.format(rundate))
    print('Project running for {}'.format(module))
    # print('Project running for {}'.format(submodule))


# def getModuleParam(module):
#     print('Project running for {}'.format(module))


if __name__ == '__main__':
    getBuildParam()
    # getModuleParam()
    # getSubModule()