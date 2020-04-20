########################################################################
# description     : Source related exception                           #
# author          : Naren K(narendra.kumar@cloudwick.com),             #
#                   Tejveer Singh (tejveer.singh@cloudwick.com)        #
#                   Shubhajit Saha (shubhajit.saha@cloudwick.com)      #
# contributor     :                                                    #
# version         : 1.0                                                #
# notes           :                                                    #
########################################################################


class SourceException:
    def sourceException(self,exception):
        raise Exception("Source file not found" + exception)