# check distinct value from csv 
cat MTG_VINTAGE.csv|cut  -d"|" -f7|sort -u

cd /v/region/na/appl/banking/pbg_analytics_prql/data/prod_cde/Falcon_Nonpii/PreAcxiom/InputTables/

module load R/core/3.2.3

/ms/dist/python/PROJ/core/2.7.11-0-64/bin/python
/ms/dist/python/PROJ/core/3.4.2-0-64/bin/python


/ms/dist/R/PROJ/core/3.2.3/bin/R

# python library
http://trainportal/
http://msdl.webfarm.ms.com/perl5/msdw-mdp/fcgi-bin/mdp/module/list?core=python&searchstring=


import ms.version
ms.version.addpkg('pyxml', '0.8.4')

# import numpy
import ms.version
ms.version.addpkg('numpy', '1.11.1-ms2')
import numpy as np

# import pandas
import ms.version
ms.version.addpkg('pandas', '0.19.2')
ms.version.addpkg('dateutil', '2.6.0')
ms.version.addpkg('pytz', '2016.6.1')
ms.version.addpkg('six', '1.9.0')
ms.version.addpkg('pandas', '0.19.2')
import six,pytz,dateutil
import pandas as pd


##############################################################
# old import code
import ms.version
ms.version.addpkg('pyxml', '0.8.4')
# import numpy
import ms.version
ms.version.addpkg('numpy', '1.11.1-ms2')
import numpy as np
# import pandas
import ms.version
ms.version.addpkg('pandas', '0.19.2')
ms.version.addpkg('dateutil', '2.1')
ms.version.addpkg('pytz', '2016.6.1')
ms.version.addpkg('six', '1.9.0')
ms.version.addpkg('pandas', '0.19.2')
import six,pytz,dateutil
import pandas as pd 
ms.version.addpkg('matplotlib', '1.3.1')
import _xmlplus
import ms.version
ms.version.addpkg('matplotlib', '1.3.1')
ms.version.addpkg('pyparsing', '2.0.1')
import pyparsing
import matplotlib.pyplot as plt
