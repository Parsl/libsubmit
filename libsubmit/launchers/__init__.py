from libsubmit.launchers.launchers import singleNodeLauncher
from libsubmit.launchers.launchers import srunLauncher
from libsubmit.launchers.launchers import srunMpiLauncher
from libsubmit.launchers.launchers import aprunLauncher

Launchers = { "singleNode" : singleNodeLauncher,
              "srun"       : srunLauncher,
              "srun_mpi"   : srunMpiLauncher,
              "aprun"      : aprunLauncher }
