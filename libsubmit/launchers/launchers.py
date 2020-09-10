from abc import ABCMeta, abstractmethod
import six

from libsubmit.utils import RepresentationMixin


@six.add_metaclass(ABCMeta)
class Launcher(RepresentationMixin):
    """ Launcher base class to enforce launcher interface
    """
    @abstractmethod
    def __call__(self, command, tasks_per_node, nodes_per_block, walltime=None):
        """ Wraps the command with the Launcher calls.
        *MUST* be implemented by the concrete child classes
        """
        pass


class SimpleLauncher(Launcher):
    """ Does no wrapping. Just returns the command as-is
    """

    def __call__(self, command, tasks_per_node, nodes_per_block, walltime=None):
        """
        Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.

        KWargs:
        - walltime (int) : This is not used by this launcher.
        """
        return command


class SingleNodeLauncher(Launcher):
    """ Worker launcher that wraps the user's command with the framework to
    launch multiple command invocations in parallel. This wrapper sets the
    bash env variable CORES to the number of cores on the machine. By setting
    task_blocks to an integer or to a bash expression the number of invocations
    of the command to be launched can be controlled.
    """
    def __call__(self, command, tasks_per_node, nodes_per_block, walltime=None):
        """
        Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.

        KWargs:
        - walltime (int) : This is not used by this launcher.
        """
        task_blocks = tasks_per_node * nodes_per_block

        x = '''export CORES=$(getconf _NPROCESSORS_ONLN)
echo "Found cores : $CORES"
WORKERCOUNT={1}

CMD ( ) {{
{0}
}}
for COUNT in $(seq 1 1 $WORKERCOUNT)
do
    echo "Launching worker: $COUNT"
    CMD &
done
wait
echo "All workers done"
'''.format(command, task_blocks)
        return x


class GnuParallelLauncher(Launcher):
    """ Worker launcher that wraps the user's command with the framework to
    launch multiple command invocations via GNU parallel sshlogin.

    This wrapper sets the bash env variable CORES to the number of cores on the
    machine.

    This launcher makes the following assumptions:
    - GNU parallel is installed and can be located in $PATH
    - Paswordless SSH login is configured between the controller node and the
      target nodes.
    - The provider makes available the $PBS_NODEFILE environment variable
    """
    def __call__(self, command, tasks_per_node, nodes_per_block, walltime=None):
        """
        Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.

        KWargs:
        - walltime (int) : This is not used by this launcher.
        """
        task_blocks = tasks_per_node * nodes_per_block

        x = '''export CORES=$(getconf _NPROCESSORS_ONLN)
echo "Found cores : $CORES"
WORKERCOUNT={3}

# Deduplicate the nodefile
SSHLOGINFILE="$JOBNAME.nodes"
if [ -z "$PBS_NODEFILE" ]; then
    echo "localhost" > $SSHLOGINFILE
else
    sort -u $PBS_NODEFILE > $SSHLOGINFILE
fi

cat << PARALLEL_CMD_EOF > cmd_$JOBNAME.sh
{0}
PARALLEL_CMD_EOF
chmod u+x cmd_$JOBNAME.sh

#file to contain the commands to parallel
PFILE=cmd_${{JOBNAME}}.sh.parallel

# Truncate the file
cp /dev/null $PFILE

for COUNT in $(seq 1 1 $WORKERCOUNT)
do
    echo "sh cmd_$JOBNAME.sh" >> $PFILE
done

parallel --env _ --joblog "$JOBNAME.sh.parallel.log" \
    --sshloginfile $SSHLOGINFILE --jobs {1} < $PFILE

echo "All workers done"
'''.format(command, tasks_per_node, nodes_per_block, task_blocks)
        return x


class MpiExecLauncher(Launcher):
    """ Worker launcher that wraps the user's command with the framework to
    launch multiple command invocations via mpiexec.

    This wrapper sets the bash env variable CORES to the number of cores on the
    machine.

    This launcher makes the following assumptions:
    - mpiexec is installed and can be located in $PATH
    - The provider makes available the $PBS_NODEFILE environment variable
    """
    def __call__(self, command, tasks_per_node, nodes_per_block, walltime=None):
        """
        Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.

        KWargs:
        - walltime (int) : This is not used by this launcher.
        """
        task_blocks = tasks_per_node * nodes_per_block

        x = '''export CORES=$(getconf _NPROCESSORS_ONLN)
echo "Found cores : $CORES"
WORKERCOUNT={3}

# Deduplicate the nodefile
HOSTFILE="$JOBNAME.nodes"
if [ -z "$PBS_NODEFILE" ]; then
    echo "localhost" > $HOSTFILE
else
    sort -u $PBS_NODEFILE > $HOSTFILE
fi

cat << MPIEXEC_EOF > cmd_$JOBNAME.sh
{0}
MPIEXEC_EOF
chmod u+x cmd_$JOBNAME.sh

mpiexec --bind-to none -n $WORKERCOUNT --hostfile $HOSTFILE /usr/bin/sh cmd_$JOBNAME.sh

echo "All workers done"
'''.format(command, tasks_per_node, nodes_per_block, task_blocks)
        return x


class SrunLauncher(Launcher):
    """ Worker launcher that wraps the user's command with the SRUN launch framework
    to launch multiple cmd invocations in parallel on a single job allocation.
    """

    def __init__(self):
        pass

    def __call__(self, command, tasks_per_node, nodes_per_block, walltime=None):
        """
        Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.

        KWargs:
        - walltime (int) : This is not used by this launcher.
        """
        task_blocks = tasks_per_node * nodes_per_block
        x = '''export CORES=$SLURM_CPUS_ON_NODE
export NODES=$SLURM_JOB_NUM_NODES

echo "Found cores : $CORES"
echo "Found nodes : $NODES"
WORKERCOUNT={1}

cat << SLURM_EOF > cmd_$SLURM_JOB_NAME.sh
{0}
SLURM_EOF
chmod a+x cmd_$SLURM_JOB_NAME.sh

TASKBLOCKS={1}

srun --ntasks $TASKBLOCKS -l bash cmd_$SLURM_JOB_NAME.sh

echo "Done"
'''.format(command, task_blocks)
        return x


class SrunMPILauncher(Launcher):
    """Worker launcher that wraps the user's command with the SRUN launch framework
    to launch multiple cmd invocations in parallel on a single job allocation.

    """
    def __call__(self, command, tasks_per_node, nodes_per_block, walltime=None):
        """
        Args:
        - command (string): The command string to be launched
        - task_block (string) : bash evaluated string.

        KWargs:
        - walltime (int) : This is not used by this launcher.
        """
        task_blocks = tasks_per_node * nodes_per_block
        x = '''export CORES=$SLURM_CPUS_ON_NODE
export NODES=$SLURM_JOB_NUM_NODES

echo "Found cores : $CORES"
echo "Found nodes : $NODES"
WORKERCOUNT={1}

cat << SLURM_EOF > cmd_$SLURM_JOB_NAME.sh
{0}
SLURM_EOF
chmod a+x cmd_$SLURM_JOB_NAME.sh

TASKBLOCKS={1}

# If there are more taskblocks to be launched than nodes use
if (( "$TASKBLOCKS" > "$NODES" ))
then
    echo "TaskBlocks:$TASKBLOCKS > Nodes:$NODES"
    CORES_PER_BLOCK=$(($NODES * $CORES / $TASKBLOCKS))
    for blk in $(seq 1 1 $TASKBLOCKS):
    do
        srun --ntasks $CORES_PER_BLOCK -l bash cmd_$SLURM_JOB_NAME.sh &
    done
    wait
else
    # A Task block could be integer multiples of Nodes
    echo "TaskBlocks:$TASKBLOCKS <= Nodes:$NODES"
    NODES_PER_BLOCK=$(( $NODES / $TASKBLOCKS ))
    for blk in $(seq 1 1 $TASKBLOCKS):
    do
        srun --exclusive --nodes $NODES_PER_BLOCK -l bash cmd_$SLURM_JOB_NAME.sh &
    done
    wait

fi


echo "Done"
'''.format(command, task_blocks)
        return x


class AprunLauncher(Launcher):
    """  Worker launcher that wraps the user's command with the Aprun launch framework
    to launch multiple cmd invocations in parallel on a single job allocation

    """
    def __init__(self, overrides=''):
        self.overrides = overrides

    def __call__(self, command, tasks_per_node, nodes_per_block, walltime=None):
        """
        Args:
        - command (string): The command string to be launched
        - tasks_per_node (int) : Workers to launch per node
        - nodes_per_block (int) : Number of nodes in a block

        KWargs:
        - walltime (int) : This is not used by this launcher.
        """

        tasks_per_block = tasks_per_node * nodes_per_block
        x = '''
WORKERCOUNT={1}

cat << APRUN_EOF > cmd_$JOBNAME.sh
{0}
APRUN_EOF
chmod a+x cmd_$JOBNAME.sh

aprun -n {tasks_per_block} -N {tasks_per_node} {overrides} /bin/bash cmd_$JOBNAME.sh &
wait

echo "Done"
'''.format(command, tasks_per_block,
           tasks_per_block=tasks_per_block,
           tasks_per_node=tasks_per_node,
           overrides=self.overrides)
        return x


if __name__ == '__main__':

    s = SingleNodeLauncher()
    wrapped = s("hello", 1, 1)
    print(wrapped)
