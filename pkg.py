"""
This module provides classes and methods to launch the DlioBenchmark application.
DlioBenchmark is ....
"""
from jarvis_cd.basic.pkg import Application
from jarvis_util import *


class DlioBenchmark(Application):
    """
    This class provides methods to launch the DlioBenchmark application.
    """
    def _init(self):
        """
        Initialize paths
        """
        self.dlio_benchmark_path = None

    def _configure_menu(self):
        """
        Create a CLI menu for the configurator method.
        For thorough documentation of these parameters, view:
        https://github.com/scs-lab/jarvis-util/wiki/3.-Argument-Parsing

        :return: List(dict)
        """
        return [
            {
                'name': 'nprocs',
                'msg': 'Number of processes to spawn',
                'type': int,
                'default': 1,
            },
            {
                'name': 'ppn',
                'msg': 'Processes per node',
                'type': int,
                'default': 16,
            },
            {
                'name': 'workload',
                'msg': 'workload name. The configurations of a workload can be specified through a yaml file, \"check dlio_benchmark/configs/workload/\"',
                'type': str,
                'default': None,
            },
            {
                'name': 'workflow.generate_data',
                'msg': 'If generate synthetic data that DLIO will use or not',
                'type': bool,
                'default': True,
            },
            {
                'name': 'workflow.train',
                'msg': 'If perform training or not',
                'type': bool,
                'default': False,
            },
            {
                'name': 'dataset.data_folder',
                'msg': 'The storage path of the generated data',
                'type': str,
                'default': None,
            },
            {
                'name': 'dataset.num_files_train',
                'msg': 'The number of training files',
                'type': int,
                'default': 0,
            },
            {
                'name': 'reader.batch_size',
                'msg': 'The batch_size of each training step.',
                'type': int,
                'default': 0,
            },  
        ]

    def _configure(self, **kwargs):
        """
        Converts the Jarvis configuration to application-specific configuration.
        E.g., OrangeFS produces an orangefs.xml file.

        :param kwargs: Configuration parameters for this pkg.
        :return: None
        """
        pass

    def start(self):
        """
        Launch an application. E.g., OrangeFS will launch the servers, clients,
        and metadata services on all necessary pkgs.

        :return: None
        """
        # Step 1. generate the data
        gen_data_cmd = [
            'dlio_benchmark',
            f"workload={self.config['workload']}",
            f"++workload.workflow.generate_data={self.config['workflow.generate_data']}",
            f"++workload.workflow.train={self.config['workflow.train']}",
        ]
        if self.config['dataset.data_folder'] != None:
            gen_data_cmd.append(f"++workload.dataset.data_folder={self.config['dataset.data_folder']}")

        if self.config['dataset.num_files_train'] > 0:
            gen_data_cmd.append(f"++workload.dataset.num_files_train={self.config['dataset.num_files_train']}") 

        if self.config['workload'].startswith("unet3d"):
            gen_data_cmd.append(f"++workload.workflow.checkpoint=False")

        if self.config['reader.batch_size'] > 0:
            gen_data_cmd.append(f"++workload.reader.batch_size={self.config['reader.batch_size']}")

        Exec('which mpiexec',
             LocalExecInfo(env=self.mod_env))
        Exec(' '.join(gen_data_cmd),
             MpiExecInfo(env=self.mod_env,
                         hostfile=self.jarvis.hostfile,
                         nprocs=self.config['nprocs'],
                         ppn=self.config['ppn']))
        
        # Step2. Clear the system cache
        # Exec(f'sudo drop_caches',
        #     PsshExecInfo(hostfile=self.jarvis.hostfile, 
        #                 env=self.mod_env))

        # Step3. Run the benchmark
        run_cmd = [
            'dlio_benchmark',
            f"workload={self.config['workload']}", 
        ]
        if self.config['dataset.data_folder'] != None:
            run_cmd.append(f"++workload.dataset.data_folder={self.config['dataset.data_folder']}")

        if self.config['dataset.num_files_train'] > 0:
            run_cmd.append(f"++workload.dataset.num_files_train={self.config['dataset.num_files_train']}") 

        if self.config['workload'].startswith("unet3d"):
            run_cmd.append(f"++workload.workflow.checkpoint=False")

        if self.config['reader.batch_size'] > 0:
            run_cmd.append(f"++workload.reader.batch_size={self.config['reader.batch_size']}")
 

        print(f"run_cmd is {run_cmd}")

        Exec(f' '.join(run_cmd),
            MpiExecInfo(env=self.mod_env,
                         hostfile=self.jarvis.hostfile,
                         nprocs=self.config['nprocs'],
                         ppn=self.config['ppn'])) 


    def stop(self):
        """
        Stop a running application. E.g., OrangeFS will terminate the servers,
        clients, and metadata services.

        :return: None
        """
        pass

    def clean(self):
        """
        Destroy all data for an application. E.g., OrangeFS will delete all
        metadata and data directories in addition to the orangefs.xml file.

        :return: None
        """
        pass
