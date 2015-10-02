# smartsubmit

A package for managing the baby-making backdoor on the TAS cluster.

#### About

Smartsubmit has two basic functions which support running code over data pulled from CMS3 ntuples. 

1. To act as a sort of distributed filesystem which spreads and indexes ntuple .root files across the cluster
2. To handle the submission of condor jobs on the TAS cluster. 

##### Filesystem

Each ntuple is associated with a 'sample,' e.g. `ttbar_powheg_pythia8_25ns` for ttbar events generated by the pythia8/powheg driven montecarlo with 25ns spacing, and conversely each sample consists of a collection of these ntuples files. Smartsubmit copies sample files from the hadoop filesystem onto to a subset of machines in the TAS cluster. By effectively managing the location of these ntuple files, smartsubmit maximizes parallelizability in converting CMS3 ntuples into TAS babies. 

smartsubmit assumes that the filenames/paths associated with ntuples will be unique and constant on the hadoop filesystem, and so uses that information as a unique identifier for files in the database. To utilize smartsubmit on a set of sample files, the ntuples must first be absorbed into the smartsubmit database. This is achieved using the command 
    
    smartsubmit_ctrl --absorb_sample -d <path_to_hadoop_directory> -n <sample_name>

which will absorb all '.root' files in the specified directory and tag them with the sample name provided. To add a single file to the filesystem you can run 

    smartsubmit_ctrl --absorb_sample -f <path_to_file_on_hadoop> -n <sample_name>
    
Smartsubmit will ensure the file is not already in the database (even under a different sample name) before it absorbs any new files. 

To delete a file from the filesystem 

    smartsubmit_ctrl --delete_sample -f <path_to_file_on_hadoop>
    
##### Condor Submission

To run a job on all files from a single sample in the smartsubmit filesystem run

    smartsubmit_ctrl --run_jobs -e <path_to_executable_on_network_drive> -s <sample_name> -t <path_to_condor_template>
    
The executable specified must take in a space separated list of .root ntuple files as the first positional argument. By providing your own condor submit file template, you may add additional arguments, but these will not be processed in smartsubmit. A default condor submit file is included in the repo. smartsubmit will replace three tokens in the template, `$$__CONDOR_SLOT__$$`, `$$__EXECUTABLE__$$`, and `$$__PATH_TO_SAMPLE__$$`. Be sure to fill in the `Grid_Resource` and `x509userproxy` options with proper values before you try to run smartsubmit. By default, smartsubmit\_ctrl will look in the working directory for a file named condor\_submit\_template if the `-t` option is not specified. 

You may run the same executable on multiple samples with a single command by using the `-s` option repeatedly, e.g.

    smartsubmit_ctrl --run_jobs -e <path_to_executable_on_network_drive> -s <sample_name_1> -s <sample_name_2> .. -s <sample_name_N>

###### Tips

* It's generally good practice to have your executable be a BASH script that will run your root code on the files that come in through the command line, e.g. 


        for i in `seq 1 $#`
        do
            /path/to/analysis_binary $i                     #run analysis code on each sample
            <copy output file back to your local machine>
        done
