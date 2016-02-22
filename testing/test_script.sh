#!/bin/bash


##### VIEWS #####
## Test all the valid views
./smartsubmit_ctrl.py --list_samples
./smartsubmit_ctrl.py --list_samples -v 0
./smartsubmit_ctrl.py --list_samples -v 1
./smartsubmit_ctrl.py --list_samples -v 2
./smartsubmit_ctrl.py --list_samples -v 3
./smartsubmit_ctrl.py --list_samples -v 4

## Test invalid views
./smartsubmit_ctrl.py --list_samples -v turkey
./smartsubmit_ctrl.py --list_samples -v 5

###### ADDING/REMOVING FILES AND DIRS ######

## Give add file command a dir
./smartsubmit_ctrl.py --absorb_sample -f /hadoop/cms/store/group/snt/run2_25ns/TT_TuneCUETP8M1_13TeV-powheg-pythia8_RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v2/V07-04-03/ -s ttbar_powheg_pythia8_25ns
## Give add dir command a file
./smartsubmit_ctrl.py --absorb_sample -f /hadoop/cms/store/group/snt/run2_50ns/TT_TuneCUETP8M1_13TeV-powheg-pythia8_RunIISpring15DR74-Asympt50ns_MCRUN2_74_V9A-v4/V07-04-03/merged_ntuple_138.root -s ttbar_powheg_pythia8_50ns
## Add file/dir that doesn't exist
./smartsubmit_ctrl.py --absorb_sample -f /not/real/path.root -s ttbar_madgraph_pythia8_25ns
./smartsubmit_ctrl.py --absorb_sample -d /not/real/path.root -s ttbar_madgraph_pythia8_25ns
## Add file/dir that does exist
./smartsubmit_ctrl.py --absorb_sample -d /hadoop/cms/store/group/snt/run2_25ns/TT_TuneCUETP8M1_13TeV-powheg-pythia8_RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v2/V07-04-03/ -s ttbar_powheg_pythia8_25ns
./smartsubmit_ctrl.py --absorb_sample -f /hadoop/cms/store/group/snt/run2_50ns/TT_TuneCUETP8M1_13TeV-powheg-pythia8_RunIISpring15DR74-Asympt50ns_MCRUN2_74_V9A-v4/V07-04-03/merged_ntuple_138.root -s ttbar_powheg_pythia8_50ns
## Add the same file twice while it's being moved
./smartsubmit_ctrl.py --absorb_sample -f /hadoop/cms/store/group/snt/run2_50ns/TT_TuneCUETP8M1_13TeV-powheg-pythia8_RunIISpring15DR74-Asympt50ns_MCRUN2_74_V9A-v4/V07-04-03/merged_ntuple_138.root -s ttbar_powheg_pythia8_50ns
## Add file with sample name that has spaces
./smartsubmit_ctrl.py --absorb_sample -f /hadoop/cms/store/group/snt/run2_50ns/TT_TuneCUETP8M1_13TeV-powheg-pythia8_RunIISpring15DR74-Asympt50ns_MCRUN2_74_V9A-v4/V07-04-03/merged_ntuple_138.root -s ttbar powheg pythia8 50ns
./smartsubmit_ctrl.py --absorb_sample -f /hadoop/cms/store/group/snt/run2_50ns/TT_TuneCUETP8M1_13TeV-powheg-pythia8_RunIISpring15DR74-Asympt50ns_MCRUN2_74_V9A-v4/V07-04-03/merged_ntuple_138.root -s "ttbar powheg pythia8 50ns"
## Remove a file that exists
./smartsubmit_ctrl.py --delete_sample -f /hadoop/cms/store/group/snt/run2_25ns/TT_TuneCUETP8M1_13TeV-powheg-pythia8_RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v2/V07-04-03/merged_ntuple_24.root
## Try to remove a directory 
./smartsubmit_ctrl.py --delete_sample -d /hadoop/cms/store/group/snt/run2_25ns/TT_TuneCUETP8M1_13TeV-powheg-pythia8_RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v2/V07-04-03/
## Remove a file that does not exist
./smartsubmit_ctrl.py --delete_sample -d /hadoop/cms/store/group/snt/run2_25ns/TT_TuneCUETP8M1_13TeV-powheg-pythia8_RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v2/V07-04-03/merged_ntuple_14444.root

##### RUN JOB #####
## Run a job without giving an exe
./smartsubmit_ctrl.py --run_job -s ttbar_powheg_pythia8_50ns
## Run a job with a fake exe
./smartsubmit_ctrl.py --run_job -s ttbar_powheg_pythia8_50ns -e /path/to/exe
## Run a job with a real exe
./smartsubmit_ctrl.py --run_job -s ttbar_powheg_pythia8_50ns -e /nfs-7/t2tas/test_executable
## Run a job with a fake sample name
./smartsubmit_ctrl.py --run_job -s fake_sample -e /nfs-7/t2tas/test_executable

##### UPDATE SAMPLE NAMES #####

## Update a sample file's name
./smartsubmit_ctrl --update_file_sample new_name -f/hadoop/cms/store/group/snt/run2_50ns/TT_TuneCUETP8M1_13TeV-powheg-pythia8_RunIISpring15DR74-Asympt50ns_MCRUN2_74_V9A-v4/V07-04-03/Fake_Ntuple.root 
## Update a non-existant sample file's name
./smartsubmit_ctrl --update_file_sample new_name -f/hadoop/cms/store/group/snt/run2_50ns/TT_TuneCUETP8M1_13TeV-powheg-pythia8_RunIISpring15DR74-Asympt50ns_MCRUN2_74_V9A-v4/V07-04-03/Fake.root


##### CHECK JOB OUTPUT #####

## Check job output
./smartsubmit_ctrl.py --check_job 0
./smartsubmit_ctrl.py --check_job 1
./smartsubmit_ctrl.py --check_job 2
./smartsubmit_ctrl.py --check_job 3
./smartsubmit_ctrl.py --check_job 4
./smartsubmit_ctrl.py --check_job 5



