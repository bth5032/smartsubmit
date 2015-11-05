#!/bin/bash
./smartsubmit_ctrl.py --list_samples
./smartsubmit_ctrl.py --list_samples -v 0
./smartsubmit_ctrl.py --list_samples -v 1
./smartsubmit_ctrl.py --list_samples -v 2
./smartsubmit_ctrl.py --list_samples -v 3
./smartsubmit_ctrl.py --list_samples -v 4
./smartsubmit_ctrl.py --list_samples -v turkey
./smartsubmit_ctrl.py --list_samples -v 5

./smartsubmit_ctrl.py --absorb_sample -f /hadoop/cms/store/group/snt/run2_25ns/TTJets_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v2/V07-04-03/merged_ntuple_1.root -s ttbar_madgraph_pythia8_25ns
./smartsubmit_ctrl.py --absorb_sample -f /hadoop/cms/store/group/snt/run2_25ns/TTJets_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v2/V07-04-03/merged_ntuple_1.root -s ttbar_madgraph_pythia8_25ns
./smartsubmit_ctrl.py --check_job 0
./smartsubmit_ctrl.py --check_job 1
