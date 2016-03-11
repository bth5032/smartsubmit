function makeDirs {

	LAST_RUN_NUM=`ls $BENCHDIR | sed -e 's/run_//' | sort -nr | head -n1`
	RUN_NUM=$((`ls $BENCHDIR | sed -e 's/run_//' | sort -nr | head -n1` + 1 ))

	RUN_DIR=${BENCHDIR}run_${RUN_NUM}/
	LAST_RUN_DIR=${BENCHDIR}run_${LAST_RUN_NUM}/

	if [[ -a ${LAST_RUN_DIR}batch/timing_output.txt ]]
	then
		BATCH_OUT_DIR=${RUN_DIR}batch/

		if [[ ! -d $RUN_DIR ]]
		then
			mkdir $RUN_DIR
		fi
	else
		BATCH_OUT_DIR=${LAST_RUN_DIR}batch/
	fi



	if [[ -a ${LAST_RUN_DIR}ss/timing_output.txt ]]
	then
		SS_OUT_DIR=${RUN_DIR}ss/

		if [[ ! -d $RUN_DIR ]]
		then
			mkdir $RUN_DIR
		fi
	else
		SS_OUT_DIR=${LAST_RUN_DIR}ss/
	fi

	if [[ ! -d $BATCH_OUT_DIR ]]
	then
		mkdir $BATCH_OUT_DIR
	fi

	if [[ ! -d $SS_OUT_DIR ]]
	then
		mkdir $SS_OUT_DIR
	fi


}

function doSS {
	cd ~/temp/smartsubmit_testing/

	cp condorFileTemplate $SS_OUT_DIR
	cp timer.py $SS_OUT_DIR

	ss_ctrl --run_job -e /nfs-7/t2tas/Software/StopAnalysisCMSSW/src/StopAnalysis/StopBabyMaker/bobak/exe -s long -l $SS_OUT_DIR | sed '/^$/d' | tail -n +4 | python timer.py > ${SS_OUT_DIR}timing_output.txt
}

function doBatch {

	PYTHONPATH=~/.local/bin/python/

	PATH=$PATH:~/.local/bin/:~/.local/bin/python/

	pushd /home/users/bhashemi/Projects/GIT/stop1l/CMSSW_7_4_1_patch1/
	cmsenv
	popd

	mkdir ${BATCH_OUT_DIR}output
	mkdir ${BATCH_OUT_DIR}files

	cp /home/users/bhashemi/Projects/GIT/stop1l/CMSSW_7_4_1_patch1/src/StopAnalysis/StopBabyMaker/batch/condorExecutable.sh $BATCH_OUT_DIR

	pushd /home/users/bhashemi/Projects/GIT/stop1l/CMSSW_7_4_1_patch1/src/StopAnalysis/StopBabyMaker/batch
	. setup.sh
	. copy.sh
	. batch.sh > ${BATCH_OUT_DIR}batch_output.txt
	popd

	cat ${BATCH_OUT_DIR}batch_output.txt | grep "1 job(s) submitted to cluster " | sed -e "s/1 job(s) submitted to cluster \(.*\)\.$/\1/g" | python timer.py > ${BATCH_OUT_DIR}timing_output.txt
}

function UC {
	echo "Updating condor submit file:"
	pushd $ssd
	git pull
	popd
	cp $ssd/condorFileTemplate .
	cp $ssd/testing/timer.py .
	cp $ssd/testing/plotmaker.sh .
	cp $ssd/testing/rootplotmaker.c .
	cp $ssd/testing/functions.sh .
	. functions.sh
}

function rmSS {
	condor_rm `condor_q | grep "exe " | cut -d ' ' -f1 | xargs`
}

function rmBatch {
	condor_rm `condor_q | grep "condorExecutable.s" | grep "bhashemi" | cut -d ' ' -f1 | xargs`
}

function loopTest {
	for run in `seq 1 25`
	do
		makeDirs
		doSS
		doBatch
	done
}

function parseBatchLogs {
	if [[ ! -z ${BATCH_OUT_DIR+x} ]]
	then
		if [[ -a ${BATCH_OUT_DIR}timing_output.txt ]]
		then
			mv ${BATCH_OUT_DIR}timing_output.txt ${BATCH_OUT_DIR}timing_output.bak.txt
		fi
		cat ${BATCH_OUT_DIR}batch_output.txt | grep "1 job(s) submitted to cluster " | sed -e "s/1 job(s) submitted to cluster \(.*\)\.$/\1/g" | python timer.py > ${BATCH_OUT_DIR}timing_output.txt
	else
		echo "Cannot parse logs, BATCH_OUT_DIR not set"
	fi
}

alias ss_ctrl=/home/users/bhashemi/Projects/GIT/smartsubmit/smartsubmit_ctrl.py
ssd=/home/users/bhashemi/Projects/GIT/smartsubmit
BENCHDIR=/home/users/bhashemi/temp/smartsubmit_testing/benchmarking/3TB_sample/
