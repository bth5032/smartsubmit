/* ROOT plot maker for smartsubmit test data
Bobak Hashemi -- March 08 2016
*/

#include "TChain.h"
#include "TCanvas.h"
#include "TString.h"
#include "TTree.h"

void rootplotmaker(TString fname, TString run_num="1", TString type="Smartsubmit")
{
	TCanvas *BG = new TCanvas("c1", "default", 1920, 1080);
	TTree *t = new TTree();
	t->ReadFile(fname);
	TString num_files;
	Double_t mx, mn;
	int BINSIZE = 3;


	if (type=="Smartsubmit"){
		num_files = "4 files";
	}
	else{
		num_files = "1 file";	
	}

	mx = t->GetMaximum("REALTIME");
	mn = t->GetMinimum("REALTIME");
	num_bins = int((mx - mn)/BINSIZE);


	TH1F *runHist = new TH1F("runHist", "Root Script Runtime for "+type+" Run "+run_num+", "+num_files+" per job", num_bins,int(0.9*mn),int(1.1*mx));
	
	runHist->SetYTitle("Number of Jobs");
	runHist->SetXTitle("Real Time (seconds)");

	t->Draw("REALTIME>>runHist");
	runHist->Draw("HIST");
	gPad->SaveAs("runHist.png");
}